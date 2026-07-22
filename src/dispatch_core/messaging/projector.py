from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from typing import Any

import asyncpg

from dispatch_core.infrastructure.messaging import (
    PendingDomainEvent,
    PostgresOutboxStore,
)
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.messaging.cards import CardRenderer
from dispatch_core.packs.catalog import PackDefinition


@dataclass(frozen=True, slots=True)
class PlannedButton:
    text: str
    action: str
    payload: dict[str, Any]
    allowed_role: str | None
    row: int = 0


@dataclass(frozen=True, slots=True)
class RecipientPlan:
    actor_ids: tuple[str, ...] = ()
    roles: tuple[str, ...] = ()
    text: str = ""
    buttons: tuple[PlannedButton, ...] = ()
    purpose: str = "notification"


class PostgresNotificationProjector:
    """Atomically turns domain events into callback tokens and outbound messages."""

    def __init__(
        self, pool: asyncpg.Pool, packs: PostgresPackStore | None = None
    ) -> None:
        self._pool = pool
        self._packs = packs

    async def project(self, event: PendingDomainEvent) -> int:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                order = await connection.fetchrow(
                    """
                    SELECT * FROM work_orders
                    WHERE organization_id = $1 AND id = $2
                    """,
                    event.organization_id,
                    event.aggregate_id,
                )
                pack = await self._active_pack(event.organization_id)
                card = self._card(order, pack)
                plans = self._plans(event, order, card)
                inserted = 0
                for plan in plans:
                    buttons = await self._buttons(connection, event, plan.buttons)
                    recipients = await self._recipients(
                        connection,
                        event.organization_id,
                        plan.actor_ids,
                        plan.roles,
                    )
                    for recipient in recipients:
                        result = await connection.execute(
                            """
                            INSERT INTO outbound_messages (
                                deduplication_key, organization_id, provider,
                                recipient_id, text_body, buttons
                            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                            ON CONFLICT (deduplication_key) DO NOTHING
                            """,
                            (
                                f"{event.event_id}:{plan.purpose}:"
                                f"{recipient['provider']}:{recipient['external_user_id']}"
                            ),
                            event.organization_id,
                            recipient["provider"],
                            recipient["external_user_id"],
                            plan.text,
                            _json_text(buttons),
                        )
                        inserted += int(result == "INSERT 0 1")
                await connection.execute(
                    """
                    UPDATE outbox_events SET
                        status = 'delivered',
                        delivered_at = now(),
                        claimed_at = NULL
                    WHERE event_id = $1 AND status = 'processing'
                    """,
                    event.event_id,
                )
        return inserted

    async def _buttons(
        self,
        connection: asyncpg.Connection,
        event: PendingDomainEvent,
        buttons: Sequence[PlannedButton],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for button in buttons:
            token = token_urlsafe(18)
            await connection.execute(
                """
                INSERT INTO callback_actions (
                    token, organization_id, action, payload,
                    allowed_role, expires_at
                ) VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                """,
                token,
                event.organization_id,
                button.action,
                _json_text(button.payload),
                button.allowed_role,
                datetime.now(UTC) + timedelta(days=7),
            )
            result.append(
                {
                    "text": button.text,
                    "callback_token": token,
                    "url": None,
                    "request_location": False,
                    "row": button.row,
                }
            )
        return result

    @staticmethod
    async def _recipients(
        connection: asyncpg.Connection,
        organization_id: str,
        actor_ids: tuple[str, ...],
        roles: tuple[str, ...],
    ) -> Sequence[asyncpg.Record]:
        if not actor_ids and not roles:
            return ()
        return await connection.fetch(
            """
            SELECT identity.provider, identity.external_user_id
            FROM external_identities AS identity
            JOIN actors AS actor
              ON actor.organization_id = identity.organization_id
             AND actor.id = identity.actor_id
            WHERE identity.organization_id = $1
              AND identity.provider IN ('telegram', 'max')
              AND actor.active
              AND (
                  actor.id = ANY($2::text[])
                  OR actor.role = ANY($3::text[])
              )
            ORDER BY identity.provider, identity.external_user_id
            """,
            organization_id,
            list(actor_ids),
            list(roles),
        )

    def _plans(
        self,
        event: PendingDomainEvent,
        order: asyncpg.Record | None,
        card: str,
    ) -> tuple[RecipientPlan, ...]:
        if event.aggregate_type != "work_order" or order is None:
            return ()
        order_id = event.aggregate_id
        assignee = order["assignee_id"]
        coordinator = order["coordinator_id"]
        requester = order["requester_id"]
        coordinators = (coordinator,) if coordinator else ()

        if event.name == "work_order.submitted":
            return (
                RecipientPlan(
                    roles=("coordinator", "admin"),
                    text=f"Новая заявка от клиента.\n{card}",
                    buttons=(
                        PlannedButton(
                            "В пул",
                            "pool_publish",
                            {"order_id": order_id},
                            "coordinator",
                        ),
                    ),
                    purpose="submitted",
                ),
            )
        if event.name == "work_order.coordination_claimed":
            coordinator_id = str(event.payload.get("coordinator_id") or "")
            return (
                RecipientPlan(
                    actor_ids=(coordinator_id,) if coordinator_id else (),
                    text=f"Вы взяли заявку в работу.\n{card}",
                    purpose="coordination_claimed",
                ),
            )
        if event.name == "work_order.pool_published":
            mode = event.payload.get("mode")
            action = "pool_interest" if mode == "curated" else "pool_claim"
            text = "Готов взять" if mode == "curated" else "Взять заявку"
            return (
                RecipientPlan(
                    roles=("executor",),
                    text=card,
                    buttons=(
                        PlannedButton(
                            text,
                            action,
                            {"order_id": order_id},
                            "executor",
                        ),
                    ),
                    purpose="pool",
                ),
            )
        if event.name == "work_order.pool_interest_recorded":
            executor_id = str(event.payload["executor_id"])
            return (
                RecipientPlan(
                    actor_ids=coordinators,
                    roles=() if coordinators else ("coordinator", "admin"),
                    text=f"{card}\nМастер готов взять: {executor_id}",
                    buttons=(
                        PlannedButton(
                            "Выбрать мастера",
                            "assign",
                            {"order_id": order_id, "executor_id": executor_id},
                            "coordinator",
                        ),
                    ),
                    purpose=f"interest:{executor_id}",
                ),
            )
        if event.name in {"work_order.assigned", "work_order.first_claim_won"}:
            if not assignee:
                return ()
            plans = [
                RecipientPlan(
                    actor_ids=(assignee,),
                    text=f"Вам назначена заявка.\n{card}",
                    buttons=(
                        PlannedButton(
                            "Принять",
                            "accept",
                            {"order_id": order_id},
                            "executor",
                        ),
                        PlannedButton(
                            "Отказаться",
                            "reject",
                            {"order_id": order_id},
                            "executor",
                            row=1,
                        ),
                    ),
                    purpose="assignment",
                ),
            ]
            if coordinators:
                label = (
                    "Мастер закрепился за заявкой."
                    if event.name == "work_order.first_claim_won"
                    else "Мастер назначен на заявку."
                )
                plans.append(
                    RecipientPlan(
                        actor_ids=coordinators,
                        text=f"{label}\n{card}",
                        purpose="coordinator_assignment",
                    )
                )
            return tuple(plans)
        if event.name == "work_order.accepted":
            plans = [
                RecipientPlan(
                    actor_ids=(assignee,) if assignee else (),
                    text=f"Заявка принята.\n{card}",
                    buttons=(
                        PlannedButton(
                            "Выехал",
                            "start_travel",
                            {"order_id": order_id},
                            "executor",
                        ),
                        PlannedButton(
                            "Начать на месте",
                            "start_work",
                            {"order_id": order_id},
                            "executor",
                            row=1,
                        ),
                    ),
                    purpose="executor_accepted",
                )
            ]
            if coordinators:
                plans.append(
                    RecipientPlan(
                        actor_ids=coordinators,
                        text=f"Мастер принял заявку.\n{card}",
                        purpose="coordinator_accepted",
                    )
                )
            return tuple(plans)
        if event.name == "work_order.travel_started":
            plans = [
                RecipientPlan(
                    actor_ids=(assignee,) if assignee else (),
                    text=f"Выезд начат.\n{card}",
                    buttons=(
                        PlannedButton(
                            "Начать работу",
                            "start_work",
                            {"order_id": order_id},
                            "executor",
                        ),
                    ),
                    purpose="travel",
                ),
            ]
            if coordinators:
                plans.append(
                    RecipientPlan(
                        actor_ids=coordinators,
                        text=f"Мастер выехал на заявку.\n{card}",
                        purpose="coordinator_travel",
                    )
                )
            return tuple(plans)
        if event.name == "work_order.started":
            return (
                RecipientPlan(
                    actor_ids=(assignee,) if assignee else (),
                    text=(
                        f"Работа начата.\n{card}\n"
                        "Отправьте фото и комментарий, затем нажмите «Завершить»."
                    ),
                    buttons=(
                        PlannedButton(
                            "Завершить",
                            "submit_report",
                            {"order_id": order_id},
                            "executor",
                        ),
                    ),
                    purpose="work_started",
                ),
            )
        if event.name == "work_order.completed":
            actors = tuple(value for value in (coordinator, requester) if value)
            return (
                RecipientPlan(
                    actor_ids=actors,
                    roles=() if actors else ("coordinator", "admin"),
                    text=f"Заявка завершена.\n{card}",
                    purpose="completed",
                ),
            )
        if event.name == "work_order.assignment_rejected":
            return (
                RecipientPlan(
                    actor_ids=coordinators,
                    roles=() if coordinators else ("coordinator", "admin"),
                    text=f"Мастер отказался; заявка возвращена в пул.\n{card}",
                    purpose="rejected",
                ),
            )
        if event.name == "work_order.cancelled":
            actors = tuple(value for value in (assignee, requester) if value)
            return (
                RecipientPlan(
                    actor_ids=actors,
                    text=f"Заявка отменена.\n{card}",
                    purpose="cancelled",
                ),
            )
        return ()

    async def _active_pack(
        self, organization_id: str
    ) -> PackDefinition | None:
        if self._packs is None:
            return None
        return await self._packs.active(organization_id)

    def _card(
        self,
        order: asyncpg.Record | None,
        pack: PackDefinition | None,
    ) -> str:
        if order is None:
            return ""
        details = _json_value(order["details"])
        if pack is not None:
            return CardRenderer(pack).order_card(
                work_type=order["work_type"], details=details
            )
        address = details.get("address") or details.get("destination") or ""
        label = (
            details.get("summary") or details.get("fault") or order["work_type"]
        )
        return f"Заявка: {label}" + (
            f"\nАдрес: {address}" if address else ""
        )


class OutboxProjectorWorker:
    def __init__(
        self,
        store: PostgresOutboxStore,
        projector: PostgresNotificationProjector,
    ) -> None:
        self._store = store
        self._projector = projector

    async def run_once(self, *, limit: int = 50) -> int:
        events = await self._store.claim_events(limit=limit)
        projected = 0
        for event in events:
            try:
                await self._projector.project(event)
            except Exception as exc:
                await self._store.mark_failed(
                    event,
                    f"{type(exc).__name__}: {exc}",
                )
                continue
            projected += 1
        return projected


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
