from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from typing import Any

import asyncpg

from dispatch_core.application.tracking_links import (
    location_submission_url,
    public_tracking_url,
)
from dispatch_core.infrastructure.messaging import (
    PendingDomainEvent,
    PostgresOutboxStore,
)
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.messaging.cards import operator_order_card
from dispatch_core.packs.catalog import PackDefinition

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PlannedButton:
    text: str
    action: str | None
    payload: dict[str, Any]
    allowed_role: str | None
    row: int = 0
    url: str | None = None
    request_location: bool = False

    def __post_init__(self) -> None:
        destinations = sum(
            (self.action is not None, self.url is not None, self.request_location)
        )
        if destinations != 1:
            raise ValueError("planned button requires exactly one destination")


@dataclass(frozen=True, slots=True)
class RecipientPlan:
    actor_ids: tuple[str, ...] = ()
    roles: tuple[str, ...] = ()
    text: str = ""
    buttons: tuple[PlannedButton, ...] = ()
    purpose: str = "notification"
    delivery_role: str = ""


class PostgresNotificationProjector:
    """Atomically turns domain events into callback tokens and outbound messages."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        packs: PostgresPackStore | None = None,
        *,
        public_base_url: str | None = None,
    ) -> None:
        self._pool = pool
        self._packs = packs
        self._public_base_url = public_base_url

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
                public_token = None
                location_token = None
                if event.name == "work_order.travel_started" and order is not None:
                    capabilities = await connection.fetchrow(
                        """
                        SELECT public_token, location_token
                        FROM tracking_sessions
                        WHERE organization_id = $1 AND work_order_id = $2
                          AND status = 'active'
                        """,
                        event.organization_id,
                        event.aggregate_id,
                    )
                    if capabilities is not None:
                        public_token = capabilities["public_token"]
                        location_token = capabilities["location_token"]
                plans = self._plans(
                    event,
                    order,
                    card,
                    public_token=public_token,
                    location_token=location_token,
                )
                inserted = 0
                for plan in plans:
                    buttons = await self._buttons(connection, event, plan.buttons)
                    recipients = await self._recipients(
                        connection,
                        event.organization_id,
                        plan.actor_ids,
                        plan.roles,
                        plan.delivery_role,
                    )
                    for recipient in recipients:
                        consumer_key = _role_to_consumer_key(str(recipient["role"]))
                        result = await connection.execute(
                            """
                            INSERT INTO outbound_messages (
                                deduplication_key, organization_id, provider,
                                recipient_id, text_body, buttons, consumer_key
                            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
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
                            consumer_key,
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
            if button.url is not None:
                result.append(
                    {
                        "text": button.text,
                        "callback_token": None,
                        "url": button.url,
                        "request_location": False,
                        "row": button.row,
                    }
                )
                continue
            if button.request_location:
                result.append(
                    {
                        "text": button.text,
                        "callback_token": None,
                        "url": None,
                        "request_location": True,
                        "row": button.row,
                    }
                )
                continue
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
        delivery_role: str,
    ) -> Sequence[asyncpg.Record]:
        if not actor_ids and not roles:
            return ()
        return await connection.fetch(
            """
            SELECT identity.provider, identity.external_user_id,
                   COALESCE(
                       NULLIF($4, ''),
                       (
                           SELECT membership.role
                           FROM actor_roles membership
                           WHERE membership.organization_id = actor.organization_id
                             AND membership.actor_id = actor.id
                             AND membership.active
                             AND membership.role = ANY($3::text[])
                           ORDER BY array_position($3::text[], membership.role)
                           LIMIT 1
                       ),
                       actor.role
                   ) AS role
            FROM external_identities AS identity
            JOIN actors AS actor
              ON actor.organization_id = identity.organization_id
             AND actor.id = identity.actor_id
            WHERE identity.organization_id = $1
              AND identity.provider IN ('telegram', 'max')
              AND actor.active
              AND (
                  $4 = '' OR EXISTS (
                      SELECT 1 FROM actor_roles delivery_membership
                      WHERE delivery_membership.organization_id = actor.organization_id
                        AND delivery_membership.actor_id = actor.id
                        AND delivery_membership.role = $4
                        AND delivery_membership.active
                  )
              )
              AND (
                  actor.id = ANY($2::text[])
                  OR EXISTS (
                      SELECT 1 FROM actor_roles assigned
                      WHERE assigned.organization_id = actor.organization_id
                        AND assigned.actor_id = actor.id
                        AND assigned.role = ANY($3::text[])
                        AND assigned.active
                  )
              )
            ORDER BY identity.provider, identity.external_user_id
            """,
            organization_id,
            list(actor_ids),
            list(roles),
            delivery_role,
        )

    def _plans(
        self,
        event: PendingDomainEvent,
        order: asyncpg.Record | None,
        card: str,
        *,
        public_token: str | None = None,
        location_token: str | None = None,
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
                    roles=("operator",),
                    text=f"Новая заявка от клиента.\n{card}",
                    buttons=(
                        PlannedButton(
                            "В пул",
                            "pool_publish",
                            {"order_id": order_id},
                            "operator",
                        ),
                    ),
                    purpose="submitted",
                    delivery_role="operator",
                ),
            )
        if event.name == "work_order.coordination_claimed":
            coordinator_id = str(event.payload.get("coordinator_id") or "")
            return (
                RecipientPlan(
                    actor_ids=(coordinator_id,) if coordinator_id else (),
                    text=f"Вы взяли заявку в работу.\n{card}",
                    purpose="coordination_claimed",
                    delivery_role="operator",
                ),
            )
        if event.name == "work_order.pool_published":
            mode = event.payload.get("mode")
            action = "pool_interest" if mode == "curated" else "pool_claim"
            text = "Готов взять" if mode == "curated" else "Взять заявку"
            return (
                RecipientPlan(
                    roles=("master",),
                    text=card,
                    buttons=(
                        PlannedButton(
                            text,
                            action,
                            {"order_id": order_id},
                            "master",
                        ),
                    ),
                    purpose="pool",
                    delivery_role="master",
                ),
            )
        if event.name == "work_order.pool_interest_recorded":
            executor_id = str(event.payload["executor_id"])
            return (
                RecipientPlan(
                    actor_ids=coordinators,
                    roles=() if coordinators else ("operator",),
                    text=f"{card}\nМастер готов взять: {executor_id}",
                    buttons=(
                        PlannedButton(
                            "Выбрать мастера",
                            "assign",
                            {"order_id": order_id, "executor_id": executor_id},
                            "operator",
                        ),
                    ),
                    purpose=f"interest:{executor_id}",
                    delivery_role="operator",
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
                            "master",
                        ),
                        PlannedButton(
                            "Отказаться",
                            "reject",
                            {"order_id": order_id},
                            "master",
                            row=1,
                        ),
                    ),
                    purpose="assignment",
                    delivery_role="master",
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
                        delivery_role="operator",
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
                            "master",
                        ),
                        PlannedButton(
                            "Начать на месте",
                            "start_work",
                            {"order_id": order_id},
                            "master",
                            row=1,
                        ),
                    ),
                    purpose="executor_accepted",
                    delivery_role="master",
                )
            ]
            if coordinators:
                plans.append(
                    RecipientPlan(
                        actor_ids=coordinators,
                        text=f"Мастер принял заявку.\n{card}",
                        purpose="coordinator_accepted",
                        delivery_role="operator",
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
                            "master",
                        ),
                    ),
                    purpose="travel",
                    delivery_role="master",
                ),
                RecipientPlan(
                    actor_ids=(assignee,) if assignee else (),
                    text=(
                        "Отправьте геопозицию. Для движения используйте "
                        "трансляцию геопозиции в мессенджере."
                    ),
                    buttons=(
                        PlannedButton(
                            "Отправить геопозицию",
                            None,
                            {},
                            "master",
                            request_location=True,
                        ),
                    ),
                    purpose="travel:location",
                    delivery_role="master",
                ),
            ]
            if coordinators:
                plans.append(
                    RecipientPlan(
                        actor_ids=coordinators,
                        text=f"Мастер выехал на заявку.\n{card}",
                        purpose="coordinator_travel",
                        delivery_role="operator",
                    )
                )
            if requester and self._public_base_url and public_token:
                plans.append(
                    RecipientPlan(
                        actor_ids=(requester,),
                        text="Мастер выехал. Положение можно смотреть по ссылке.",
                        buttons=(
                            PlannedButton(
                                "Открыть карту",
                                None,
                                {},
                                "client",
                                url=public_tracking_url(
                                    self._public_base_url,
                                    public_token,
                                ),
                            ),
                        ),
                        purpose="travel:client",
                        delivery_role="client",
                    )
                )
            if assignee and self._public_base_url and location_token:
                plans.append(
                    RecipientPlan(
                        actor_ids=(assignee,),
                        text=(
                            "Если мессенджер не передаёт геопозицию, откройте "
                            "резервную передачу GPS."
                        ),
                        buttons=(
                            PlannedButton(
                                "Передавать GPS",
                                None,
                                {},
                                "master",
                                url=location_submission_url(
                                    self._public_base_url,
                                    location_token,
                                ),
                            ),
                        ),
                        purpose="travel:browser_location",
                        delivery_role="master",
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
                            "master",
                        ),
                    ),
                    purpose="work_started",
                    delivery_role="master",
                ),
            )
        if event.name == "work_order.completed":
            plans: list[RecipientPlan] = []
            if coordinator:
                plans.append(
                    RecipientPlan(
                        actor_ids=(coordinator,),
                        text=f"Заявка завершена.\n{card}",
                        purpose="completed:operator",
                        delivery_role="operator",
                    )
                )
            if requester:
                plans.append(
                    RecipientPlan(
                        actor_ids=(requester,),
                        text=f"Заявка завершена.\n{card}",
                        purpose="completed:client",
                        delivery_role="client",
                    )
                )
            if not plans:
                plans.append(
                    RecipientPlan(
                        roles=("operator",),
                        text=f"Заявка завершена.\n{card}",
                        purpose="completed:operator",
                        delivery_role="operator",
                    )
                )
            return tuple(plans)
        if event.name == "work_order.assignment_rejected":
            return (
                RecipientPlan(
                    actor_ids=coordinators,
                    roles=() if coordinators else ("operator",),
                    text=f"Мастер отказался; заявка возвращена в пул.\n{card}",
                    purpose="rejected",
                    delivery_role="operator",
                ),
            )
        if event.name == "work_order.cancelled":
            plans = []
            if assignee:
                plans.append(
                    RecipientPlan(
                        actor_ids=(assignee,),
                        text=f"Заявка отменена.\n{card}",
                        purpose="cancelled:master",
                        delivery_role="master",
                    )
                )
            if requester:
                plans.append(
                    RecipientPlan(
                        actor_ids=(requester,),
                        text=f"Заявка отменена.\n{card}",
                        purpose="cancelled:client",
                        delivery_role="client",
                    )
                )
            return tuple(plans)
        return ()

    async def _active_pack(self, organization_id: str) -> PackDefinition | None:
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
        values = dict(order)
        values["details"] = details
        return operator_order_card(values, pack=pack)


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
                logger.exception("projection failed for %s", event.event_id)
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


def _role_to_consumer_key(role: str) -> str:
    return role if role in {"admin", "operator", "master", "client"} else ""
