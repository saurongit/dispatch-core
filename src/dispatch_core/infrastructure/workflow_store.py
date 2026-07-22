from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import asyncpg

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.work_order import CompletionReport
from dispatch_core.messaging.models import Provider


@dataclass(frozen=True, slots=True)
class ActiveExecution:
    order_id: str
    status: str
    tracking_session_id: str | None


class PostgresIdentityStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def resolve(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
    ) -> ActorIdentity | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT actor.id, actor.role, actor.display_name
                FROM external_identities AS identity
                JOIN actors AS actor
                  ON actor.organization_id = identity.organization_id
                 AND actor.id = identity.actor_id
                WHERE identity.organization_id = $1
                  AND identity.provider = $2
                  AND identity.external_user_id = $3
                  AND actor.active
                """,
                organization_id,
                provider.value,
                external_user_id,
            )
        if row is None:
            return None
        return ActorIdentity(
            organization_id=organization_id,
            actor_id=row["id"],
            role=row["role"],
            display_name=row["display_name"],
            provider=provider,
            external_user_id=external_user_id,
        )

    async def external_ids_for_role(
        self,
        *,
        organization_id: str,
        provider: Provider,
        role: str,
    ) -> list[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT identity.external_user_id
                FROM external_identities AS identity
                JOIN actors AS actor
                  ON actor.organization_id = identity.organization_id
                 AND actor.id = identity.actor_id
                WHERE identity.organization_id = $1
                  AND identity.provider = $2
                  AND actor.role = $3
                  AND actor.active
                """,
                organization_id,
                provider.value,
                role,
            )
        return [str(row["external_user_id"]) for row in rows]

    async def upsert_actor(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        display_name: str,
        provider: Provider | None = None,
        external_user_id: str | None = None,
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO actors (
                        organization_id, id, role, display_name, active
                    ) VALUES ($1, $2, $3, $4, true)
                    ON CONFLICT (organization_id, id) DO UPDATE SET
                        role = EXCLUDED.role,
                        display_name = EXCLUDED.display_name,
                        active = true
                    """,
                    organization_id,
                    actor_id,
                    role,
                    display_name,
                )
                if provider is not None and external_user_id is not None:
                    await connection.execute(
                        """
                        INSERT INTO external_identities (
                            organization_id, provider, external_user_id, actor_id
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (
                            organization_id, provider, external_user_id
                        ) DO UPDATE SET actor_id = EXCLUDED.actor_id
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                        actor_id,
                    )


class PostgresExecutionStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def active_for_executor(
        self, organization_id: str, executor_id: str
    ) -> ActiveExecution | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT orders.id, orders.status, tracking.id AS tracking_session_id
                FROM work_orders AS orders
                LEFT JOIN tracking_sessions AS tracking
                  ON tracking.organization_id = orders.organization_id
                 AND tracking.work_order_id = orders.id
                 AND tracking.status = 'active'
                WHERE orders.organization_id = $1
                  AND orders.assignee_id = $2
                  AND orders.status IN (
                      'assigned', 'accepted', 'en_route', 'in_progress'
                  )
                """,
                organization_id,
                executor_id,
            )
        if row is None:
            return None
        return ActiveExecution(
            order_id=row["id"],
            status=row["status"],
            tracking_session_id=row["tracking_session_id"],
        )


class PostgresReportDraftStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def append_photo(
        self,
        *,
        organization_id: str,
        order_id: str,
        executor_id: str,
        photo_ref: str,
    ) -> int:
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                """
                INSERT INTO report_drafts (
                    organization_id, work_order_id, executor_id, photo_refs
                ) VALUES ($1, $2, $3, jsonb_build_array($4::text))
                ON CONFLICT (
                    organization_id, work_order_id, executor_id
                ) DO UPDATE SET
                    photo_refs = CASE
                        WHEN report_drafts.photo_refs
                            @> jsonb_build_array($4::text)
                        THEN report_drafts.photo_refs
                        ELSE report_drafts.photo_refs
                            || jsonb_build_array($4::text)
                    END,
                    updated_at = now()
                RETURNING jsonb_array_length(photo_refs)
                """,
                organization_id,
                order_id,
                executor_id,
                photo_ref,
            )
        return int(value)

    async def set_comment(
        self,
        *,
        organization_id: str,
        order_id: str,
        executor_id: str,
        comment: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO report_drafts (
                    organization_id, work_order_id, executor_id, comment
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (
                    organization_id, work_order_id, executor_id
                ) DO UPDATE SET comment = EXCLUDED.comment, updated_at = now()
                """,
                organization_id,
                order_id,
                executor_id,
                comment,
            )

    async def get(
        self,
        *,
        organization_id: str,
        order_id: str,
        executor_id: str,
    ) -> CompletionReport:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT photo_refs, comment, signature_ref, customer_code
                FROM report_drafts
                WHERE organization_id = $1
                  AND work_order_id = $2
                  AND executor_id = $3
                """,
                organization_id,
                order_id,
                executor_id,
            )
        if row is None:
            return CompletionReport()
        photos: Any = row["photo_refs"]
        if isinstance(photos, str):
            photos = json.loads(photos)
        return CompletionReport(
            photo_refs=tuple(str(value) for value in photos),
            comment=row["comment"],
            signature_ref=row["signature_ref"],
            customer_code=row["customer_code"],
        )

    async def clear(
        self,
        *,
        organization_id: str,
        order_id: str,
        executor_id: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM report_drafts
                WHERE organization_id = $1
                  AND work_order_id = $2
                  AND executor_id = $3
                """,
                organization_id,
                order_id,
                executor_id,
            )


class _PostgresSessionStore:
    """Per-actor FSM state persisted as jsonb; shared by intake and config."""

    _table = ""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(
        self, organization_id: str, actor_id: str
    ) -> dict[str, Any] | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT state FROM {self._table}
                WHERE organization_id = $1 AND actor_id = $2
                """,
                organization_id,
                actor_id,
            )
        if row is None:
            return None
        state = row["state"]
        return json.loads(state) if isinstance(state, str) else state

    async def put(
        self,
        *,
        organization_id: str,
        actor_id: str,
        provider: Provider,
        state: dict[str, Any],
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                f"""
                INSERT INTO {self._table} (
                    organization_id, actor_id, provider, state
                ) VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (organization_id, actor_id) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    state = EXCLUDED.state,
                    updated_at = now()
                """,
                organization_id,
                actor_id,
                provider.value,
                json.dumps(state, ensure_ascii=False, separators=(",", ":")),
            )

    async def clear(self, organization_id: str, actor_id: str) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                f"""
                DELETE FROM {self._table}
                WHERE organization_id = $1 AND actor_id = $2
                """,
                organization_id,
                actor_id,
            )

    async def cleanup_stale(
        self, *, max_age_hours: int = 24, organization_id: str | None = None
    ) -> int:
        async with self._pool.acquire() as connection:
            if organization_id:
                result = await connection.execute(
                    f"""
                    DELETE FROM {self._table}
                    WHERE organization_id = $1
                      AND updated_at < now() - make_interval(hours => $2)
                    """,
                    organization_id,
                    max_age_hours,
                )
            else:
                result = await connection.execute(
                    f"""
                    DELETE FROM {self._table}
                    WHERE updated_at < now() - make_interval(hours => $1)
                    """,
                    max_age_hours,
                )
            parts = result.split()
            return int(parts[-1]) if len(parts) >= 3 else 0


class PostgresIntakeSessionStore(_PostgresSessionStore):
    _table = "intake_sessions"


class PostgresConfigSessionStore(_PostgresSessionStore):
    _table = "config_sessions"

