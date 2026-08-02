from __future__ import annotations

import json
from typing import Any

import asyncpg

from dispatch_core.messaging.models import Provider

from .workflow_store import state_handled_event, state_with_session_event


class PostgresStaffWorkflowSessionStore:
    """Role/provider-scoped state for operator and master messenger flows."""

    _roles = frozenset({"operator", "master"})

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    def _validate_role(cls, role: str) -> None:
        if role not in cls._roles:
            raise ValueError(f"unsupported staff workflow role: {role!r}")

    async def get(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
    ) -> dict[str, Any] | None:
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            state = await connection.fetchval(
                """
                SELECT state FROM staff_workflow_sessions
                WHERE organization_id = $1 AND actor_id = $2
                  AND role = $3 AND provider = $4
                """,
                organization_id,
                actor_id,
                role,
                provider.value,
            )
        if state is None:
            return None
        return json.loads(state) if isinstance(state, str) else dict(state)

    async def put(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
        state: dict[str, Any],
    ) -> None:
        self._validate_role(role)
        stored_state = state_with_session_event(state)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO staff_workflow_sessions (
                    organization_id, actor_id, role, provider, state
                ) VALUES ($1, $2, $3, $4, $5::jsonb)
                ON CONFLICT (organization_id, actor_id, role, provider)
                DO UPDATE SET state = EXCLUDED.state, updated_at = now()
                """,
                organization_id,
                actor_id,
                role,
                provider.value,
                json.dumps(stored_state, ensure_ascii=False, separators=(",", ":")),
            )

    async def handled_event(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
        event_id: str,
    ) -> bool:
        state = await self.get(
            organization_id=organization_id,
            actor_id=actor_id,
            role=role,
            provider=provider,
        )
        return state_handled_event(state, event_id)

    async def clear(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
    ) -> None:
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM staff_workflow_sessions
                WHERE organization_id = $1 AND actor_id = $2
                  AND role = $3 AND provider = $4
                """,
                organization_id,
                actor_id,
                role,
                provider.value,
            )

    async def cleanup_stale(self, *, max_age_hours: int = 24) -> int:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                DELETE FROM staff_workflow_sessions
                WHERE updated_at < now() - make_interval(hours => $1)
                """,
                max_age_hours,
            )
        return int(result.rsplit(" ", 1)[-1])


class PostgresStaffViewStore:
    """Small read model for role menus; all access is organization-scoped."""

    _roles = frozenset({"operator", "master"})
    _active_statuses = (
        "submitted",
        "pool_open",
        "assigned",
        "accepted",
        "en_route",
        "in_progress",
    )

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    def _validate_role(cls, role: str) -> None:
        if role not in cls._roles:
            raise ValueError(f"unsupported staff view role: {role!r}")

    async def list_active_orders(
        self,
        *,
        organization_id: str,
        role: str,
        actor_id: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self._validate_role(role)
        if not 1 <= limit <= 100:
            raise ValueError("order list limit must be between 1 and 100")
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT orders.id, orders.public_number, orders.work_type,
                       orders.source, orders.details, orders.status,
                       orders.assignee_id, orders.coordinator_id,
                       orders.created_at, orders.updated_at,
                       master.display_name AS master_name,
                       master.phone AS master_phone
                FROM work_orders AS orders
                LEFT JOIN actors AS master
                  ON master.organization_id = orders.organization_id
                 AND master.id = orders.assignee_id
                WHERE orders.organization_id = $1
                  AND orders.status = ANY($2::text[])
                  AND ($3 <> 'master' OR orders.assignee_id = $4)
                ORDER BY orders.updated_at DESC, orders.id
                LIMIT $5
                """,
                organization_id,
                list(self._active_statuses),
                role,
                actor_id,
                limit,
            )
        return [_order_dict(row) for row in rows]

    async def get_active_order(
        self,
        *,
        organization_id: str,
        role: str,
        actor_id: str,
        order_id: str,
    ) -> dict[str, Any] | None:
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT orders.id, orders.public_number, orders.work_type,
                       orders.source, orders.details, orders.status,
                       orders.assignee_id, orders.coordinator_id,
                       orders.created_at, orders.updated_at,
                       master.display_name AS master_name,
                       master.phone AS master_phone
                FROM work_orders AS orders
                LEFT JOIN actors AS master
                  ON master.organization_id = orders.organization_id
                 AND master.id = orders.assignee_id
                WHERE orders.organization_id = $1 AND orders.id = $2
                  AND orders.status = ANY($3::text[])
                  AND ($4 <> 'master' OR orders.assignee_id = $5)
                """,
                organization_id,
                order_id,
                list(self._active_statuses),
                role,
                actor_id,
            )
        return _order_dict(row) if row is not None else None

    async def statistics(self, *, organization_id: str) -> dict[str, int]:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    count(*)::int AS total,
                    count(*) FILTER (
                        WHERE status = ANY($2::text[])
                    )::int AS active,
                    count(*) FILTER (WHERE status = 'submitted')::int AS submitted,
                    count(*) FILTER (
                        WHERE status = 'completed'
                          AND updated_at >= CURRENT_DATE
                    )::int AS completed_today,
                    (
                        SELECT count(*)::int
                        FROM actor_roles membership
                        JOIN actors actor
                          ON actor.organization_id = membership.organization_id
                         AND actor.id = membership.actor_id
                        WHERE membership.organization_id = $1
                          AND membership.role = 'master'
                          AND membership.active AND actor.active
                    ) AS masters_total,
                    (
                        SELECT count(DISTINCT membership.actor_id)::int
                        FROM actor_roles membership
                        JOIN actors actor
                          ON actor.organization_id = membership.organization_id
                         AND actor.id = membership.actor_id
                        JOIN external_identities identity
                          ON identity.organization_id = actor.organization_id
                         AND identity.actor_id = actor.id
                         AND identity.provider IN ('telegram', 'max')
                        WHERE membership.organization_id = $1
                          AND membership.role = 'master'
                          AND membership.active AND actor.active
                    ) AS masters_bound
                FROM work_orders
                WHERE organization_id = $1
                """,
                organization_id,
                list(self._active_statuses),
            )
        if row is None:
            return {
                "total": 0,
                "active": 0,
                "submitted": 0,
                "completed_today": 0,
                "masters_total": 0,
                "masters_bound": 0,
            }
        return {key: int(row[key] or 0) for key in row.keys()}

    async def master_has_active_orders(
        self,
        *,
        organization_id: str,
        master_id: str,
    ) -> bool:
        async with self._pool.acquire() as connection:
            exists = await connection.fetchval(
                """
                SELECT 1 FROM work_orders
                WHERE organization_id = $1 AND assignee_id = $2
                  AND status = ANY($3::text[])
                LIMIT 1
                """,
                organization_id,
                master_id,
                list(self._active_statuses),
            )
        return bool(exists)


def _order_dict(row: asyncpg.Record) -> dict[str, Any]:
    result = dict(row)
    details = result.get("details")
    if isinstance(details, str):
        result["details"] = json.loads(details)
    return result
