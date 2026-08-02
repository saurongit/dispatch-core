from __future__ import annotations

import json
import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.work_order import CompletionReport
from dispatch_core.messaging.models import Provider

_SESSION_EVENT_KEY = "_dispatch_last_event_id"
_CURRENT_SESSION_EVENT_ID: ContextVar[str | None] = ContextVar(
    "dispatch_session_event_id",
    default=None,
)


@contextmanager
def session_event(event_id: str):
    token = _CURRENT_SESSION_EVENT_ID.set(event_id)
    try:
        yield
    finally:
        _CURRENT_SESSION_EVENT_ID.reset(token)


def state_with_session_event(state: dict[str, Any]) -> dict[str, Any]:
    stored_state = dict(state)
    event_id = _CURRENT_SESSION_EVENT_ID.get()
    if event_id:
        stored_state[_SESSION_EVENT_KEY] = event_id
    return stored_state


def state_handled_event(state: dict[str, Any] | None, event_id: str) -> bool:
    return bool(state and state.get(_SESSION_EVENT_KEY) == event_id)


@dataclass(frozen=True, slots=True)
class ActiveExecution:
    order_id: str
    status: str
    tracking_session_id: str | None


@dataclass(frozen=True, slots=True)
class IntakeAddressSelection:
    organization_id: str
    actor_id: str
    provider: Provider
    address: str
    latitude: float
    longitude: float


class PostgresIdentityStore:
    _roles = frozenset({"admin", "operator", "master", "client"})

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    def _validate_role(cls, role: str) -> None:
        if role not in cls._roles:
            raise ValueError(f"unsupported actor role: {role!r}")

    @classmethod
    def _requested_role(cls, consumer_key: str) -> str | None:
        if consumer_key in cls._roles:
            return consumer_key
        if consumer_key.startswith("staff:"):
            candidate = consumer_key.partition(":")[2]
            return candidate if candidate in cls._roles else None
        return None

    @classmethod
    def _effective_role(
        cls,
        *,
        primary_role: str,
        roles: frozenset[str],
        consumer_key: str,
    ) -> str | None:
        requested = cls._requested_role(consumer_key)
        if requested is not None:
            return requested if requested in roles else None
        if consumer_key == "staff":
            staff_roles = roles.intersection({"admin", "operator", "master"})
            return next(iter(staff_roles)) if len(staff_roles) == 1 else None
        if primary_role in roles:
            return primary_role
        return sorted(roles)[0] if roles else None

    @staticmethod
    def _role_set(value: Any) -> frozenset[str]:
        return frozenset(str(role) for role in (value or ()))

    async def resolve(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str = "",
    ) -> ActorIdentity | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT actor.id, actor.role AS primary_role,
                       actor.display_name,
                       ARRAY(
                           SELECT membership.role
                           FROM actor_roles AS membership
                           WHERE membership.organization_id = actor.organization_id
                             AND membership.actor_id = actor.id
                             AND membership.active
                           ORDER BY membership.role
                       ) AS roles
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
        roles = self._role_set(row["roles"])
        effective_role = self._effective_role(
            primary_role=str(row["primary_role"]),
            roles=roles,
            consumer_key=consumer_key,
        )
        if effective_role is None:
            return None
        return ActorIdentity(
            organization_id=organization_id,
            actor_id=row["id"],
            role=effective_role,
            display_name=row["display_name"],
            provider=provider,
            external_user_id=external_user_id,
            roles=roles,
        )

    async def resolve_actor_id(
        self,
        *,
        organization_id: str,
        actor_id: str,
        required_role: str,
    ) -> ActorIdentity | None:
        self._validate_role(required_role)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT actor.display_name,
                       identity.provider,
                       identity.external_user_id,
                       ARRAY(
                           SELECT membership.role
                           FROM actor_roles AS membership
                           WHERE membership.organization_id = actor.organization_id
                             AND membership.actor_id = actor.id
                             AND membership.active
                           ORDER BY membership.role
                       ) AS roles
                FROM actors AS actor
                LEFT JOIN LATERAL (
                    SELECT external.provider, external.external_user_id
                    FROM external_identities AS external
                    WHERE external.organization_id = actor.organization_id
                      AND external.actor_id = actor.id
                    ORDER BY (external.provider = 'telegram') DESC,
                             external.provider,
                             external.external_user_id
                    LIMIT 1
                ) AS identity ON true
                WHERE actor.organization_id = $1
                  AND actor.id = $2
                  AND actor.active
                  AND EXISTS (
                      SELECT 1 FROM actor_roles AS required
                      WHERE required.organization_id = actor.organization_id
                        AND required.actor_id = actor.id
                        AND required.role = $3
                        AND required.active
                  )
                """,
                organization_id,
                actor_id,
                required_role,
            )
        if row is None:
            return None
        roles = self._role_set(row["roles"])
        provider = Provider(row["provider"] or Provider.TELEGRAM.value)
        external_user_id = str(row["external_user_id"] or actor_id)
        return ActorIdentity(
            organization_id=organization_id,
            actor_id=actor_id,
            role=required_role,
            display_name=row["display_name"],
            provider=provider,
            external_user_id=external_user_id,
            roles=roles,
        )

    async def external_ids_for_role(
        self,
        *,
        organization_id: str,
        provider: Provider,
        role: str,
        consumer_key: str = "",
    ) -> list[str]:
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT identity.external_user_id
                FROM external_identities AS identity
                JOIN actors AS actor
                  ON actor.organization_id = identity.organization_id
                 AND actor.id = identity.actor_id
                JOIN actor_roles AS membership
                  ON membership.organization_id = actor.organization_id
                 AND membership.actor_id = actor.id
                 AND membership.role = $3
                 AND membership.active
                WHERE identity.organization_id = $1
                  AND identity.provider = $2
                  AND actor.active
                ORDER BY identity.external_user_id
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
        consumer_key: str = "",
    ) -> None:
        self._validate_role(role)
        if (provider is None) != (external_user_id is None):
            raise ValueError("provider and external_user_id must be specified together")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO actors (
                        organization_id, id, role, display_name, active
                    ) VALUES ($1, $2, $3, $4, true)
                    ON CONFLICT (organization_id, id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        active = true
                    """,
                    organization_id,
                    actor_id,
                    role,
                    display_name,
                )
                await connection.execute(
                    """
                    INSERT INTO actor_roles (
                        organization_id, actor_id, role, active, revoked_at
                    ) VALUES ($1, $2, $3, true, NULL)
                    ON CONFLICT (organization_id, actor_id, role) DO UPDATE SET
                        active = true,
                        granted_at = now(),
                        revoked_at = NULL
                    """,
                    organization_id,
                    actor_id,
                    role,
                )
                if provider is not None and external_user_id is not None:
                    existing_actor_id = await connection.fetchval(
                        """
                        SELECT actor_id
                        FROM external_identities
                        WHERE organization_id = $1
                          AND provider = $2
                          AND external_user_id = $3
                        FOR UPDATE
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                    )
                    if existing_actor_id is not None and existing_actor_id != actor_id:
                        raise ValueError(
                            "external account is already bound to another actor"
                        )
                    await connection.execute(
                        """
                        INSERT INTO external_identities (
                            organization_id, provider, external_user_id, actor_id
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (
                            organization_id, provider, external_user_id
                        ) DO NOTHING
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                        actor_id,
                    )

    # -- staff management (operator / executor) ---------------------------

    async def create_staff_actor(
        self,
        *,
        organization_id: str,
        role: str,
        name: str,
        phone: str | None = None,
        provider: Provider | None = None,
        external_user_id: str | None = None,
        request_key: str | None = None,
    ) -> dict[str, Any]:
        """Create an operator or master actor with a 4-digit bind code."""
        if role not in {"operator", "master"}:
            raise ValueError("staff role must be operator or master")
        if (provider is None) != (external_user_id is None):
            raise ValueError("provider and external_user_id must be specified together")
        if request_key is not None and not request_key.strip():
            raise ValueError("staff creation request key cannot be blank")
        actor_id = f"{role}:{secrets.token_hex(8)}"
        expires_at = datetime.now(UTC) + timedelta(days=7)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                if request_key is not None:
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"{organization_id}:{request_key}",
                    )
                    existing = await connection.fetchrow(
                        """
                        SELECT id, role, display_name, phone, bind_code
                        FROM actors
                        WHERE organization_id = $1 AND staff_creation_key = $2
                        """,
                        organization_id,
                        request_key,
                    )
                    if existing is not None:
                        if (
                            existing["role"] != role
                            or existing["display_name"] != name
                            or existing["phone"] != phone
                        ):
                            raise ValueError(
                                "staff creation request key was reused with "
                                "different actor data"
                            )
                        return {
                            "actor_id": str(existing["id"]),
                            "role": str(existing["role"]),
                            "name": str(existing["display_name"]),
                            "phone": existing["phone"],
                            "bind_code": existing["bind_code"],
                        }
                bind_code = await self._unused_bind_code(connection, organization_id)
                await connection.execute(
                    """
                    INSERT INTO actors (
                        organization_id, id, role, display_name, phone,
                        bind_code, bind_code_expires_at, active,
                        staff_creation_key
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)
                    """,
                    organization_id,
                    actor_id,
                    role,
                    name,
                    phone,
                    bind_code,
                    expires_at,
                    request_key,
                )
                await connection.execute(
                    """
                    INSERT INTO actor_roles (organization_id, actor_id, role)
                    VALUES ($1, $2, $3)
                    """,
                    organization_id,
                    actor_id,
                    role,
                )
                if provider is not None and external_user_id is not None:
                    existing_actor_id = await connection.fetchval(
                        """
                        SELECT actor_id FROM external_identities
                        WHERE organization_id = $1 AND provider = $2
                          AND external_user_id = $3
                        FOR UPDATE
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                    )
                    if existing_actor_id is not None:
                        raise ValueError(
                            "external account is already bound to another actor"
                        )
                    await connection.execute(
                        """
                        INSERT INTO external_identities (
                            organization_id, provider, external_user_id, actor_id
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (
                            organization_id, provider, external_user_id
                        ) DO NOTHING
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                        actor_id,
                    )
        return {
            "actor_id": actor_id,
            "role": role,
            "name": name,
            "phone": phone,
            "bind_code": bind_code,
        }

    async def list_actors(
        self,
        *,
        organization_id: str,
        role: str,
    ) -> list[dict[str, Any]]:
        """List active actors by role."""
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT a.id, $2::text AS role, a.display_name, a.phone, a.active,
                       a.bind_code IS NOT NULL AS has_bind_code,
                       ARRAY(
                           SELECT assigned.role
                           FROM actor_roles assigned
                           WHERE assigned.organization_id = a.organization_id
                             AND assigned.actor_id = a.id
                             AND assigned.active
                           ORDER BY assigned.role
                       ) AS roles,
                       ARRAY(
                           SELECT DISTINCT e.provider
                           FROM external_identities e
                           WHERE e.organization_id = a.organization_id
                             AND e.actor_id = a.id
                       ) AS channels
                FROM actors a
                JOIN actor_roles membership
                  ON membership.organization_id = a.organization_id
                 AND membership.actor_id = a.id
                 AND membership.role = $2
                 AND membership.active
                WHERE a.organization_id = $1
                  AND a.active
                ORDER BY a.display_name, a.id
                """,
                organization_id,
                role,
            )
        return [dict(row) for row in rows]

    async def get_actor(
        self,
        *,
        organization_id: str,
        actor_id: str,
    ) -> dict[str, Any] | None:
        """Get a single actor by ID."""
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT actor.id, actor.role, actor.display_name, actor.phone,
                       actor.active,
                       ARRAY(
                           SELECT membership.role
                           FROM actor_roles membership
                           WHERE membership.organization_id = actor.organization_id
                             AND membership.actor_id = actor.id
                             AND membership.active
                           ORDER BY membership.role
                       ) AS roles
                FROM actors actor
                WHERE actor.organization_id = $1 AND actor.id = $2
                """,
                organization_id,
                actor_id,
            )
        return dict(row) if row is not None else None

    async def update_actor_phone(
        self,
        *,
        organization_id: str,
        actor_id: str,
        phone: str,
    ) -> bool:
        result: str
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE actors
                SET phone = $3
                WHERE organization_id = $1 AND id = $2 AND active
                """,
                organization_id,
                actor_id,
                phone,
            )
        return result.endswith("1")

    async def delete_actor(
        self,
        *,
        organization_id: str,
        actor_id: str,
    ) -> bool:
        """Deactivate a non-admin actor while preserving its audit history."""
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                is_admin = await connection.fetchval(
                    """
                    SELECT 1 FROM actor_roles
                    WHERE organization_id = $1 AND actor_id = $2
                      AND role = 'admin' AND active
                    """,
                    organization_id,
                    actor_id,
                )
                if is_admin:
                    return False
                result = await connection.execute(
                    """
                    UPDATE actors
                    SET active = false, bind_code = NULL,
                        bind_code_expires_at = NULL
                    WHERE organization_id = $1 AND id = $2 AND active
                    """,
                    organization_id,
                    actor_id,
                )
                await connection.execute(
                    """
                    UPDATE actor_roles
                    SET active = false, revoked_at = now()
                    WHERE organization_id = $1 AND actor_id = $2 AND active
                    """,
                    organization_id,
                    actor_id,
                )
        return result.endswith("1")

    async def grant_role(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
    ) -> bool:
        """Grant or reactivate a role without changing the actor identity."""
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                exists = await connection.fetchval(
                    """
                    SELECT 1 FROM actors
                    WHERE organization_id = $1 AND id = $2 AND active
                    FOR UPDATE
                    """,
                    organization_id,
                    actor_id,
                )
                if not exists:
                    return False
                await connection.execute(
                    """
                    INSERT INTO actor_roles (
                        organization_id, actor_id, role, active, revoked_at
                    ) VALUES ($1, $2, $3, true, NULL)
                    ON CONFLICT (organization_id, actor_id, role) DO UPDATE SET
                        active = true,
                        granted_at = now(),
                        revoked_at = NULL
                    """,
                    organization_id,
                    actor_id,
                    role,
                )
        return True

    async def revoke_role(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
    ) -> bool:
        """Revoke one membership; deactivate only an actor left with no roles."""
        self._validate_role(role)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.execute(
                    """
                    UPDATE actor_roles
                    SET active = false, revoked_at = now()
                    WHERE organization_id = $1 AND actor_id = $2
                      AND role = $3 AND active
                    """,
                    organization_id,
                    actor_id,
                    role,
                )
                if not result.endswith("1"):
                    return False
                remaining = await connection.fetch(
                    """
                    SELECT role FROM actor_roles
                    WHERE organization_id = $1 AND actor_id = $2 AND active
                    ORDER BY CASE role
                        WHEN 'admin' THEN 1
                        WHEN 'operator' THEN 2
                        WHEN 'master' THEN 3
                        ELSE 4
                    END
                    """,
                    organization_id,
                    actor_id,
                )
                if remaining:
                    await connection.execute(
                        """
                        UPDATE actors SET role = $3
                        WHERE organization_id = $1 AND id = $2 AND role = $4
                        """,
                        organization_id,
                        actor_id,
                        remaining[0]["role"],
                        role,
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE actors
                        SET active = false, bind_code = NULL,
                            bind_code_expires_at = NULL
                        WHERE organization_id = $1 AND id = $2
                        """,
                        organization_id,
                        actor_id,
                    )
        return True

    async def bind_actor_by_code(
        self,
        *,
        organization_id: str,
        bind_code: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str = "",
    ) -> ActorIdentity | None:
        """Bind a Telegram/MAX account to an actor via 4-digit code."""
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT id, role, display_name
                    FROM actors
                    WHERE organization_id = $1
                      AND bind_code = $2
                      AND bind_code_expires_at > now()
                      AND active
                    FOR UPDATE
                    """,
                    organization_id,
                    bind_code,
                )
                if row is None:
                    return None
                pending_actor_id = str(row["id"])
                pending_roles = await connection.fetch(
                    """
                    SELECT role FROM actor_roles
                    WHERE organization_id = $1 AND actor_id = $2 AND active
                    ORDER BY role
                    """,
                    organization_id,
                    pending_actor_id,
                )
                pending_role_set = frozenset(
                    str(item["role"]) for item in pending_roles
                )
                requested_role = self._requested_role(consumer_key)
                if consumer_key and consumer_key != "staff" and requested_role is None:
                    return None
                if not pending_role_set or (
                    requested_role is not None
                    and requested_role not in pending_role_set
                ):
                    return None
                if consumer_key == "staff":
                    staff_roles = pending_role_set.intersection(
                        {"admin", "operator", "master"}
                    )
                    if not staff_roles:
                        return None
                existing_actor_id = await connection.fetchval(
                    """
                    SELECT actor_id FROM external_identities
                    WHERE organization_id = $1 AND provider = $2
                      AND external_user_id = $3
                    FOR UPDATE
                    """,
                    organization_id,
                    provider.value,
                    external_user_id,
                )
                actor_id = str(existing_actor_id or pending_actor_id)
                if existing_actor_id and actor_id != pending_actor_id:
                    pending_has_bindings = await connection.fetchval(
                        """
                        SELECT 1 FROM external_identities
                        WHERE organization_id = $1 AND actor_id = $2
                        LIMIT 1
                        """,
                        organization_id,
                        pending_actor_id,
                    )
                    if pending_has_bindings:
                        return None
                    await connection.execute(
                        """
                        INSERT INTO actor_roles (
                            organization_id, actor_id, role, active, revoked_at
                        )
                        SELECT organization_id, $3, role, true, NULL
                        FROM actor_roles
                        WHERE organization_id = $1 AND actor_id = $2 AND active
                        ON CONFLICT (organization_id, actor_id, role) DO UPDATE SET
                            active = true,
                            granted_at = now(),
                            revoked_at = NULL
                        """,
                        organization_id,
                        pending_actor_id,
                        actor_id,
                    )
                    await connection.execute(
                        """
                        UPDATE actors SET active = true
                        WHERE organization_id = $1 AND id = $2
                        """,
                        organization_id,
                        actor_id,
                    )
                    await connection.execute(
                        """
                        DELETE FROM actors
                        WHERE organization_id = $1 AND id = $2
                          AND bind_code = $3
                        """,
                        organization_id,
                        pending_actor_id,
                        bind_code,
                    )
                else:
                    await connection.execute(
                        """
                        INSERT INTO external_identities (
                            organization_id, provider, external_user_id, actor_id
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (
                            organization_id, provider, external_user_id
                        ) DO NOTHING
                        """,
                        organization_id,
                        provider.value,
                        external_user_id,
                        actor_id,
                    )
                    await connection.execute(
                        """
                        UPDATE actors
                        SET bind_code = NULL, bind_code_expires_at = NULL
                        WHERE organization_id = $1 AND id = $2
                        """,
                        organization_id,
                        actor_id,
                    )
                actor = await connection.fetchrow(
                    """
                    SELECT id, role, display_name
                    FROM actors
                    WHERE organization_id = $1 AND id = $2 AND active
                    """,
                    organization_id,
                    actor_id,
                )
                active_roles = await connection.fetch(
                    """
                    SELECT role FROM actor_roles
                    WHERE organization_id = $1 AND actor_id = $2 AND active
                    ORDER BY role
                    """,
                    organization_id,
                    actor_id,
                )
        if actor is None:
            return None
        roles = frozenset(str(item["role"]) for item in active_roles)
        pending_primary_role = str(row["role"])
        pending_role = (
            pending_primary_role
            if pending_primary_role in pending_role_set
            else str(pending_roles[0]["role"])
            if pending_roles
            else ""
        )
        effective_consumer_key = (
            pending_role if consumer_key == "staff" else consumer_key or pending_role
        )
        effective_role = self._effective_role(
            primary_role=str(actor["role"]),
            roles=roles,
            consumer_key=effective_consumer_key,
        )
        if effective_role is None:
            return None
        return ActorIdentity(
            organization_id=organization_id,
            actor_id=actor_id,
            role=effective_role,
            display_name=actor["display_name"],
            provider=provider,
            external_user_id=external_user_id,
            roles=roles,
        )

    async def issue_bind_code(
        self,
        *,
        organization_id: str,
        actor_id: str,
    ) -> str | None:
        """Generate a new 4-digit bind code for an existing actor."""
        expires_at = datetime.now(UTC) + timedelta(days=7)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                bind_code = await self._unused_bind_code(connection, organization_id)
                result = await connection.execute(
                    """
                    UPDATE actors
                    SET bind_code = $3, bind_code_expires_at = $4
                    WHERE organization_id = $1 AND id = $2
                      AND active
                    """,
                    organization_id,
                    actor_id,
                    bind_code,
                    expires_at,
                )
        return bind_code if result.endswith("1") else None

    @staticmethod
    async def _unused_bind_code(
        connection: asyncpg.Connection,
        organization_id: str,
    ) -> str:
        for _ in range(100):
            candidate = f"{secrets.randbelow(10000):04d}"
            exists = await connection.fetchval(
                """
                SELECT 1 FROM actors
                WHERE organization_id = $1 AND bind_code = $2
                """,
                organization_id,
                candidate,
            )
            if not exists:
                return candidate
        raise RuntimeError("could not allocate a unique staff bind code")


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


class PostgresStaffBindingSessionStore:
    """Short-lived pre-authentication state for staff account binding."""

    MAX_ATTEMPTS = 5

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def begin(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str,
        ttl_minutes: int = 15,
    ) -> bool:
        if consumer_key not in {"operator", "master", "staff"}:
            raise ValueError(
                "staff binding requires an operator, master or shared frontend"
            )
        if ttl_minutes <= 0:
            raise ValueError("binding session TTL must be positive")
        async with self._pool.acquire() as connection:
            active = await connection.fetchval(
                """
                INSERT INTO staff_binding_sessions (
                    organization_id, provider, external_user_id,
                    consumer_key, attempts, expires_at
                ) VALUES (
                    $1, $2, $3, $4, 0,
                    now() + make_interval(mins => $5)
                )
                ON CONFLICT (
                    organization_id, provider, external_user_id, consumer_key
                ) DO UPDATE SET
                    attempts = CASE
                        WHEN staff_binding_sessions.expires_at <= now() THEN 0
                        ELSE staff_binding_sessions.attempts
                    END,
                    expires_at = CASE
                        WHEN staff_binding_sessions.expires_at <= now()
                        THEN EXCLUDED.expires_at
                        ELSE staff_binding_sessions.expires_at
                    END,
                    updated_at = now()
                RETURNING expires_at > now() AND attempts < $6
                """,
                organization_id,
                provider.value,
                external_user_id,
                consumer_key,
                ttl_minutes,
                self.MAX_ATTEMPTS,
            )
        return bool(active)

    async def take_attempt(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str,
    ) -> int | None:
        """Atomically reserve one attempt, or return None for an inactive session."""
        async with self._pool.acquire() as connection:
            attempts = await connection.fetchval(
                """
                UPDATE staff_binding_sessions
                SET attempts = attempts + 1, updated_at = now()
                WHERE organization_id = $1
                  AND provider = $2
                  AND external_user_id = $3
                  AND consumer_key = $4
                  AND expires_at > now()
                  AND attempts < $5
                RETURNING attempts
                """,
                organization_id,
                provider.value,
                external_user_id,
                consumer_key,
                self.MAX_ATTEMPTS,
            )
        return int(attempts) if attempts is not None else None

    async def is_active(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str,
    ) -> bool:
        async with self._pool.acquire() as connection:
            active = await connection.fetchval(
                """
                SELECT 1 FROM staff_binding_sessions
                WHERE organization_id = $1
                  AND provider = $2
                  AND external_user_id = $3
                  AND consumer_key = $4
                  AND expires_at > now()
                  AND attempts < $5
                """,
                organization_id,
                provider.value,
                external_user_id,
                consumer_key,
                self.MAX_ATTEMPTS,
            )
        return bool(active)

    async def clear(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        consumer_key: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM staff_binding_sessions
                WHERE organization_id = $1
                  AND provider = $2
                  AND external_user_id = $3
                  AND consumer_key = $4
                """,
                organization_id,
                provider.value,
                external_user_id,
                consumer_key,
            )

    async def cleanup_expired(self) -> int:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                DELETE FROM staff_binding_sessions
                WHERE expires_at <= now()
                """
            )
        parts = result.split()
        return int(parts[-1]) if len(parts) >= 2 else 0


class PostgresStaffRoleSelectionStore:
    """Selected effective role for the shared MAX staff frontend."""

    _roles = frozenset({"admin", "operator", "master"})

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
    ) -> str | None:
        async with self._pool.acquire() as connection:
            role = await connection.fetchval(
                """
                SELECT role FROM staff_role_selections
                WHERE organization_id = $1 AND provider = $2
                  AND external_user_id = $3
                """,
                organization_id,
                provider.value,
                external_user_id,
            )
        return str(role) if role is not None else None

    async def put(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
        role: str,
    ) -> None:
        if provider is not Provider.MAX:
            raise ValueError("shared staff role selection is supported only for MAX")
        if role not in self._roles:
            raise ValueError(f"unsupported shared staff role: {role!r}")
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO staff_role_selections (
                    organization_id, provider, external_user_id, role
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (
                    organization_id, provider, external_user_id
                ) DO UPDATE SET role = EXCLUDED.role, updated_at = now()
                """,
                organization_id,
                provider.value,
                external_user_id,
                role,
            )

    async def clear(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM staff_role_selections
                WHERE organization_id = $1 AND provider = $2
                  AND external_user_id = $3
                """,
                organization_id,
                provider.value,
                external_user_id,
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

    async def get(self, organization_id: str, actor_id: str) -> dict[str, Any] | None:
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
        stored_state = state_with_session_event(state)
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
                json.dumps(stored_state, ensure_ascii=False, separators=(",", ":")),
            )

    async def handled_event(
        self,
        organization_id: str,
        actor_id: str,
        event_id: str,
    ) -> bool:
        state = await self.get(organization_id, actor_id)
        return state_handled_event(state, event_id)

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
            return int(parts[-1]) if len(parts) >= 2 else 0


class PostgresIntakeSessionStore(_PostgresSessionStore):
    _table = "intake_sessions"

    async def select_address_by_token(
        self,
        *,
        token: str,
        latitude: float,
        longitude: float,
        address: str | None = None,
    ) -> IntakeAddressSelection | None:
        label = (address or "").strip()[:500]
        if not label:
            label = f"Точка на карте: {latitude:.5f}, {longitude:.5f}"
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT organization_id, actor_id, provider, state
                    FROM intake_sessions
                    WHERE state ->> 'address_token' = $1
                      AND state ->> 'step' = 'address'
                    FOR UPDATE
                    """,
                    token,
                )
                if row is None:
                    return None
                raw_state = row["state"]
                state = (
                    json.loads(raw_state)
                    if isinstance(raw_state, str)
                    else dict(raw_state)
                )
                field_values = dict(state.get("field_values") or {})
                field_values["address"] = label
                state["field_values"] = field_values
                state["service_location"] = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "method": "map",
                }
                state["step"] = "services"
                state.setdefault("selected", [])
                state.pop("address_token", None)
                state.pop("address_mode", None)
                await connection.execute(
                    """
                    UPDATE intake_sessions
                    SET state = $3::jsonb, updated_at = now()
                    WHERE organization_id = $1 AND actor_id = $2
                    """,
                    row["organization_id"],
                    row["actor_id"],
                    json.dumps(
                        state,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
        return IntakeAddressSelection(
            organization_id=row["organization_id"],
            actor_id=row["actor_id"],
            provider=Provider(row["provider"]),
            address=label,
            latitude=latitude,
            longitude=longitude,
        )


class PostgresConfigSessionStore(_PostgresSessionStore):
    _table = "config_sessions"
