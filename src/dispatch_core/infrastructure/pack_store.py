from __future__ import annotations

import json
from typing import Any

import asyncpg

from dispatch_core.domain.errors import DomainError, NotFound
from dispatch_core.packs.catalog import FieldType, PackDefinition


class PackValidationError(DomainError):
    """A draft pack does not satisfy the requirements for publication."""


def validate_definition(definition: PackDefinition) -> None:
    if not definition.branding.name.strip():
        raise PackValidationError("нужно название бренда")
    if not definition.service_catalog.categories:
        raise PackValidationError("нужна хотя бы одна услуга в каталоге")
    field_keys = {item.key for item in definition.fields}
    if not any(
        item.field_type is FieldType.ADDRESS for item in definition.fields
    ) and "address" not in field_keys:
        raise PackValidationError("нужно хотя бы одно адресное поле")


class PostgresPackStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def active(self, organization_id: str) -> PackDefinition | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT definition FROM org_packs
                WHERE organization_id = $1 AND state = 'active'
                """,
                organization_id,
            )
        if row is None:
            return None
        return PackDefinition.from_json(_json_value(row["definition"]))

    async def draft(self, organization_id: str) -> PackDefinition | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT definition FROM org_packs
                WHERE organization_id = $1 AND state = 'draft'
                """,
                organization_id,
            )
        if row is None:
            return None
        return PackDefinition.from_json(_json_value(row["definition"]))

    async def ensure_draft(
        self,
        organization_id: str,
        *,
        seed: PackDefinition,
    ) -> PackDefinition:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                existing = await connection.fetchrow(
                    """
                    SELECT definition FROM org_packs
                    WHERE organization_id = $1 AND state = 'draft'
                    FOR UPDATE
                    """,
                    organization_id,
                )
                if existing is not None:
                    return PackDefinition.from_json(
                        _json_value(existing["definition"])
                    )
                version = await _next_version(connection, organization_id)
                await connection.execute(
                    """
                    INSERT INTO org_packs (
                        organization_id, version, state, definition
                    ) VALUES ($1, $2, 'draft', $3::jsonb)
                    """,
                    organization_id,
                    version,
                    _json_text(seed.to_json()),
                )
        return seed

    async def update_draft(
        self,
        organization_id: str,
        definition: PackDefinition,
    ) -> None:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE org_packs SET definition = $2::jsonb
                WHERE organization_id = $1 AND state = 'draft'
                """,
                organization_id,
                _json_text(definition.to_json()),
            )
        if result != "UPDATE 1":
            raise NotFound(f"no draft pack for organization {organization_id!r}")

    async def publish_draft(self, organization_id: str) -> int:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                draft = await connection.fetchrow(
                    """
                    SELECT version, definition FROM org_packs
                    WHERE organization_id = $1 AND state = 'draft'
                    FOR UPDATE
                    """,
                    organization_id,
                )
                if draft is None:
                    raise NotFound(
                        f"no draft pack for organization {organization_id!r}"
                    )
                definition = PackDefinition.from_json(
                    _json_value(draft["definition"])
                )
                validate_definition(definition)
                await connection.execute(
                    """
                    UPDATE org_packs SET state = 'archived'
                    WHERE organization_id = $1 AND state = 'active'
                    """,
                    organization_id,
                )
                await connection.execute(
                    """
                    UPDATE org_packs
                    SET state = 'active', activated_at = now()
                    WHERE organization_id = $1 AND version = $2
                    """,
                    organization_id,
                    draft["version"],
                )
        return int(draft["version"])

    async def bootstrap_active(
        self,
        organization_id: str,
        *,
        seed: PackDefinition,
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                INSERT INTO org_packs (
                    organization_id, version, state, definition, activated_at
                )
                SELECT $1, 1, 'active', $2::jsonb, now()
                WHERE NOT EXISTS (
                    SELECT 1 FROM org_packs WHERE organization_id = $1
                )
                ON CONFLICT DO NOTHING
                """,
                organization_id,
                _json_text(seed.to_json()),
            )
        return result == "INSERT 0 1"


async def _next_version(
    connection: asyncpg.Connection, organization_id: str
) -> int:
    value = await connection.fetchval(
        """
        SELECT coalesce(max(version), 0) + 1 FROM org_packs
        WHERE organization_id = $1
        """,
        organization_id,
    )
    return int(value)


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
