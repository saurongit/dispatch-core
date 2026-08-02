from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import asyncpg

from dispatch_core.domain.errors import DomainError, NotFound
from dispatch_core.packs.catalog import FieldType, PackDefinition


class PackValidationError(DomainError):
    """A draft pack does not satisfy the requirements for publication."""


@dataclass(frozen=True, slots=True)
class PackRevision:
    version: int
    state: str
    definition: PackDefinition


def validate_definition(definition: PackDefinition) -> None:
    if not definition.branding.name.strip():
        raise PackValidationError("нужно название бренда")
    if not definition.service_catalog.categories:
        raise PackValidationError("нужна хотя бы одна услуга в каталоге")
    field_keys = {item.key for item in definition.fields}
    if (
        not any(item.field_type is FieldType.ADDRESS for item in definition.fields)
        and "address" not in field_keys
    ):
        raise PackValidationError("нужно хотя бы одно адресное поле")


class PostgresPackStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def active(self, organization_id: str) -> PackDefinition | None:
        revision = await self.active_revision(organization_id)
        return revision.definition if revision is not None else None

    async def active_revision(self, organization_id: str) -> PackRevision | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT version, state, definition FROM org_packs
                WHERE organization_id = $1 AND state = 'active'
                """,
                organization_id,
            )
        if row is None:
            return None
        return _revision_from_row(row)

    async def revision(
        self,
        organization_id: str,
        version: int,
    ) -> PackRevision | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT version, state, definition FROM org_packs
                WHERE organization_id = $1 AND version = $2
                  AND state IN ('active', 'archived')
                """,
                organization_id,
                version,
            )
        if row is None:
            return None
        return _revision_from_row(row)

    async def revisions(self, organization_id: str) -> tuple[PackRevision, ...]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT version, state, definition FROM org_packs
                WHERE organization_id = $1
                ORDER BY version DESC
                """,
                organization_id,
            )
        return tuple(_revision_from_row(row) for row in rows)

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
                    return PackDefinition.from_json(_json_value(existing["definition"]))
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

    async def discard_draft(self, organization_id: str) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                DELETE FROM org_packs
                WHERE organization_id = $1 AND state = 'draft'
                """,
                organization_id,
            )
        return result == "DELETE 1"

    async def restore_as_draft(
        self,
        organization_id: str,
        version: int,
    ) -> int:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                source = await connection.fetchrow(
                    """
                    SELECT definition FROM org_packs
                    WHERE organization_id = $1 AND version = $2
                      AND state IN ('active', 'archived')
                    """,
                    organization_id,
                    version,
                )
                if source is None:
                    raise NotFound(
                        f"pack version {version!r} was not found for "
                        f"organization {organization_id!r}"
                    )
                draft_version = await connection.fetchval(
                    """
                    SELECT version FROM org_packs
                    WHERE organization_id = $1 AND state = 'draft'
                    FOR UPDATE
                    """,
                    organization_id,
                )
                if draft_version is None:
                    draft_version = await _next_version(connection, organization_id)
                    await connection.execute(
                        """
                        INSERT INTO org_packs (
                            organization_id, version, state, definition
                        ) VALUES ($1, $2, 'draft', $3::jsonb)
                        """,
                        organization_id,
                        draft_version,
                        _json_text(_json_value(source["definition"])),
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE org_packs
                        SET definition = $3::jsonb
                        WHERE organization_id = $1 AND version = $2
                        """,
                        organization_id,
                        draft_version,
                        _json_text(_json_value(source["definition"])),
                    )
        return int(draft_version)

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
                definition = PackDefinition.from_json(_json_value(draft["definition"]))
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


async def _next_version(connection: asyncpg.Connection, organization_id: str) -> int:
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


def _revision_from_row(row: Any) -> PackRevision:
    return PackRevision(
        version=int(row["version"]),
        state=str(row["state"]),
        definition=PackDefinition.from_json(_json_value(row["definition"])),
    )
