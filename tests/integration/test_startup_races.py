from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest

from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.postgres import PostgresDatabase
from dispatch_core.packs.catalog import seed_definition

pytestmark = pytest.mark.postgres


def _database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is not configured")
    return value


@pytest.mark.asyncio
async def test_concurrent_migrations_are_serialized(tmp_path: Path) -> None:
    suffix = uuid4().hex
    table_name = f"startup_race_{suffix}"
    version = f"startup_race_{suffix}.sql"
    (tmp_path / version).write_text(
        f"CREATE TABLE {table_name} (id integer PRIMARY KEY);",
        encoding="utf-8",
    )
    databases = [
        PostgresDatabase(_database_url(), min_size=1, max_size=1)
        for _ in range(8)
    ]
    await asyncio.gather(*(database.connect() for database in databases))
    try:
        results = await asyncio.gather(
            *(database.migrate(tmp_path) for database in databases)
        )
        assert sum(version in result for result in results) == 1
    finally:
        pool = databases[0].pool
        assert pool is not None
        async with pool.acquire() as connection:
            await connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            await connection.execute(
                "DELETE FROM schema_migrations WHERE version = $1",
                version,
            )
        await asyncio.gather(*(database.close() for database in databases))


@pytest.mark.asyncio
async def test_concurrent_pack_bootstrap_is_idempotent() -> None:
    database = PostgresDatabase(_database_url(), min_size=1, max_size=8)
    await database.connect()
    assert database.pool is not None
    organization_id = f"startup-pack-{uuid4()}"
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "Startup race test",
        )
    try:
        store = PostgresPackStore(database.pool)
        results = await asyncio.gather(
            *(
                store.bootstrap_active(
                    organization_id,
                    seed=seed_definition("field_service"),
                )
                for _ in range(8)
            )
        )
        assert results.count(True) == 1
        async with database.pool.acquire() as connection:
            count = await connection.fetchval(
                "SELECT count(*) FROM org_packs WHERE organization_id = $1",
                organization_id,
            )
        assert count == 1
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )
        await database.close()
