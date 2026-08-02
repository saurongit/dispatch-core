from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from dispatch_core.api.settings import Settings
from dispatch_core.infrastructure.operations import PostgresOperationsStore
from dispatch_core.infrastructure.postgres import PostgresDatabase
from dispatch_core.runtime.healthcheck import worker_is_healthy

pytestmark = pytest.mark.postgres

ADMIN_KEY = "operations-test-admin-key-at-least-32-characters"


@pytest.fixture
async def database() -> PostgresDatabase:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=4)
    await database.connect()
    migration_directory = (
        Path(__file__).resolve().parents[2]  # noqa: ASYNC240
        / "migrations"
    )
    await database.migrate(migration_directory)
    try:
        yield database
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_operations_snapshot_heartbeat_and_safe_cleanup(
    database: PostgresDatabase,
) -> None:
    assert database.pool is not None
    organization_id = f"operations-{uuid4().hex}"
    store = PostgresOperationsStore(database.pool)
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, 'Operations')",
            organization_id,
        )
        await connection.execute(
            """
            INSERT INTO inbox_events (
                provider, external_event_id, organization_id, payload,
                status, received_at, processed_at, consumer_key
            ) VALUES
                ('telegram', 'processed-old', $1, '{}'::jsonb, 'processed',
                 now() - interval '40 days', now() - interval '40 days', 'client'),
                ('telegram', 'dead-kept', $1, '{}'::jsonb, 'dead',
                 now() - interval '40 days', NULL, 'client')
            """,
            organization_id,
        )
        await connection.execute(
            """
            INSERT INTO outbox_events (
                event_id, organization_id, aggregate_type, aggregate_id,
                aggregate_version, event_name, payload, occurred_at,
                status, delivered_at
            ) VALUES (
                'outbox-old', $1, 'work_order', 'order-old', 1,
                'test.old', '{}'::jsonb, now() - interval '40 days',
                'delivered', now() - interval '40 days'
            )
            """,
            organization_id,
        )
        await connection.execute(
            """
            INSERT INTO outbound_messages (
                deduplication_key, organization_id, provider, recipient_id,
                text_body, status, delivered_at, created_at
            ) VALUES (
                'outbound-old', $1, 'telegram', 'recipient', 'done',
                'delivered', now() - interval '40 days',
                now() - interval '40 days'
            )
            """,
            organization_id,
        )
        await connection.execute(
            """
            INSERT INTO idempotency_keys (
                organization_id, scope, idempotency_key, request_hash, expires_at
            ) VALUES ($1, 'test', 'expired', 'hash', now() - interval '1 day')
            """,
            organization_id,
        )
        await connection.execute(
            """
            INSERT INTO callback_actions (
                token, organization_id, action, payload, expires_at
            ) VALUES (
                'callback-old', $1, 'test', '{}'::jsonb,
                now() - interval '40 days'
            )
            """,
            organization_id,
        )

    await store.heartbeat(
        organization_id=organization_id,
        consumer_key="client",
        instance_id="worker-1",
    )
    assert await store.worker_is_healthy(
        organization_id=organization_id,
        consumer_key="client",
        instance_id="worker-1",
        stale_after_seconds=45,
    )
    snapshot = await store.snapshot(
        organization_id,
        worker_stale_after_seconds=45,
    )
    assert any(
        item.queue == "inbox" and item.status == "dead" and item.count == 1
        for item in snapshot.queues
    )
    assert snapshot.workers[0].healthy is True

    deleted = await store.cleanup_terminal(organization_id, retention_days=30)
    assert deleted == {
        "inbox": 1,
        "outbox": 1,
        "outbound": 1,
        "idempotency": 1,
        "callbacks": 1,
        "heartbeats": 0,
    }
    async with database.pool.acquire() as connection:
        assert await connection.fetchval(
            "SELECT count(*) FROM inbox_events WHERE organization_id = $1",
            organization_id,
        ) == 1

    settings = Settings(
        database_url=os.environ["TEST_DATABASE_URL"],
        admin_api_key=ADMIN_KEY,
        organization_id=organization_id,
        consumer_key="client",
        worker_instance_id="worker-1",
    )
    assert await worker_is_healthy(settings)
    await store.remove_worker(
        organization_id=organization_id,
        consumer_key="client",
        instance_id="worker-1",
    )
    assert not await worker_is_healthy(settings)
    async with database.pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM organizations WHERE id = $1",
            organization_id,
        )


@pytest.mark.asyncio
async def test_operations_cleanup_validates_limits(database: PostgresDatabase) -> None:
    assert database.pool is not None
    store = PostgresOperationsStore(database.pool)
    with pytest.raises(ValueError, match="retention_days"):
        await store.cleanup_terminal("unused", retention_days=0)
    with pytest.raises(ValueError, match="batch_size"):
        await store.cleanup_terminal("unused", retention_days=1, batch_size=0)
