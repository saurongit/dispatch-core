from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from dispatch_core.infrastructure.messaging import (
    PostgresOutboundStore,
    PostgresOutboxStore,
)
from dispatch_core.infrastructure.postgres import PostgresDatabase
from dispatch_core.messaging.models import Provider

pytestmark = pytest.mark.postgres


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
async def test_outbound_and_outbox_claims_are_tenant_scoped(
    database: PostgresDatabase,
) -> None:
    assert database.pool is not None
    suffix = uuid4().hex
    organizations = (f"tenant-a-{suffix}", f"tenant-b-{suffix}")
    async with database.pool.acquire() as connection:
        await connection.executemany(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            [(organization, organization) for organization in organizations],
        )
        await connection.executemany(
            """
            INSERT INTO outbox_events (
                event_id, organization_id, aggregate_type, aggregate_id,
                aggregate_version, event_name, payload, occurred_at
            ) VALUES ($1, $2, 'work_order', $3, 1, 'test.event', '{}'::jsonb, now())
            """,
            [
                (f"event-{organization}", organization, f"order-{organization}")
                for organization in organizations
            ],
        )

    outbound = PostgresOutboundStore(database.pool)
    for organization in organizations:
        await outbound.enqueue(
            deduplication_key=f"message-{organization}",
            organization_id=organization,
            provider=Provider.TELEGRAM,
            recipient_id="recipient",
            text=organization,
        )

    outbox_events = await PostgresOutboxStore(database.pool).claim_events(
        organization_id=organizations[0]
    )
    outbound_messages = await outbound.claim(
        Provider.TELEGRAM,
        organization_id=organizations[0],
    )

    assert {event.organization_id for event in outbox_events} == {organizations[0]}
    assert {message.organization_id for message in outbound_messages} == {
        organizations[0]
    }
    async with database.pool.acquire() as connection:
        other_outbox_status = await connection.fetchval(
            "SELECT status FROM outbox_events WHERE organization_id = $1",
            organizations[1],
        )
        other_outbound_status = await connection.fetchval(
            "SELECT status FROM outbound_messages WHERE organization_id = $1",
            organizations[1],
        )
        await connection.execute(
            "DELETE FROM organizations WHERE id = ANY($1::text[])",
            list(organizations),
        )
    assert other_outbox_status == "pending"
    assert other_outbound_status == "pending"
