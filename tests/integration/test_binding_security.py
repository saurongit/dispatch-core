from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from dispatch_core.infrastructure.postgres import PostgresDatabase
from dispatch_core.infrastructure.staff_workflows import (
    PostgresStaffWorkflowSessionStore,
)
from dispatch_core.infrastructure.workflow_store import (
    PostgresIdentityStore,
    PostgresIntakeSessionStore,
    PostgresStaffBindingSessionStore,
    session_event,
)
from dispatch_core.messaging.models import Provider

pytestmark = pytest.mark.postgres


@pytest.mark.asyncio
async def test_binding_attempt_lock_survives_start_until_expiry() -> None:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=2)
    await database.connect()
    migration_directory = (
        Path(__file__).resolve().parents[2]  # noqa: ASYNC240
        / "migrations"
    )
    await database.migrate(migration_directory)
    assert database.pool is not None
    organization_id = f"binding-security-{uuid4()}"
    values = {
        "organization_id": organization_id,
        "provider": Provider.TELEGRAM,
        "external_user_id": "attacker-account",
        "consumer_key": "master",
    }
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "Binding security test",
        )
    try:
        sessions = PostgresStaffBindingSessionStore(database.pool)
        assert await sessions.begin(**values)
        assert [await sessions.take_attempt(**values) for _ in range(5)] == [
            1,
            2,
            3,
            4,
            5,
        ]
        assert not await sessions.begin(**values)
        assert not await sessions.is_active(**values)

        async with database.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE staff_binding_sessions
                SET expires_at = now() - interval '1 second'
                WHERE organization_id = $1
                """,
                organization_id,
            )
        assert await sessions.begin(**values)
        assert await sessions.take_attempt(**values) == 1
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )
        await database.close()


@pytest.mark.asyncio
async def test_executor_identity_requires_an_active_master_role() -> None:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=2)
    await database.connect()
    migration_directory = (
        Path(__file__).resolve().parents[2]  # noqa: ASYNC240
        / "migrations"
    )
    await database.migrate(migration_directory)
    assert database.pool is not None
    organization_id = f"executor-auth-{uuid4()}"
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "Executor auth test",
        )
    try:
        identities = PostgresIdentityStore(database.pool)
        await identities.upsert_actor(
            organization_id=organization_id,
            actor_id="master-1",
            role="master",
            display_name="Master",
            provider=Provider.MAX,
            external_user_id="max-master-1",
        )
        resolved = await identities.resolve_actor_id(
            organization_id=organization_id,
            actor_id="master-1",
            required_role="master",
        )
        assert resolved is not None
        assert resolved.provider is Provider.MAX
        assert resolved.external_user_id == "max-master-1"

        assert await identities.revoke_role(
            organization_id=organization_id,
            actor_id="master-1",
            role="master",
        )
        assert (
            await identities.resolve_actor_id(
                organization_id=organization_id,
                actor_id="master-1",
                required_role="master",
            )
            is None
        )
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )
        await database.close()


@pytest.mark.asyncio
async def test_fsm_state_records_the_inbound_event_atomically() -> None:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=2)
    await database.connect()
    migration_directory = (
        Path(__file__).resolve().parents[2]  # noqa: ASYNC240
        / "migrations"
    )
    await database.migrate(migration_directory)
    assert database.pool is not None
    organization_id = f"fsm-idempotency-{uuid4()}"
    actor_id = "client-1"
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "FSM idempotency test",
        )
        await connection.execute(
            """
            INSERT INTO actors (organization_id, id, role, display_name)
            VALUES ($1, $2, 'client', 'Client')
            """,
            organization_id,
            actor_id,
        )
    try:
        sessions = PostgresIntakeSessionStore(database.pool)
        with session_event("telegram:42:0"):
            await sessions.put(
                organization_id=organization_id,
                actor_id=actor_id,
                provider=Provider.TELEGRAM,
                state={"step": "address", "field_values": {"phone": "+7999"}},
            )

        assert await sessions.handled_event(
            organization_id,
            actor_id,
            "telegram:42:0",
        )
        state = await sessions.get(organization_id, actor_id)
        assert state is not None
        assert state["step"] == "address"
        assert state["field_values"]["phone"] == "+7999"
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )
        await database.close()


@pytest.mark.asyncio
async def test_operator_fsm_state_records_the_inbound_event_atomically() -> None:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=2)
    await database.connect()
    migration_directory = (
        Path(__file__).resolve().parents[2]  # noqa: ASYNC240
        / "migrations"
    )
    await database.migrate(migration_directory)
    assert database.pool is not None
    organization_id = f"operator-fsm-idempotency-{uuid4()}"
    actor_id = "operator-1"
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "Operator FSM idempotency test",
        )
        await connection.execute(
            """
            INSERT INTO actors (organization_id, id, role, display_name)
            VALUES ($1, $2, 'operator', 'Operator')
            """,
            organization_id,
            actor_id,
        )
    try:
        sessions = PostgresStaffWorkflowSessionStore(database.pool)
        with session_event("telegram:operator:42:0"):
            await sessions.put(
                organization_id=organization_id,
                actor_id=actor_id,
                role="operator",
                provider=Provider.TELEGRAM,
                state={"flow": "add_master", "step": "phone", "name": "Ivan"},
            )

        assert await sessions.handled_event(
            organization_id=organization_id,
            actor_id=actor_id,
            role="operator",
            provider=Provider.TELEGRAM,
            event_id="telegram:operator:42:0",
        )
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )
        await database.close()
