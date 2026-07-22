from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from dispatch_core.api import create_app
from dispatch_core.api.settings import Settings
from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.domain.errors import ConcurrencyConflict, InvalidTransition, NotFound
from dispatch_core.domain.tracking import LocationSource
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    PoolMode,
    WorkOrderStatus,
)
from dispatch_core.infrastructure.messaging import (
    PostgresCallbackStore,
    PostgresInboxStore,
    PostgresOutboundStore,
    PostgresOutboxStore,
)
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.postgres import (
    PostgresDatabase,
    PostgresUnitOfWorkFactory,
)
from dispatch_core.infrastructure.read_models import PostgresOrderReader
from dispatch_core.infrastructure.staff_workflows import (
    PostgresStaffViewStore,
    PostgresStaffWorkflowSessionStore,
)
from dispatch_core.infrastructure.workflow_store import (
    PostgresExecutionStore,
    PostgresIdentityStore,
    PostgresIntakeSessionStore,
    PostgresReportDraftStore,
    PostgresStaffBindingSessionStore,
    PostgresStaffRoleSelectionStore,
)
from dispatch_core.messaging.intake import IntakeCoordinator
from dispatch_core.messaging.models import OutboundButton, Provider
from dispatch_core.messaging.processor import InboundProcessor
from dispatch_core.messaging.projector import PostgresNotificationProjector
from dispatch_core.messaging.staff import StaffRoleCoordinator
from dispatch_core.messaging.workspaces import MasterCoordinator, OperatorCoordinator
from dispatch_core.packs.catalog import seed_definition
from dispatch_core.runtime.worker import run_worker
from dispatch_core.transports.max import MaxTransport
from dispatch_core.transports.telegram import TelegramTransport

pytestmark = pytest.mark.postgres


@pytest.fixture
async def database() -> PostgresDatabase:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    database = PostgresDatabase(dsn, min_size=1, max_size=12)
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


@pytest.fixture
async def organization(database: PostgresDatabase) -> str:
    organization_id = f"test-{uuid4()}"
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO organizations(id, name) VALUES ($1, $2)",
            organization_id,
            "Integration Test",
        )
    try:
        yield organization_id
    finally:
        async with database.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM organizations WHERE id = $1",
                organization_id,
            )


def service(database: PostgresDatabase) -> AsyncDispatchService:
    assert database.pool is not None
    return AsyncDispatchService(PostgresUnitOfWorkFactory(database.pool))


async def create_order(
    database: PostgresDatabase,
    organization_id: str,
    order_id: str,
) -> None:
    await service(database).create_order(
        organization_id=organization_id,
        order_id=order_id,
        work_type="repair",
        source="phone",
        details={"asset": order_id},
    )


@pytest.mark.asyncio
async def test_migrations_are_idempotent(database: PostgresDatabase) -> None:
    directory = Path(__file__).resolve().parents[2] / "migrations"  # noqa: ASYNC240
    assert await database.migrate(directory) == ()


@pytest.mark.asyncio
async def test_production_api_lifespan_wires_real_postgres(
    database: PostgresDatabase,
) -> None:
    organization_id = f"api-{uuid4()}"
    settings = Settings(
        database_url=os.environ["TEST_DATABASE_URL"],
        admin_api_key="integration-admin-key-with-32-characters",
        organization_id=organization_id,
        organization_name="Production Wiring Test",
        auto_migrate=False,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            ready = await client.get("/health/ready")
            created = await client.post(
                "/v1/orders",
                headers={
                    "Authorization": (
                        "Bearer integration-admin-key-with-32-characters"
                    ),
                    "Idempotency-Key": "production-wiring-order",
                },
                json={
                    "work_type": "repair",
                    "source": "api",
                    "details": {"asset": "lift-1"},
                },
            )
    assert ready.status_code == 200
    assert created.status_code == 201
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM organizations WHERE id = $1",
            organization_id,
        )


@pytest.mark.asyncio
async def test_worker_wires_both_polling_transports_and_stops_cleanly(
    database: PostgresDatabase,
) -> None:
    organization_id = f"worker-{uuid4()}"
    settings = Settings(
        database_url=os.environ["TEST_DATABASE_URL"],
        admin_api_key="integration-admin-key-with-32-characters",
        organization_id=organization_id,
        organization_name="Worker Wiring Test",
        auto_migrate=False,
        telegram_receive_mode="polling",
        telegram_bot_token="telegram-test-token",
        max_receive_mode="polling",
        max_bot_token="max-test-token",
    )
    stop = asyncio.Event()
    stop.set()
    await run_worker(settings, stop_event=stop)
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT name FROM organizations WHERE id = $1",
                organization_id,
            )
            == "Worker Wiring Test"
        )
        await connection.execute(
            "DELETE FROM organizations WHERE id = $1",
            organization_id,
        )


@pytest.mark.asyncio
async def test_create_round_trip_and_outbox_share_commit(
    database: PostgresDatabase,
    organization: str,
) -> None:
    await create_order(database, organization, "order-1")
    assert database.pool is not None
    loaded = await PostgresOrderReader(database.pool).get(organization, "order-1")
    async with database.pool.acquire() as connection:
        outbox_count = await connection.fetchval(
            "SELECT count(*) FROM outbox_events WHERE organization_id = $1",
            organization,
        )
    assert loaded.status is WorkOrderStatus.SUBMITTED
    assert dict(loaded.details) == {"asset": "order-1"}
    assert outbox_count == 1


@pytest.mark.asyncio
async def test_tenant_scope_hides_same_order_id_in_another_organization(
    database: PostgresDatabase,
    organization: str,
) -> None:
    await create_order(database, organization, "same-id")
    assert database.pool is not None
    with pytest.raises(NotFound):
        await PostgresOrderReader(database.pool).get("another-org", "same-id")


@pytest.mark.asyncio
async def test_duplicate_order_creation_is_atomic_conflict(
    database: PostgresDatabase,
    organization: str,
) -> None:
    await create_order(database, organization, "order-1")
    with pytest.raises(ConcurrencyConflict):
        await create_order(database, organization, "order-1")
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM work_orders WHERE organization_id = $1",
                organization,
            )
            == 1
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM outbox_events WHERE organization_id = $1",
                organization,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_first_claim_race_has_one_database_winner(
    database: PostgresDatabase,
    organization: str,
) -> None:
    commands = service(database)
    await create_order(database, organization, "order-1")
    await commands.publish_pool(organization, "order-1", PoolMode.FIRST_CLAIM)

    async def claim(executor_id: str) -> object:
        try:
            return await service(database).claim_first(
                organization, "order-1", executor_id
            )
        except (ConcurrencyConflict, InvalidTransition) as exc:
            return exc

    results = await asyncio.gather(
        claim("executor-1"),
        claim("executor-2"),
        claim("executor-3"),
        claim("executor-4"),
    )
    winners = [result for result in results if not isinstance(result, Exception)]
    losers = [result for result in results if isinstance(result, Exception)]
    assert len(winners) == 1
    assert len(losers) == 3
    assert winners[0].assignee_id in {
        "executor-1",
        "executor-2",
        "executor-3",
        "executor-4",
    }


@pytest.mark.asyncio
async def test_curated_interest_race_preserves_every_distinct_response(
    database: PostgresDatabase,
    organization: str,
) -> None:
    await create_order(database, organization, "order-1")
    await service(database).publish_pool(organization, "order-1", PoolMode.CURATED)
    await asyncio.gather(
        *(
            service(database).express_interest(
                organization, "order-1", f"executor-{number}"
            )
            for number in range(1, 9)
        )
    )
    assert database.pool is not None
    loaded = await PostgresOrderReader(database.pool).get(organization, "order-1")
    assert set(loaded.interested_executor_ids()) == {
        f"executor-{number}" for number in range(1, 9)
    }


@pytest.mark.asyncio
async def test_partial_unique_index_prevents_executor_double_booking(
    database: PostgresDatabase,
    organization: str,
) -> None:
    await create_order(database, organization, "order-1")
    await create_order(database, organization, "order-2")

    async def assign(order_id: str) -> object:
        try:
            return await service(database).assign_order(
                organization, order_id, "executor-1"
            )
        except ConcurrencyConflict as exc:
            return exc

    results = await asyncio.gather(assign("order-1"), assign("order-2"))
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(isinstance(result, ConcurrencyConflict) for result in results) == 1


@pytest.mark.asyncio
async def test_completed_order_releases_executor_capacity(
    database: PostgresDatabase,
    organization: str,
) -> None:
    commands = service(database)
    await create_order(database, organization, "order-1")
    await create_order(database, organization, "order-2")
    await commands.assign_order(organization, "order-1", "executor-1")
    await commands.accept_order(organization, "order-1", "executor-1")
    await commands.start_work(organization, "order-1", "executor-1")
    await commands.complete_order(
        organization,
        "order-1",
        "executor-1",
        CompletionReport(),
    )
    second = await commands.assign_order(organization, "order-2", "executor-1")
    assert second.assignee_id == "executor-1"


@pytest.mark.asyncio
async def test_tracking_and_completion_commit_all_events(
    database: PostgresDatabase,
    organization: str,
) -> None:
    commands = service(database)
    await create_order(database, organization, "order-1")
    await commands.assign_order(organization, "order-1", "executor-1")
    await commands.accept_order(organization, "order-1", "executor-1")
    _, tracking = await commands.start_travel(
        organization,
        "order-1",
        "executor-1",
        session_id="track-1",
    )
    await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
        executor_id="executor-1",
        latitude=53.75,
        longitude=87.1,
        source=LocationSource.TELEGRAM,
        source_event_id="telegram:location-1",
    )
    await commands.start_work(organization, "order-1", "executor-1")
    await commands.complete_order(
        organization,
        "order-1",
        "executor-1",
        CompletionReport(),
    )
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        tracking_status = await connection.fetchval(
            """
            SELECT status FROM tracking_sessions
            WHERE organization_id = $1 AND id = 'track-1'
            """,
            organization,
        )
        point_count = await connection.fetchval(
            """
            SELECT count(*) FROM tracking_points
            WHERE organization_id = $1 AND session_id = 'track-1'
            """,
            organization,
        )
        event_count = await connection.fetchval(
            "SELECT count(*) FROM outbox_events WHERE organization_id = $1",
            organization,
        )
    assert tracking_status == "completed"
    assert point_count == 1
    assert event_count == 9


@pytest.mark.asyncio
async def test_tracking_points_are_appended_without_rewriting_history(
    database: PostgresDatabase,
    organization: str,
) -> None:
    commands = service(database)
    await create_order(database, organization, "order-append-track")
    await commands.assign_order(organization, "order-append-track", "executor-append")
    await commands.accept_order(organization, "order-append-track", "executor-append")
    _, tracking = await commands.start_travel(
        organization,
        "order-append-track",
        "executor-append",
        session_id="track-append",
    )
    await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
        executor_id="executor-append",
        latitude=53.75,
        longitude=87.1,
        source=LocationSource.TELEGRAM,
        source_event_id="telegram:append-location-1",
    )
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        first_xmin = await connection.fetchval(
            """
            SELECT xmin::text FROM tracking_points
            WHERE organization_id = $1 AND session_id = $2 AND sequence_no = 1
            """,
            organization,
            tracking.id,
        )
    await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
        executor_id="executor-append",
        latitude=53.76,
        longitude=87.11,
        source=LocationSource.MAX,
        source_event_id="max:location-2",
    )
    duplicate = await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
        executor_id="executor-append",
        latitude=53.75,
        longitude=87.1,
        source=LocationSource.TELEGRAM,
        source_event_id="telegram:append-location-1",
    )
    async with database.pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT sequence_no, xmin::text AS xmin FROM tracking_points
            WHERE organization_id = $1 AND session_id = $2
            ORDER BY sequence_no
            """,
            organization,
            tracking.id,
        )
    assert [row["sequence_no"] for row in rows] == [1, 2]
    assert rows[0]["xmin"] == first_xmin
    assert len(duplicate.points) == 2


@pytest.mark.asyncio
async def test_inbox_accept_is_durable_and_deduplicated(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    inbox = PostgresInboxStore(database.pool)
    first = await inbox.accept(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:1",
        organization_id=organization,
        payload={"update_id": 1},
    )
    duplicate = await inbox.accept(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:1",
        organization_id=organization,
        payload={"update_id": 1, "changed": True},
    )
    claimed = await inbox.claim()
    assert first is True
    assert duplicate is False
    assert len(claimed) == 1
    assert claimed[0].payload == {"update_id": 1}


@pytest.mark.asyncio
async def test_inbox_isolates_identical_provider_updates_by_frontend(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    inbox = PostgresInboxStore(database.pool)
    values = {
        "provider": Provider.MAX,
        "external_event_id": "max:same-update",
        "organization_id": organization,
        "payload": {"update_type": "bot_started"},
    }
    assert await inbox.accept(**values, consumer_key="client")
    assert await inbox.accept(**values, consumer_key="staff")
    assert not await inbox.accept(**values, consumer_key="client")

    client_items = await inbox.claim(
        organization_id=organization,
        consumer_key="client",
    )
    staff_items = await inbox.claim(
        organization_id=organization,
        consumer_key="staff",
    )
    assert [item.consumer_key for item in client_items] == ["client"]
    assert [item.consumer_key for item in staff_items] == ["staff"]


@pytest.mark.asyncio
async def test_poll_batch_and_cursor_commit_together(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    inbox = PostgresInboxStore(database.pool)
    inserted = await inbox.accept_poll_batch(
        provider=Provider.TELEGRAM,
        organization_id=organization,
        consumer_key=organization,
        events=[
            ("telegram:10", {"update_id": 10}),
            ("telegram:11", {"update_id": 11}),
        ],
        next_cursor="12",
    )
    assert inserted == 2
    assert await inbox.get_cursor(Provider.TELEGRAM, organization) == "12"
    assert {
        item.external_event_id for item in await inbox.claim(consumer_key=organization)
    } == {
        "telegram:10",
        "telegram:11",
    }


@pytest.mark.asyncio
async def test_concurrent_inbox_claimers_never_receive_same_event(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    inbox = PostgresInboxStore(database.pool)
    for number in range(20):
        await inbox.accept(
            provider=Provider.MAX,
            external_event_id=f"max:{number}",
            organization_id=organization,
            payload={"number": number},
        )
    batches = await asyncio.gather(
        inbox.claim(limit=10),
        inbox.claim(limit=10),
    )
    identifiers = [item.external_event_id for batch in batches for item in batch]
    assert len(identifiers) == 20
    assert len(set(identifiers)) == 20


@pytest.mark.asyncio
async def test_inbox_failure_moves_to_dead_letter_at_limit(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    inbox = PostgresInboxStore(database.pool)
    await inbox.accept(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:dead",
        organization_id=organization,
        payload={},
    )
    item = (await inbox.claim())[0]
    await inbox.mark_failed(item, "permanent failure", max_attempts=1)
    async with database.pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT status, last_error FROM inbox_events
            WHERE provider = 'telegram' AND external_event_id = 'telegram:dead'
            """
        )
    assert row["status"] == "dead"
    assert row["last_error"] == "permanent failure"


@pytest.mark.asyncio
async def test_outbox_claimers_use_skip_locked(
    database: PostgresDatabase,
    organization: str,
) -> None:
    for number in range(12):
        await create_order(database, organization, f"order-{number}")
    assert database.pool is not None
    outbox = PostgresOutboxStore(database.pool)
    batches = await asyncio.gather(
        outbox.claim_events(limit=6),
        outbox.claim_events(limit=6),
    )
    identifiers = [event.event_id for batch in batches for event in batch]
    assert len(identifiers) == 12
    assert len(set(identifiers)) == 12


@pytest.mark.asyncio
async def test_outbound_queue_deduplicates_and_tracks_delivery(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    outbound = PostgresOutboundStore(database.pool)
    values = {
        "deduplication_key": "event-1:telegram:user-1",
        "organization_id": organization,
        "provider": Provider.TELEGRAM,
        "recipient_id": "7001",
        "text": "Новая заявка",
        "buttons": (OutboundButton("Готов взять", callback_token="token-1"),),
    }
    assert await outbound.enqueue(**values) is True
    assert await outbound.enqueue(**values) is False
    message = (await outbound.claim(Provider.TELEGRAM))[0]
    await outbound.mark_delivered(message, "external-77")
    async with database.pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT status, external_message_id FROM outbound_messages
            WHERE deduplication_key = $1
            """,
            values["deduplication_key"],
        )
    assert row["status"] == "delivered"
    assert row["external_message_id"] == "external-77"


@pytest.mark.asyncio
async def test_shared_max_sender_claims_staff_roles_but_not_client_queue(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    outbound = PostgresOutboundStore(database.pool)
    for role in ("staff", "admin", "operator", "master", "client"):
        assert await outbound.enqueue(
            deduplication_key=f"shared-max:{role}",
            organization_id=organization,
            provider=Provider.MAX,
            recipient_id=f"recipient-{role}",
            text=role,
            consumer_key=role,
        )

    claimed = await outbound.claim(
        Provider.MAX,
        consumer_keys=("staff", "admin", "operator", "master"),
    )
    assert {message.consumer_key for message in claimed} == {
        "staff",
        "admin",
        "operator",
        "master",
    }
    client = await outbound.claim(Provider.MAX, consumer_key="client")
    assert [message.consumer_key for message in client] == ["client"]


@pytest.mark.asyncio
async def test_staff_workspace_sessions_and_order_views_are_role_scoped(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="nano-workspace-owner",
        role="admin",
        display_name="Nano Owner",
    )
    for role in ("operator", "master"):
        assert await identities.grant_role(
            organization_id=organization,
            actor_id="nano-workspace-owner",
            role=role,
        )
    sessions = PostgresStaffWorkflowSessionStore(pool)
    for role, provider, marker in (
        ("operator", Provider.TELEGRAM, "operator-telegram"),
        ("operator", Provider.MAX, "operator-max"),
        ("master", Provider.TELEGRAM, "master-telegram"),
    ):
        await sessions.put(
            organization_id=organization,
            actor_id="nano-workspace-owner",
            role=role,
            provider=provider,
            state={"marker": marker},
        )
    assert await sessions.get(
        organization_id=organization,
        actor_id="nano-workspace-owner",
        role="operator",
        provider=Provider.TELEGRAM,
    ) == {"marker": "operator-telegram"}
    assert await sessions.get(
        organization_id=organization,
        actor_id="nano-workspace-owner",
        role="operator",
        provider=Provider.MAX,
    ) == {"marker": "operator-max"}
    assert await sessions.get(
        organization_id=organization,
        actor_id="nano-workspace-owner",
        role="master",
        provider=Provider.TELEGRAM,
    ) == {"marker": "master-telegram"}

    for actor_id in ("workspace-master-1", "workspace-master-2"):
        await identities.upsert_actor(
            organization_id=organization,
            actor_id=actor_id,
            role="master",
            display_name=actor_id,
        )
    commands = service(database)
    await commands.create_order(
        organization_id=organization,
        order_id="workspace-order-1",
        work_type="repair",
        source="phone",
        details={"summary": "Scoped order"},
    )
    await commands.assign_order(
        organization,
        "workspace-order-1",
        "workspace-master-1",
        actor_id="nano-workspace-owner",
    )
    views = PostgresStaffViewStore(pool)
    first = await views.list_active_orders(
        organization_id=organization,
        role="master",
        actor_id="workspace-master-1",
    )
    second = await views.list_active_orders(
        organization_id=organization,
        role="master",
        actor_id="workspace-master-2",
    )
    operator_orders = await views.list_active_orders(
        organization_id=organization,
        role="operator",
        actor_id="nano-workspace-owner",
    )
    assert [item["id"] for item in first] == ["workspace-order-1"]
    assert second == []
    assert [item["id"] for item in operator_orders] == ["workspace-order-1"]
    assert await views.master_has_active_orders(
        organization_id=organization,
        master_id="workspace-master-1",
    )
    assert not await views.master_has_active_orders(
        organization_id=organization,
        master_id="workspace-master-2",
    )
    stats = await views.statistics(organization_id=organization)
    assert stats["active"] == 1
    assert stats["masters_total"] == 3


@pytest.mark.asyncio
async def test_callback_token_is_opaque_role_scoped_and_expiring(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    callbacks = PostgresCallbackStore(database.pool)
    created = await callbacks.create(
        organization_id=organization,
        action="pool_interest",
        payload={"order_id": "order-1"},
        allowed_role="master",
    )
    resolved = await callbacks.resolve(
        token=created.token,
        organization_id=organization,
        actor_role="master",
    )
    denied = await callbacks.resolve(
        token=created.token,
        organization_id=organization,
        actor_role="operator",
    )
    assert resolved is not None
    assert dict(resolved.payload) == {"order_id": "order-1"}
    assert denied is None


@pytest.mark.asyncio
async def test_identity_binding_and_report_draft_round_trip(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    drafts = PostgresReportDraftStore(database.pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="executor-1",
        role="master",
        display_name="Executor One",
        provider=Provider.TELEGRAM,
        external_user_id="7001",
    )
    resolved = await identities.resolve(
        organization_id=organization,
        provider=Provider.TELEGRAM,
        external_user_id="7001",
    )
    await create_order(database, organization, "order-1")
    assert (
        await drafts.append_photo(
            organization_id=organization,
            order_id="order-1",
            executor_id="executor-1",
            photo_ref="telegram:photo-1",
        )
        == 1
    )
    assert (
        await drafts.append_photo(
            organization_id=organization,
            order_id="order-1",
            executor_id="executor-1",
            photo_ref="telegram:photo-2",
        )
        == 2
    )
    assert (
        await drafts.append_photo(
            organization_id=organization,
            order_id="order-1",
            executor_id="executor-1",
            photo_ref="telegram:photo-1",
        )
        == 2
    )
    await drafts.set_comment(
        organization_id=organization,
        order_id="order-1",
        executor_id="executor-1",
        comment="done",
    )
    report = await drafts.get(
        organization_id=organization,
        order_id="order-1",
        executor_id="executor-1",
    )
    assert resolved is not None
    assert resolved.actor_id == "executor-1"
    assert report.photo_refs == ("telegram:photo-1", "telegram:photo-2")
    assert report.comment == "done"


@pytest.mark.asyncio
async def test_staff_creation_request_is_idempotent(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    values = {
        "organization_id": organization,
        "role": "master",
        "name": "Idempotent Master",
        "phone": "+79991112233",
        "request_key": "operator-1:add-master:stable-request",
    }
    first = await identities.create_staff_actor(**values)
    duplicate = await identities.create_staff_actor(**values)
    with pytest.raises(ValueError, match="different actor data"):
        await identities.create_staff_actor(**{**values, "phone": "+70000000000"})

    async with database.pool.acquire() as connection:
        count = await connection.fetchval(
            """
            SELECT count(*) FROM actors
            WHERE organization_id = $1
              AND staff_creation_key = $2
            """,
            organization,
            values["request_key"],
        )
    assert duplicate == first
    assert count == 1


@pytest.mark.asyncio
async def test_one_actor_can_use_explicit_nano_roles_in_separate_frontends(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="owner-1",
        role="admin",
        display_name="Owner",
        provider=Provider.TELEGRAM,
        external_user_id="7999",
    )
    assert await identities.grant_role(
        organization_id=organization,
        actor_id="owner-1",
        role="operator",
    )
    assert await identities.grant_role(
        organization_id=organization,
        actor_id="owner-1",
        role="master",
    )

    resolved = {}
    for role in ("admin", "operator", "master"):
        identity = await identities.resolve(
            organization_id=organization,
            provider=Provider.TELEGRAM,
            external_user_id="7999",
            consumer_key=role,
        )
        assert identity is not None
        resolved[role] = identity

    assert {item.actor_id for item in resolved.values()} == {"owner-1"}
    assert {item.role for item in resolved.values()} == {
        "admin",
        "operator",
        "master",
    }
    assert resolved["admin"].roles == frozenset({"admin", "operator", "master"})
    assert (
        await identities.resolve(
            organization_id=organization,
            provider=Provider.TELEGRAM,
            external_user_id="7999",
            consumer_key="client",
        )
        is None
    )

    assert await identities.revoke_role(
        organization_id=organization,
        actor_id="owner-1",
        role="operator",
    )
    assert (
        await identities.resolve(
            organization_id=organization,
            provider=Provider.TELEGRAM,
            external_user_id="7999",
            consumer_key="operator",
        )
        is None
    )
    master = await identities.resolve(
        organization_id=organization,
        provider=Provider.TELEGRAM,
        external_user_id="7999",
        consumer_key="master",
    )
    assert master is not None
    assert master.roles == frozenset({"admin", "master"})


@pytest.mark.asyncio
async def test_staff_bind_code_merges_role_into_an_existing_person(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="owner-existing",
        role="admin",
        display_name="Existing Owner",
        provider=Provider.TELEGRAM,
        external_user_id="7998",
    )
    pending = await identities.create_staff_actor(
        organization_id=organization,
        role="operator",
        name="Pending duplicate",
    )

    bound = await identities.bind_actor_by_code(
        organization_id=organization,
        bind_code=pending["bind_code"],
        provider=Provider.TELEGRAM,
        external_user_id="7998",
        consumer_key="operator",
    )

    assert bound is not None
    assert bound.actor_id == "owner-existing"
    assert bound.role == "operator"
    assert bound.roles == frozenset({"admin", "operator"})
    assert (
        await identities.get_actor(
            organization_id=organization,
            actor_id=pending["actor_id"],
        )
        is None
    )


@pytest.mark.asyncio
async def test_staff_bot_start_and_code_bind_an_operator_end_to_end(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    pending = await identities.create_staff_actor(
        organization_id=organization,
        role="operator",
        name="Operator From Code",
    )
    inbox = PostgresInboxStore(pool)
    outbound = PostgresOutboundStore(pool)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": True, "result": True})
        )
    )
    transport = TelegramTransport("test-token", client=client)
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=outbound,
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.TELEGRAM: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        consumer_key="operator",
    )

    await accept_telegram_update(
        inbox,
        organization,
        801,
        {
            "message": {
                "from": {"id": 4801},
                "chat": {"id": 4801},
                "text": "/start",
            }
        },
        consumer_key="operator",
    )
    assert await processor.run_once() == 1
    await accept_telegram_update(
        inbox,
        organization,
        802,
        {
            "message": {
                "from": {"id": 4801},
                "chat": {"id": 4801},
                "text": pending["bind_code"],
            }
        },
        consumer_key="operator",
    )
    assert await processor.run_once() == 1

    bound = await identities.resolve(
        organization_id=organization,
        provider=Provider.TELEGRAM,
        external_user_id="4801",
        consumer_key="operator",
    )
    async with pool.acquire() as connection:
        actor_code = await connection.fetchval(
            """
            SELECT bind_code FROM actors
            WHERE organization_id = $1 AND id = $2
            """,
            organization,
            pending["actor_id"],
        )
        session_count = await connection.fetchval(
            """
            SELECT count(*) FROM staff_binding_sessions
            WHERE organization_id = $1
            """,
            organization,
        )
        replies = await connection.fetch(
            """
            SELECT text_body, consumer_key FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = '4801'
            ORDER BY id
            """,
            organization,
        )
    await client.aclose()

    assert bound is not None
    assert bound.actor_id == pending["actor_id"]
    assert bound.role == "operator"
    assert actor_code is None
    assert session_count == 0
    assert "4-значный код" in replies[0]["text_body"]
    assert "Привязка выполнена" in replies[1]["text_body"]
    assert {reply["consumer_key"] for reply in replies} == {"operator"}


@pytest.mark.asyncio
async def test_max_start_and_code_use_the_same_master_binding_flow(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    pending = await identities.create_staff_actor(
        organization_id=organization,
        role="master",
        name="MAX Master From Code",
    )
    inbox = PostgresInboxStore(pool)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    transport = MaxTransport("test-token", client=client)
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.MAX: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        staff_roles=StaffRoleCoordinator(PostgresStaffRoleSelectionStore(pool)),
        consumer_key="staff",
    )
    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:bind-start-5802",
        organization_id=organization,
        payload={
            "update_type": "bot_started",
            "timestamp": 95801,
            "user": {"user_id": 5802},
        },
        consumer_key="staff",
    )
    assert await processor.run_once() == 1
    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:bind-code-5802",
        organization_id=organization,
        payload={
            "update_type": "message_created",
            "timestamp": 95802,
            "message": {
                "sender": {"user_id": 5802},
                "body": {"mid": "bind-code-5802", "text": pending["bind_code"]},
            },
        },
        consumer_key="staff",
    )
    assert await processor.run_once() == 1
    await client.aclose()

    bound = await identities.resolve(
        organization_id=organization,
        provider=Provider.MAX,
        external_user_id="5802",
        consumer_key="master",
    )
    assert bound is not None
    assert bound.actor_id == pending["actor_id"]
    assert bound.role == "master"


@pytest.mark.asyncio
async def test_operator_creates_master_and_master_binds_in_shared_max_workspace(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="operator-max-workspace",
        role="operator",
        display_name="Дежурный оператор",
        provider=Provider.MAX,
        external_user_id="5804",
    )
    inbox = PostgresInboxStore(pool)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    transport = MaxTransport("test-token", client=client)
    views = PostgresStaffViewStore(pool)
    sessions = PostgresStaffWorkflowSessionStore(pool)
    selections = PostgresStaffRoleSelectionStore(pool)
    operator = OperatorCoordinator(
        identities=identities,
        sessions=sessions,
        views=views,
    )
    master = MasterCoordinator(views=views)

    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.MAX: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        staff_roles=StaffRoleCoordinator(selections),
        operator=operator,
        master=master,
        organization_id=organization,
        consumer_key="staff",
    )

    next_event = 830

    async def max_start(user_id: int) -> None:
        nonlocal next_event
        next_event += 1
        assert await inbox.accept(
            provider=Provider.MAX,
            external_event_id=f"max:workspace:{next_event}",
            organization_id=organization,
            consumer_key="staff",
            payload={
                "update_type": "bot_started",
                "timestamp": next_event,
                "user": {"user_id": user_id},
            },
        )
        assert await processor.run_once() == 1

    async def max_says(user_id: int, text: str) -> None:
        nonlocal next_event
        next_event += 1
        assert await inbox.accept(
            provider=Provider.MAX,
            external_event_id=f"max:workspace:{next_event}",
            organization_id=organization,
            consumer_key="staff",
            payload={
                "update_type": "message_created",
                "timestamp": next_event,
                "message": {
                    "sender": {"user_id": user_id},
                    "body": {
                        "mid": f"workspace-message-{next_event}",
                        "text": text,
                    },
                },
            },
        )
        assert await processor.run_once() == 1

    async def max_click(user_id: int, token: str) -> None:
        nonlocal next_event
        next_event += 1
        assert await inbox.accept(
            provider=Provider.MAX,
            external_event_id=f"max:workspace:{next_event}",
            organization_id=organization,
            consumer_key="staff",
            payload={
                "update_type": "message_callback",
                "timestamp": next_event,
                "callback": {
                    "callback_id": f"workspace-callback-{next_event}",
                    "payload": f"dc1:{token}",
                    "user": {"user_id": user_id},
                },
            },
        )
        assert await processor.run_once() == 1

    await max_start(5804)
    operator_role_token = await callback_token(
        database, organization, "5804", "🧭 Оператор"
    )
    await max_click(5804, operator_role_token)
    masters_token = await callback_token(database, organization, "5804", "👨‍🔧 Мастера")
    await max_click(5804, masters_token)
    add_token = await callback_token(
        database, organization, "5804", "➕ Добавить мастера"
    )
    await max_click(5804, add_token)
    await max_says(5804, "Антон Полевой")
    await max_says(5804, "89991112233")

    async with pool.acquire() as connection:
        pending = await connection.fetchrow(
            """
            SELECT actor.id, actor.bind_code, actor.phone
            FROM actors actor
            JOIN actor_roles membership
              ON membership.organization_id = actor.organization_id
             AND membership.actor_id = actor.id
             AND membership.role = 'master' AND membership.active
            WHERE actor.organization_id = $1
              AND actor.display_name = 'Антон Полевой'
            """,
            organization,
        )
    assert pending is not None
    assert pending["phone"] == "+7 (999) 111-22-33"
    assert len(pending["bind_code"]) == 4

    await max_start(5904)
    await max_says(5904, pending["bind_code"])
    master_role_token = await callback_token(
        database, organization, "5904", "🧰 Мастер"
    )
    await max_click(5904, master_role_token)
    await client.aclose()

    bound = await identities.resolve(
        organization_id=organization,
        provider=Provider.MAX,
        external_user_id="5904",
        consumer_key="master",
    )
    assert bound is not None
    assert bound.actor_id == pending["id"]
    async with pool.acquire() as connection:
        replies = await connection.fetch(
            """
            SELECT text_body, buttons FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = '5904'
            ORDER BY id
            """,
            organization,
        )
    assert "4-значный код" in replies[0]["text_body"]
    assert "Привязка выполнена" in replies[1]["text_body"]
    assert "Выберите, в какой роли" in replies[1]["text_body"]
    assert "Режим «Мастер» включён" in replies[2]["text_body"]
    assert "Рабочее место мастера" in replies[2]["text_body"]
    buttons = replies[2]["buttons"]
    if isinstance(buttons, str):
        buttons = json.loads(buttons)
    assert {button["text"] for button in buttons} == {"📋 Мои заявки"}


@pytest.mark.asyncio
async def test_operator_creates_master_and_master_binds_in_telegram_workspace(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="operator-telegram-workspace",
        role="operator",
        display_name="Telegram Operator",
        provider=Provider.TELEGRAM,
        external_user_id="4802",
    )
    inbox = PostgresInboxStore(pool)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    transport = TelegramTransport("test-token", client=client)
    views = PostgresStaffViewStore(pool)
    operator = OperatorCoordinator(
        identities=identities,
        sessions=PostgresStaffWorkflowSessionStore(pool),
        views=views,
    )
    master = MasterCoordinator(views=views)
    operator_processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.TELEGRAM: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        operator=operator,
        master=master,
        organization_id=organization,
        consumer_key="operator",
    )

    async def say(update_id: int, user_id: int, text: str, consumer: str) -> None:
        await accept_telegram_update(
            inbox,
            organization,
            update_id,
            {
                "message": {
                    "from": {"id": user_id},
                    "chat": {"id": user_id},
                    "text": text,
                }
            },
            consumer_key=consumer,
        )

    await say(850, 4802, "/masters", "operator")
    assert await operator_processor.run_once() == 1
    add_token = await callback_token(
        database, organization, "4802", "➕ Добавить мастера"
    )
    await accept_telegram_update(
        inbox,
        organization,
        851,
        {
            "callback_query": {
                "id": "telegram-add-master",
                "from": {"id": 4802},
                "message": {"chat": {"id": 4802}},
                "data": f"dc1:{add_token}",
            }
        },
        consumer_key="operator",
    )
    assert await operator_processor.run_once() == 1
    await say(852, 4802, "Борис Полевой", "operator")
    assert await operator_processor.run_once() == 1
    await say(853, 4802, "+358 40 123 4567", "operator")
    assert await operator_processor.run_once() == 1

    async with pool.acquire() as connection:
        pending = await connection.fetchrow(
            """
            SELECT id, bind_code, phone FROM actors
            WHERE organization_id = $1 AND display_name = 'Борис Полевой'
            """,
            organization,
        )
    assert pending is not None
    assert pending["phone"] == "+358401234567"

    master_processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.TELEGRAM: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        operator=operator,
        master=master,
        organization_id=organization,
        consumer_key="master",
    )
    await say(854, 4902, "/start", "master")
    assert await master_processor.run_once() == 1
    await say(855, 4902, pending["bind_code"], "master")
    assert await master_processor.run_once() == 1
    await client.aclose()

    bound = await identities.resolve(
        organization_id=organization,
        provider=Provider.TELEGRAM,
        external_user_id="4902",
        consumer_key="master",
    )
    assert bound is not None
    assert bound.actor_id == pending["id"]
    async with pool.acquire() as connection:
        replies = await connection.fetch(
            """
            SELECT text_body, buttons FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = '4902'
            ORDER BY id
            """,
            organization,
        )
    assert "4-значный код" in replies[0]["text_body"]
    assert "Привязка выполнена" in replies[1]["text_body"]
    assert "Рабочее место мастера" in replies[1]["text_body"]


@pytest.mark.asyncio
async def test_shared_max_staff_bot_requires_and_persists_explicit_role_choice(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="max-nano-owner",
        role="admin",
        display_name="MAX Nano Owner",
        provider=Provider.MAX,
        external_user_id="5803",
    )
    for role in ("operator", "master"):
        assert await identities.grant_role(
            organization_id=organization,
            actor_id="max-nano-owner",
            role=role,
        )
    selections = PostgresStaffRoleSelectionStore(pool)
    views = PostgresStaffViewStore(pool)
    inbox = PostgresInboxStore(pool)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    transport = MaxTransport("test-token", client=client)
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=service(database),
        reader=PostgresOrderReader(pool),
        transports={Provider.MAX: transport},
        binding_sessions=PostgresStaffBindingSessionStore(pool),
        staff_roles=StaffRoleCoordinator(selections),
        operator=OperatorCoordinator(
            identities=identities,
            sessions=PostgresStaffWorkflowSessionStore(pool),
            views=views,
        ),
        master=MasterCoordinator(views=views),
        organization_id=organization,
        consumer_key="staff",
    )
    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:staff-start-5803",
        organization_id=organization,
        consumer_key="staff",
        payload={
            "update_type": "bot_started",
            "timestamp": 95803,
            "user": {"user_id": 5803},
        },
    )
    assert await processor.run_once() == 1

    master_token = await callback_token(
        database,
        organization,
        "5803",
        "🧰 Мастер",
    )
    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:staff-role-5803",
        organization_id=organization,
        consumer_key="staff",
        payload={
            "update_type": "message_callback",
            "timestamp": 95804,
            "callback": {
                "callback_id": "staff-role-5803",
                "payload": f"dc1:{master_token}",
                "user": {"user_id": 5803},
            },
        },
    )
    assert await processor.run_once() == 1
    selected = await selections.get(
        organization_id=organization,
        provider=Provider.MAX,
        external_user_id="5803",
    )
    assert selected == "master"

    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:staff-message-5803",
        organization_id=organization,
        consumer_key="staff",
        payload={
            "update_type": "message_created",
            "timestamp": 95805,
            "message": {
                "sender": {"user_id": 5803},
                "body": {"mid": "staff-message-5803", "text": "Проверка"},
            },
        },
    )
    assert await processor.run_once() == 1
    assert await inbox.accept(
        provider=Provider.MAX,
        external_event_id="max:staff-reopen-5803",
        organization_id=organization,
        consumer_key="staff",
        payload={
            "update_type": "bot_started",
            "timestamp": 95806,
            "user": {"user_id": 5803},
        },
    )
    assert await processor.run_once() == 1
    assert (
        await selections.get(
            organization_id=organization,
            provider=Provider.MAX,
            external_user_id="5803",
        )
        is None
    )
    await client.aclose()

    async with pool.acquire() as connection:
        replies = await connection.fetch(
            """
            SELECT text_body, buttons FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = '5803'
            ORDER BY id
            """,
            organization,
        )
    first_buttons = replies[0]["buttons"]
    if isinstance(first_buttons, str):
        first_buttons = json.loads(first_buttons)
    assert {button["text"] for button in first_buttons} == {
        "🛡 Администратор",
        "🧭 Оператор",
        "🧰 Мастер",
    }
    assert "Режим «Мастер» включён" in replies[1]["text_body"]
    assert "Рабочее место мастера" in replies[1]["text_body"]
    selected_buttons = replies[1]["buttons"]
    if isinstance(selected_buttons, str):
        selected_buttons = json.loads(selected_buttons)
    assert {button["text"] for button in selected_buttons} == {"📋 Мои заявки"}
    assert "Рабочее место мастера" in replies[2]["text_body"]
    assert "Выберите, в какой роли" in replies[3]["text_body"]


@pytest.mark.asyncio
async def test_bind_code_for_another_role_is_not_consumed_or_mutated(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    pending = await identities.create_staff_actor(
        organization_id=organization,
        role="master",
        name="Master In Wrong Bot",
    )

    wrong_role = await identities.bind_actor_by_code(
        organization_id=organization,
        bind_code=pending["bind_code"],
        provider=Provider.TELEGRAM,
        external_user_id="4802",
        consumer_key="operator",
    )

    assert wrong_role is None
    assert (
        await identities.resolve(
            organization_id=organization,
            provider=Provider.TELEGRAM,
            external_user_id="4802",
            consumer_key="master",
        )
        is None
    )
    correctly_bound = await identities.bind_actor_by_code(
        organization_id=organization,
        bind_code=pending["bind_code"],
        provider=Provider.TELEGRAM,
        external_user_id="4802",
        consumer_key="master",
    )
    assert correctly_bound is not None
    assert correctly_bound.actor_id == pending["actor_id"]


@pytest.mark.asyncio
async def test_staff_binding_session_limits_attempts_and_expires(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    sessions = PostgresStaffBindingSessionStore(database.pool)
    values = {
        "organization_id": organization,
        "provider": Provider.MAX,
        "external_user_id": "5801",
        "consumer_key": "master",
    }
    await sessions.begin(**values)
    assert await sessions.is_active(**values)
    assert [await sessions.take_attempt(**values) for _ in range(5)] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert await sessions.take_attempt(**values) is None
    assert not await sessions.is_active(**values)

    await sessions.begin(**values)
    async with database.pool.acquire() as connection:
        await connection.execute(
            """
            UPDATE staff_binding_sessions SET expires_at = now() - interval '1 second'
            WHERE organization_id = $1 AND provider = $2
              AND external_user_id = $3 AND consumer_key = $4
            """,
            organization,
            Provider.MAX.value,
            "5801",
            "master",
        )
    assert not await sessions.is_active(**values)
    assert await sessions.cleanup_expired() == 1


@pytest.mark.asyncio
async def test_bind_code_does_not_implicitly_merge_two_established_people(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="staff-person",
        role="operator",
        display_name="Staff",
        provider=Provider.TELEGRAM,
        external_user_id="7997",
    )
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="max-person",
        role="client",
        display_name="MAX Client",
        provider=Provider.MAX,
        external_user_id="8997",
    )
    code = await identities.issue_bind_code(
        organization_id=organization,
        actor_id="staff-person",
    )
    assert code is not None

    assert (
        await identities.bind_actor_by_code(
            organization_id=organization,
            bind_code=code,
            provider=Provider.MAX,
            external_user_id="8997",
            consumer_key="operator",
        )
        is None
    )
    assert (
        await identities.get_actor(
            organization_id=organization,
            actor_id="staff-person",
        )
        is not None
    )
    assert (
        await identities.get_actor(
            organization_id=organization,
            actor_id="max-person",
        )
        is not None
    )


async def project_all(
    database: PostgresDatabase,
    packs: PostgresPackStore | None = None,
) -> int:
    assert database.pool is not None
    outbox = PostgresOutboxStore(database.pool)
    projector = PostgresNotificationProjector(database.pool, packs)
    projected = 0
    while events := await outbox.claim_events(limit=100):
        for event in events:
            await projector.project(event)
            projected += 1
    return projected


@pytest.mark.asyncio
async def test_multirole_owner_notifications_use_the_required_role_bot(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    identities = PostgresIdentityStore(database.pool)
    await identities.upsert_actor(
        organization_id=organization,
        actor_id="nano-owner",
        role="admin",
        display_name="Nano Owner",
        provider=Provider.TELEGRAM,
        external_user_id="7888",
    )
    for role in ("operator", "master"):
        assert await identities.grant_role(
            organization_id=organization,
            actor_id="nano-owner",
            role=role,
        )

    commands = service(database)
    await commands.create_order(
        organization_id=organization,
        order_id="nano-order",
        work_type="repair",
        source="client_bot",
        details={"address": "Workshop"},
    )
    await project_all(database)
    await commands.publish_pool(
        organization,
        "nano-order",
        PoolMode.CURATED,
        actor_id="nano-owner",
    )
    await project_all(database)

    async with database.pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT consumer_key, count(*) AS total
            FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = '7888'
            GROUP BY consumer_key
            ORDER BY consumer_key
            """,
            organization,
        )
    assert {row["consumer_key"]: row["total"] for row in rows} == {
        "master": 1,
        "operator": 1,
    }


async def callback_token(
    database: PostgresDatabase,
    organization_id: str,
    recipient_id: str,
    button_text: str,
) -> str:
    assert database.pool is not None
    async with database.pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT buttons FROM outbound_messages
            WHERE organization_id = $1 AND recipient_id = $2
            ORDER BY id DESC
            """,
            organization_id,
            recipient_id,
        )
    for row in rows:
        buttons = row["buttons"]
        if isinstance(buttons, str):
            buttons = json.loads(buttons)
        for button in buttons:
            if button["text"] == button_text:
                return str(button["callback_token"])
    raise AssertionError(f"button {button_text!r} was not projected")


async def accept_telegram_update(
    inbox: PostgresInboxStore,
    organization_id: str,
    update_id: int,
    payload: dict,
    consumer_key: str = "",
) -> None:
    update = {"update_id": update_id, **payload}
    assert await inbox.accept(
        provider=Provider.TELEGRAM,
        external_event_id=f"telegram:{update_id}",
        organization_id=organization_id,
        payload=update,
        consumer_key=consumer_key,
    )


@pytest.mark.asyncio
async def test_messenger_curated_flow_reaches_evidence_backed_completion(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    for values in (
        ("operator-1", "operator", "Operator", "1001"),
        ("executor-1", "master", "Executor", "2001"),
    ):
        await identities.upsert_actor(
            organization_id=organization,
            actor_id=values[0],
            role=values[1],
            display_name=values[2],
            provider=Provider.TELEGRAM,
            external_user_id=values[3],
        )
    commands = service(database)
    await commands.create_order(
        organization_id=organization,
        order_id="order-messenger",
        work_type="repair",
        source="customer_bot",
        details={"summary": "Repair lift", "address": "Building 7"},
        evidence_requirements=EvidenceRequirements(
            minimum_photos=1,
            comment_required=True,
        ),
    )
    await commands.claim_coordination(organization, "order-messenger", "operator-1")
    await commands.publish_pool(
        organization,
        "order-messenger",
        PoolMode.CURATED,
        actor_id="operator-1",
    )

    async def telegram_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(telegram_response))
    transport = TelegramTransport("test-token", client=client)
    inbox = PostgresInboxStore(pool)
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=commands,
        reader=PostgresOrderReader(pool),
        transports={Provider.TELEGRAM: transport},
    )

    await project_all(database)
    interest_token = await callback_token(database, organization, "2001", "Готов взять")
    await accept_telegram_update(
        inbox,
        organization,
        1,
        {
            "callback_query": {
                "id": "cb-1",
                "from": {"id": 2001},
                "message": {"chat": {"id": 2001}},
                "data": f"dc1:{interest_token}",
            }
        },
    )
    assert await processor.run_once() == 1

    await project_all(database)
    assign_token = await callback_token(
        database, organization, "1001", "Выбрать мастера"
    )
    await accept_telegram_update(
        inbox,
        organization,
        2,
        {
            "callback_query": {
                "id": "cb-2",
                "from": {"id": 1001},
                "message": {"chat": {"id": 1001}},
                "data": f"dc1:{assign_token}",
            }
        },
    )
    assert await processor.run_once() == 1

    await project_all(database)
    accept_token = await callback_token(database, organization, "2001", "Принять")
    await accept_telegram_update(
        inbox,
        organization,
        3,
        {
            "callback_query": {
                "id": "cb-3",
                "from": {"id": 2001},
                "message": {"chat": {"id": 2001}},
                "data": f"dc1:{accept_token}",
            }
        },
    )
    assert await processor.run_once() == 1

    await project_all(database)
    start_token = await callback_token(
        database, organization, "2001", "Начать на месте"
    )
    await accept_telegram_update(
        inbox,
        organization,
        4,
        {
            "callback_query": {
                "id": "cb-4",
                "from": {"id": 2001},
                "message": {"chat": {"id": 2001}},
                "data": f"dc1:{start_token}",
            }
        },
    )
    assert await processor.run_once() == 1

    await project_all(database)
    submit_token = await callback_token(database, organization, "2001", "Завершить")
    await accept_telegram_update(
        inbox,
        organization,
        5,
        {
            "message": {
                "from": {"id": 2001},
                "chat": {"id": 2001},
                "photo": [{"file_id": "photo-small"}, {"file_id": "photo-large"}],
            }
        },
    )
    await accept_telegram_update(
        inbox,
        organization,
        6,
        {
            "message": {
                "from": {"id": 2001},
                "chat": {"id": 2001},
                "text": "Lift repaired and tested",
            }
        },
    )
    assert await processor.run_once() == 2
    await accept_telegram_update(
        inbox,
        organization,
        7,
        {
            "callback_query": {
                "id": "cb-7",
                "from": {"id": 2001},
                "message": {"chat": {"id": 2001}},
                "data": f"dc1:{submit_token}",
            }
        },
    )
    assert await processor.run_once() == 1
    await client.aclose()

    completed = await PostgresOrderReader(pool).get(organization, "order-messenger")
    assert completed.status is WorkOrderStatus.COMPLETED
    assert completed.report is not None
    assert completed.report.photo_refs == ("telegram:photo-large",)
    assert completed.report.comment == "Lift repaired and tested"


@pytest.mark.asyncio
async def test_client_intake_from_first_message_reaches_completion(
    database: PostgresDatabase,
    organization: str,
) -> None:
    assert database.pool is not None
    pool = database.pool
    identities = PostgresIdentityStore(pool)
    for values in (
        ("operator-1", "operator", "Operator", "1001"),
        ("executor-1", "master", "Executor", "2001"),
    ):
        await identities.upsert_actor(
            organization_id=organization,
            actor_id=values[0],
            role=values[1],
            display_name=values[2],
            provider=Provider.TELEGRAM,
            external_user_id=values[3],
        )

    packs = PostgresPackStore(pool)
    assert await packs.bootstrap_active(
        organization, seed=seed_definition("field_service")
    )

    async def telegram_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(telegram_response))
    transport = TelegramTransport("test-token", client=client)
    inbox = PostgresInboxStore(pool)
    commands = service(database)
    intake = IntakeCoordinator(
        packs=packs,
        sessions=PostgresIntakeSessionStore(pool),
        service=commands,
    )
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=PostgresOutboundStore(pool),
        service=commands,
        reader=PostgresOrderReader(pool),
        transports={Provider.TELEGRAM: transport},
        packs=packs,
        intake=intake,
    )

    next_update = 0

    async def say(chat: int, text: str) -> None:
        nonlocal next_update
        next_update += 1
        await accept_telegram_update(
            inbox,
            organization,
            next_update,
            {"message": {"from": {"id": chat}, "chat": {"id": chat}, "text": text}},
        )

    async def click(chat: int, token: str) -> None:
        nonlocal next_update
        next_update += 1
        await accept_telegram_update(
            inbox,
            organization,
            next_update,
            {
                "callback_query": {
                    "id": f"cb-{next_update}",
                    "from": {"id": chat},
                    "message": {"chat": {"id": chat}},
                    "data": f"dc1:{token}",
                }
            },
        )

    # A brand-new client is auto-registered, then follows the fixed
    # phone -> address rails before the IndustryPack service fields.
    await say(3001, "Здравствуйте")
    assert await processor.run_once() == 1
    await say(3001, "+7 999 000-00-01")
    assert await processor.run_once() == 1
    await say(3001, "Ленина 1")
    assert await processor.run_once() == 1
    repair_token = await callback_token(database, organization, "3001", "Ремонт")
    await click(3001, repair_token)
    assert await processor.run_once() == 1
    done_token = await callback_token(database, organization, "3001", "Готово")
    await click(3001, done_token)
    assert await processor.run_once() == 1

    # Guided field prompts: required address, skipped optional asset, required fault.
    await say(3001, "Ленина 1")
    assert await processor.run_once() == 1
    await say(3001, "-")
    assert await processor.run_once() == 1
    await say(3001, "Течёт кран")
    assert await processor.run_once() == 1
    confirm_token = await callback_token(database, organization, "3001", "Отправить")
    await click(3001, confirm_token)
    assert await processor.run_once() == 1

    async with pool.acquire() as connection:
        order_id = await connection.fetchval(
            """
            SELECT id FROM work_orders
            WHERE organization_id = $1 AND requester_id = $2
            """,
            organization,
            "telegram:3001",
        )
    assert order_id is not None
    submitted = await PostgresOrderReader(pool).get(organization, order_id)
    assert submitted.status is WorkOrderStatus.SUBMITTED
    assert submitted.work_type == "repair"

    # Operator is notified of the new client order and publishes it to the pool.
    await project_all(database, packs)
    pool_token = await callback_token(database, organization, "1001", "В пул")
    await click(1001, pool_token)
    assert await processor.run_once() == 1

    # Curated pool: executor expresses interest, operator assigns, executor works.
    await project_all(database, packs)
    interest_token = await callback_token(database, organization, "2001", "Готов взять")
    await click(2001, interest_token)
    assert await processor.run_once() == 1

    await project_all(database, packs)
    assign_token = await callback_token(
        database, organization, "1001", "Выбрать мастера"
    )
    await click(1001, assign_token)
    assert await processor.run_once() == 1

    await project_all(database, packs)
    accept_token = await callback_token(database, organization, "2001", "Принять")
    await click(2001, accept_token)
    assert await processor.run_once() == 1

    await project_all(database, packs)
    start_token = await callback_token(
        database, organization, "2001", "Начать на месте"
    )
    await click(2001, start_token)
    assert await processor.run_once() == 1

    await project_all(database, packs)
    submit_token = await callback_token(database, organization, "2001", "Завершить")
    next_update += 1
    await accept_telegram_update(
        inbox,
        organization,
        next_update,
        {
            "message": {
                "from": {"id": 2001},
                "chat": {"id": 2001},
                "photo": [{"file_id": "photo-small"}, {"file_id": "photo-large"}],
            }
        },
    )
    await say(2001, "Кран заменён и проверен")
    assert await processor.run_once() == 2
    await click(2001, submit_token)
    assert await processor.run_once() == 1
    await client.aclose()

    completed = await PostgresOrderReader(pool).get(organization, order_id)
    assert completed.status is WorkOrderStatus.COMPLETED
    assert completed.report is not None
    assert completed.report.photo_refs == ("telegram:photo-large",)
    assert completed.report.comment == "Кран заменён и проверен"
