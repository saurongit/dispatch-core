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
from dispatch_core.infrastructure.postgres import (
    PostgresDatabase,
    PostgresUnitOfWorkFactory,
)
from dispatch_core.infrastructure.read_models import PostgresOrderReader
from dispatch_core.infrastructure.workflow_store import (
    PostgresExecutionStore,
    PostgresIdentityStore,
    PostgresReportDraftStore,
)
from dispatch_core.messaging.models import OutboundButton, Provider
from dispatch_core.messaging.processor import InboundProcessor
from dispatch_core.messaging.projector import PostgresNotificationProjector
from dispatch_core.runtime.worker import run_worker
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
        assert await connection.fetchval(
            "SELECT name FROM organizations WHERE id = $1",
            organization_id,
        ) == "Worker Wiring Test"
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
        assert await connection.fetchval(
            "SELECT count(*) FROM work_orders WHERE organization_id = $1",
            organization,
        ) == 1
        assert await connection.fetchval(
            "SELECT count(*) FROM outbox_events WHERE organization_id = $1",
            organization,
        ) == 1


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
    await service(database).publish_pool(
        organization, "order-1", PoolMode.CURATED
    )
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
    second = await commands.assign_order(
        organization, "order-2", "executor-1"
    )
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
    await commands.assign_order(
        organization, "order-append-track", "executor-append"
    )
    await commands.accept_order(
        organization, "order-append-track", "executor-append"
    )
    _, tracking = await commands.start_travel(
        organization,
        "order-append-track",
        "executor-append",
        session_id="track-append",
    )
    await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
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
        latitude=53.76,
        longitude=87.11,
        source=LocationSource.MAX,
        source_event_id="max:location-2",
    )
    duplicate = await commands.record_location(
        organization_id=organization,
        session_id=tracking.id,
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
    assert {item.external_event_id for item in await inbox.claim()} == {
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
    identifiers = [
        item.external_event_id for batch in batches for item in batch
    ]
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
        allowed_role="executor",
    )
    resolved = await callbacks.resolve(
        token=created.token,
        organization_id=organization,
        actor_role="executor",
    )
    denied = await callbacks.resolve(
        token=created.token,
        organization_id=organization,
        actor_role="coordinator",
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
        role="executor",
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
    assert await drafts.append_photo(
        organization_id=organization,
        order_id="order-1",
        executor_id="executor-1",
        photo_ref="telegram:photo-1",
    ) == 1
    assert await drafts.append_photo(
        organization_id=organization,
        order_id="order-1",
        executor_id="executor-1",
        photo_ref="telegram:photo-2",
    ) == 2
    assert await drafts.append_photo(
        organization_id=organization,
        order_id="order-1",
        executor_id="executor-1",
        photo_ref="telegram:photo-1",
    ) == 2
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


async def project_all(database: PostgresDatabase) -> int:
    assert database.pool is not None
    outbox = PostgresOutboxStore(database.pool)
    projector = PostgresNotificationProjector(database.pool)
    projected = 0
    while events := await outbox.claim_events(limit=100):
        for event in events:
            await projector.project(event)
            projected += 1
    return projected


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
) -> None:
    update = {"update_id": update_id, **payload}
    assert await inbox.accept(
        provider=Provider.TELEGRAM,
        external_event_id=f"telegram:{update_id}",
        organization_id=organization_id,
        payload=update,
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
        ("operator-1", "coordinator", "Operator", "1001"),
        ("executor-1", "executor", "Executor", "2001"),
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
    await commands.claim_coordination(
        organization, "order-messenger", "operator-1"
    )
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
    interest_token = await callback_token(
        database, organization, "2001", "Готов взять"
    )
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
    accept_token = await callback_token(
        database, organization, "2001", "Принять"
    )
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
    submit_token = await callback_token(
        database, organization, "2001", "Завершить"
    )
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

    completed = await PostgresOrderReader(pool).get(
        organization, "order-messenger"
    )
    assert completed.status is WorkOrderStatus.COMPLETED
    assert completed.report is not None
    assert completed.report.photo_refs == ("telegram:photo-large",)
    assert completed.report.comment == "Lift repaired and tested"
