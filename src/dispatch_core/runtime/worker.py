from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

from dispatch_core.api.settings import Settings
from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.infrastructure.messaging import (
    PostgresCallbackStore,
    PostgresInboxStore,
    PostgresOutboundStore,
    PostgresOutboxStore,
)
from dispatch_core.infrastructure.operations import (
    OperationsSnapshot,
    PostgresOperationsStore,
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
    PostgresConfigSessionStore,
    PostgresExecutionStore,
    PostgresIdentityStore,
    PostgresIntakeSessionStore,
    PostgresReportDraftStore,
    PostgresStaffBindingSessionStore,
    PostgresStaffRoleSelectionStore,
)
from dispatch_core.messaging.config import MENU_COMMANDS, ConfigCoordinator
from dispatch_core.messaging.intake import IntakeCoordinator
from dispatch_core.messaging.models import Provider
from dispatch_core.messaging.processor import InboundProcessor
from dispatch_core.messaging.projector import (
    OutboxProjectorWorker,
    PostgresNotificationProjector,
)
from dispatch_core.messaging.sender import OutboundSender
from dispatch_core.messaging.staff import StaffRoleCoordinator
from dispatch_core.messaging.workspaces import (
    MASTER_MENU_COMMANDS,
    OPERATOR_MENU_COMMANDS,
    MasterCoordinator,
    OperatorCoordinator,
)
from dispatch_core.packs.catalog import seed_definition
from dispatch_core.transports.max import MaxTransport
from dispatch_core.transports.polling import DurablePollingReceiver
from dispatch_core.transports.telegram import TelegramTransport

from .factory import build_transports

logger = logging.getLogger(__name__)

_SESSION_TTL_HOURS = 24
_SESSION_CLEANUP_INTERVAL_SECONDS = 1800  # 30 minutes


async def run_worker(
    settings: Settings,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    database = PostgresDatabase(settings.database_url.get_secret_value())
    await database.connect()
    if settings.auto_migrate:
        migration_directory = settings.migrations_directory or (
            Path(__file__).resolve().parents[3]  # noqa: ASYNC240
            / "migrations"
        )
        await database.migrate(migration_directory)
    if database.pool is None:
        raise RuntimeError("database pool was not initialized")
    pool = database.pool
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO organizations(id, name)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            settings.organization_id,
            settings.organization_name,
        )
    transports = build_transports(settings)
    inbox = PostgresInboxStore(pool)
    outbound = PostgresOutboundStore(pool)
    service = AsyncDispatchService(PostgresUnitOfWorkFactory(pool))
    packs = PostgresPackStore(pool)
    await packs.bootstrap_active(
        settings.organization_id,
        seed=seed_definition(settings.default_pack),
    )
    intake_sessions = PostgresIntakeSessionStore(pool)
    config_sessions = PostgresConfigSessionStore(pool)
    binding_sessions = PostgresStaffBindingSessionStore(pool)
    staff_workflow_sessions = PostgresStaffWorkflowSessionStore(pool)
    staff_roles = StaffRoleCoordinator(PostgresStaffRoleSelectionStore(pool))
    staff_views = PostgresStaffViewStore(pool)
    identities = PostgresIdentityStore(pool)
    operations = PostgresOperationsStore(pool)
    intake = IntakeCoordinator(
        packs=packs,
        sessions=intake_sessions,
        service=service,
        public_base_url=settings.public_base_url,
    )
    config = ConfigCoordinator(
        packs=packs,
        sessions=config_sessions,
        identities=identities,
    )
    operator = OperatorCoordinator(
        identities=identities,
        sessions=staff_workflow_sessions,
        views=staff_views,
        packs=packs,
    )
    master = MasterCoordinator(views=staff_views, packs=packs)
    processor = InboundProcessor(
        inbox=inbox,
        identities=identities,
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=outbound,
        service=service,
        reader=PostgresOrderReader(pool),
        transports=transports,
        packs=packs,
        intake=intake,
        config=config,
        binding_sessions=binding_sessions,
        staff_roles=staff_roles,
        operator=operator,
        master=master,
        organization_id=settings.organization_id,
        consumer_key=settings.consumer_key,
    )
    projector = OutboxProjectorWorker(
        PostgresOutboxStore(pool),
        PostgresNotificationProjector(
            pool,
            packs,
            public_base_url=settings.public_base_url,
        ),
        organization_id=settings.organization_id,
    )
    sender = OutboundSender(
        outbound,
        transports,
        organization_id=settings.organization_id,
        consumer_key=settings.consumer_key,
    )
    polling = DurablePollingReceiver(inbox)
    telegram = transports.get(Provider.TELEGRAM)
    if isinstance(telegram, TelegramTransport):
        await _register_frontend_commands(
            telegram,
            settings.consumer_key,
        )
    stop = stop_event or asyncio.Event()
    worker_started_at = datetime.now(UTC)
    if stop_event is None:
        loop = asyncio.get_running_loop()
        for name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(name, stop.set)
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                _repeat(processor.run_once, stop, settings.worker_idle_seconds)
            )
            tasks.create_task(
                _repeat(projector.run_once, stop, settings.worker_idle_seconds)
            )
            tasks.create_task(
                _periodic(
                    lambda: _cleanup_sessions(
                        intake_sessions,
                        config_sessions,
                        binding_sessions,
                        staff_workflow_sessions,
                    ),
                    stop,
                    _SESSION_CLEANUP_INTERVAL_SECONDS,
                )
            )
            tasks.create_task(
                _periodic(
                    lambda: operations.heartbeat(
                        organization_id=settings.organization_id,
                        consumer_key=settings.consumer_key,
                        instance_id=settings.worker_instance_id,
                        started_at=worker_started_at,
                    ),
                    stop,
                    settings.worker_heartbeat_seconds,
                )
            )
            tasks.create_task(
                _periodic(
                    lambda: _maintain_operations(
                        operations,
                        organization_id=settings.organization_id,
                        retention_days=settings.queue_retention_days,
                        warning_age_seconds=settings.queue_warning_age_seconds,
                        worker_stale_after_seconds=(
                            settings.worker_health_stale_seconds
                        ),
                    ),
                    stop,
                    settings.maintenance_interval_seconds,
                )
            )
            for provider in transports:
                tasks.create_task(
                    _repeat(
                        lambda provider=provider: sender.run_once(provider),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            telegram_poll = transports.get(Provider.TELEGRAM)
            if settings.telegram_receive_mode == "polling" and isinstance(
                telegram_poll, TelegramTransport
            ):
                tasks.create_task(
                    _repeat(
                        lambda: polling.telegram_once(
                            telegram_poll,
                            organization_id=settings.organization_id,
                            consumer_key=settings.consumer_key,
                        ),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            maximum = transports.get(Provider.MAX)
            if settings.max_receive_mode == "polling" and isinstance(
                maximum, MaxTransport
            ):
                tasks.create_task(
                    _repeat(
                        lambda: polling.max_once(
                            maximum,
                            organization_id=settings.organization_id,
                            consumer_key=settings.consumer_key,
                        ),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            await stop.wait()
    finally:
        try:
            await operations.remove_worker(
                organization_id=settings.organization_id,
                consumer_key=settings.consumer_key,
                instance_id=settings.worker_instance_id,
            )
        except Exception:
            logger.warning("failed to remove worker heartbeat", exc_info=True)
        for transport in transports.values():
            await transport.close()
        await database.close()


async def _register_frontend_commands(
    telegram: TelegramTransport,
    consumer_key: str,
) -> None:
    role_commands = {
        "admin": MENU_COMMANDS,
        "operator": OPERATOR_MENU_COMMANDS,
        "master": MASTER_MENU_COMMANDS,
    }
    commands = role_commands.get(consumer_key)
    if commands is None:
        return
    try:
        await telegram.set_my_commands(commands)
    except Exception:
        logger.warning(
            "failed to register %s bot commands",
            consumer_key,
            exc_info=True,
        )


async def _repeat(
    operation: Callable[[], Awaitable[int]],
    stop: asyncio.Event,
    idle_seconds: float,
) -> None:
    failures = 0
    while not stop.is_set():
        try:
            work_count = await operation()
        except Exception:
            failures += 1
            logger.exception("worker operation failed; it will be retried")
            retry_seconds = min(
                max(idle_seconds, 0.1) * (2 ** min(failures - 1, 8)),
                30.0,
            )
            await _wait_or_stop(stop, retry_seconds)
            continue
        failures = 0
        if work_count:
            continue
        await _wait_or_stop(stop, idle_seconds)


async def _wait_or_stop(stop: asyncio.Event, delay_seconds: float) -> None:
    if stop.is_set():
        return
    if delay_seconds <= 0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


async def _periodic(
    operation: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
    interval_seconds: float,
) -> None:
    while not stop.is_set():
        try:
            await operation()
        except Exception:
            logger.exception("periodic operation failed")
        await _wait_or_stop(stop, interval_seconds)


async def _cleanup_sessions(
    intake_sessions: PostgresIntakeSessionStore,
    config_sessions: PostgresConfigSessionStore,
    binding_sessions: PostgresStaffBindingSessionStore,
    staff_workflow_sessions: PostgresStaffWorkflowSessionStore,
) -> None:
    intake_deleted = await intake_sessions.cleanup_stale(
        max_age_hours=_SESSION_TTL_HOURS,
    )
    config_deleted = await config_sessions.cleanup_stale(
        max_age_hours=_SESSION_TTL_HOURS,
    )
    binding_deleted = await binding_sessions.cleanup_expired()
    staff_deleted = await staff_workflow_sessions.cleanup_stale(
        max_age_hours=_SESSION_TTL_HOURS,
    )
    total = intake_deleted + config_deleted + binding_deleted + staff_deleted
    if total:
        logger.info(
            (
                "cleaned up %d stale sessions "
                "(intake=%d, config=%d, binding=%d, staff=%d)"
            ),
            total,
            intake_deleted,
            config_deleted,
            binding_deleted,
            staff_deleted,
        )


async def _maintain_operations(
    operations: PostgresOperationsStore,
    *,
    organization_id: str,
    retention_days: int,
    warning_age_seconds: int,
    worker_stale_after_seconds: int,
) -> None:
    deleted = await operations.cleanup_terminal(
        organization_id,
        retention_days=retention_days,
    )
    deleted_total = sum(deleted.values())
    if deleted_total:
        logger.info(
            "cleaned up %d terminal runtime records: %s",
            deleted_total,
            deleted,
        )
    snapshot = await operations.snapshot(
        organization_id,
        worker_stale_after_seconds=worker_stale_after_seconds,
    )
    _log_operations_alerts(snapshot, warning_age_seconds=warning_age_seconds)


def _log_operations_alerts(
    snapshot: OperationsSnapshot,
    *,
    warning_age_seconds: int,
) -> None:
    now = datetime.now(UTC)
    dead = [item for item in snapshot.queues if item.status == "dead" and item.count]
    if dead:
        logger.error(
            "dead-letter items require review for organization %s: %s",
            snapshot.organization_id,
            {item.queue: item.count for item in dead},
        )
    delayed = [
        item
        for item in snapshot.queues
        if item.status in {"pending", "processing"}
        and (now - item.oldest_at).total_seconds() >= warning_age_seconds
    ]
    if delayed:
        logger.warning(
            "runtime queues are delayed for organization %s: %s",
            snapshot.organization_id,
            {
                f"{item.queue}:{item.status}": {
                    "status": item.status,
                    "count": item.count,
                    "oldest_at": item.oldest_at.isoformat(),
                }
                for item in delayed
            },
        )


def main() -> None:
    asyncio.run(run_worker(Settings()))  # type: ignore[call-arg]
