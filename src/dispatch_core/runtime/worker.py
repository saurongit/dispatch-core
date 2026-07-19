from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from pathlib import Path

from dispatch_core.api.settings import Settings
from dispatch_core.application.async_service import AsyncDispatchService
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
from dispatch_core.messaging.models import Provider
from dispatch_core.messaging.processor import InboundProcessor
from dispatch_core.messaging.projector import (
    OutboxProjectorWorker,
    PostgresNotificationProjector,
)
from dispatch_core.messaging.sender import OutboundSender
from dispatch_core.transports.max import MaxTransport
from dispatch_core.transports.polling import DurablePollingReceiver
from dispatch_core.transports.telegram import TelegramTransport

from .factory import build_transports

logger = logging.getLogger(__name__)


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
    processor = InboundProcessor(
        inbox=inbox,
        identities=PostgresIdentityStore(pool),
        callbacks=PostgresCallbackStore(pool),
        executions=PostgresExecutionStore(pool),
        drafts=PostgresReportDraftStore(pool),
        outbound=outbound,
        service=service,
        reader=PostgresOrderReader(pool),
        transports=transports,
    )
    projector = OutboxProjectorWorker(
        PostgresOutboxStore(pool),
        PostgresNotificationProjector(pool),
    )
    sender = OutboundSender(outbound, transports)
    polling = DurablePollingReceiver(inbox)
    stop = stop_event or asyncio.Event()
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
            for provider in transports:
                tasks.create_task(
                    _repeat(
                        lambda provider=provider: sender.run_once(provider),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            telegram = transports.get(Provider.TELEGRAM)
            if (
                settings.telegram_receive_mode == "polling"
                and isinstance(telegram, TelegramTransport)
            ):
                tasks.create_task(
                    _repeat(
                        lambda: polling.telegram_once(
                            telegram,
                            organization_id=settings.organization_id,
                            consumer_key=f"{settings.organization_id}:telegram",
                        ),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            maximum = transports.get(Provider.MAX)
            if (
                settings.max_receive_mode == "polling"
                and isinstance(maximum, MaxTransport)
            ):
                tasks.create_task(
                    _repeat(
                        lambda: polling.max_once(
                            maximum,
                            organization_id=settings.organization_id,
                            consumer_key=f"{settings.organization_id}:max",
                        ),
                        stop,
                        settings.worker_idle_seconds,
                    )
                )
            await stop.wait()
    finally:
        for transport in transports.values():
            await transport.close()
        await database.close()


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


def main() -> None:
    asyncio.run(run_worker(Settings()))  # type: ignore[call-arg]
