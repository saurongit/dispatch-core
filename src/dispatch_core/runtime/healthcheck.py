from __future__ import annotations

import asyncio
import logging

from dispatch_core.api.settings import Settings
from dispatch_core.infrastructure.operations import PostgresOperationsStore
from dispatch_core.infrastructure.postgres import PostgresDatabase

logger = logging.getLogger(__name__)


async def worker_is_healthy(settings: Settings) -> bool:
    database = PostgresDatabase(
        settings.database_url.get_secret_value(),
        min_size=1,
        max_size=1,
    )
    try:
        await database.connect()
        if database.pool is None:
            return False
        return await PostgresOperationsStore(database.pool).worker_is_healthy(
            organization_id=settings.organization_id,
            consumer_key=settings.consumer_key,
            instance_id=settings.worker_instance_id,
            stale_after_seconds=settings.worker_health_stale_seconds,
        )
    except Exception:
        logger.exception("worker healthcheck failed")
        return False
    finally:
        await database.close()


def main() -> None:
    healthy = asyncio.run(worker_is_healthy(Settings()))  # type: ignore[call-arg]
    raise SystemExit(0 if healthy else 1)
