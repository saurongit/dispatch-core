from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import asyncpg


@dataclass(frozen=True, slots=True)
class QueueStatus:
    queue: str
    status: str
    count: int
    oldest_at: datetime


@dataclass(frozen=True, slots=True)
class WorkerHeartbeat:
    consumer_key: str
    instance_id: str
    started_at: datetime
    heartbeat_at: datetime
    healthy: bool


@dataclass(frozen=True, slots=True)
class OperationsSnapshot:
    organization_id: str
    generated_at: datetime
    queues: tuple[QueueStatus, ...]
    workers: tuple[WorkerHeartbeat, ...]


class PostgresOperationsStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def heartbeat(
        self,
        *,
        organization_id: str,
        consumer_key: str,
        instance_id: str,
        started_at: datetime | None = None,
    ) -> None:
        process_started_at = started_at or datetime.now(UTC)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO worker_heartbeats (
                    organization_id, consumer_key, instance_id,
                    started_at, heartbeat_at
                ) VALUES ($1, $2, $3, $4, now())
                ON CONFLICT (organization_id, consumer_key, instance_id)
                DO UPDATE SET
                    started_at = EXCLUDED.started_at,
                    heartbeat_at = EXCLUDED.heartbeat_at
                """,
                organization_id,
                consumer_key,
                instance_id,
                process_started_at,
            )

    async def remove_worker(
        self,
        *,
        organization_id: str,
        consumer_key: str,
        instance_id: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM worker_heartbeats
                WHERE organization_id = $1 AND consumer_key = $2
                  AND instance_id = $3
                """,
                organization_id,
                consumer_key,
                instance_id,
            )

    async def worker_is_healthy(
        self,
        *,
        organization_id: str,
        consumer_key: str,
        instance_id: str,
        stale_after_seconds: int,
    ) -> bool:
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM worker_heartbeats
                        WHERE organization_id = $1 AND consumer_key = $2
                          AND instance_id = $3
                          AND heartbeat_at >= now()
                              - make_interval(secs => $4)
                    )
                    """,
                    organization_id,
                    consumer_key,
                    instance_id,
                    stale_after_seconds,
                )
            )

    async def snapshot(
        self,
        organization_id: str,
        *,
        worker_stale_after_seconds: int,
    ) -> OperationsSnapshot:
        async with self._pool.acquire() as connection:
            queue_rows = await connection.fetch(
                """
                WITH queue_items AS (
                    SELECT 'inbox'::text AS queue, status, received_at AS queued_at
                    FROM inbox_events WHERE organization_id = $1
                    UNION ALL
                    SELECT 'outbox', status, occurred_at
                    FROM outbox_events WHERE organization_id = $1
                    UNION ALL
                    SELECT 'outbound', status, created_at
                    FROM outbound_messages WHERE organization_id = $1
                )
                SELECT queue, status, count(*)::bigint AS item_count,
                       min(queued_at) AS oldest_at
                FROM queue_items
                GROUP BY queue, status
                ORDER BY queue, status
                """,
                organization_id,
            )
            worker_rows = await connection.fetch(
                """
                SELECT consumer_key, instance_id, started_at, heartbeat_at,
                       heartbeat_at >= now() - make_interval(secs => $2)
                           AS healthy
                FROM worker_heartbeats
                WHERE organization_id = $1
                ORDER BY consumer_key, instance_id
                """,
                organization_id,
                worker_stale_after_seconds,
            )
            generated_at = await connection.fetchval("SELECT now()")
        return OperationsSnapshot(
            organization_id=organization_id,
            generated_at=generated_at,
            queues=tuple(
                QueueStatus(
                    queue=row["queue"],
                    status=row["status"],
                    count=row["item_count"],
                    oldest_at=row["oldest_at"],
                )
                for row in queue_rows
            ),
            workers=tuple(
                WorkerHeartbeat(
                    consumer_key=row["consumer_key"],
                    instance_id=row["instance_id"],
                    started_at=row["started_at"],
                    heartbeat_at=row["heartbeat_at"],
                    healthy=row["healthy"],
                )
                for row in worker_rows
            ),
        )

    async def cleanup_terminal(
        self,
        organization_id: str,
        *,
        retention_days: int,
        batch_size: int = 1000,
    ) -> dict[str, int]:
        if retention_days < 1:
            raise ValueError("retention_days must be positive")
        if not 1 <= batch_size <= 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        statements = {
            "inbox": """
                WITH doomed AS (
                    SELECT ctid FROM inbox_events
                    WHERE organization_id = $1 AND status = 'processed'
                      AND processed_at < now() - make_interval(days => $2)
                    LIMIT $3
                )
                DELETE FROM inbox_events AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
            "outbox": """
                WITH doomed AS (
                    SELECT ctid FROM outbox_events
                    WHERE organization_id = $1 AND status = 'delivered'
                      AND delivered_at < now() - make_interval(days => $2)
                    LIMIT $3
                )
                DELETE FROM outbox_events AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
            "outbound": """
                WITH doomed AS (
                    SELECT ctid FROM outbound_messages
                    WHERE organization_id = $1 AND status = 'delivered'
                      AND delivered_at < now() - make_interval(days => $2)
                    LIMIT $3
                )
                DELETE FROM outbound_messages AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
            "idempotency": """
                WITH doomed AS (
                    SELECT ctid FROM idempotency_keys
                    WHERE organization_id = $1 AND expires_at < now()
                      AND $2::integer >= 1
                    LIMIT $3
                )
                DELETE FROM idempotency_keys AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
            "callbacks": """
                WITH doomed AS (
                    SELECT ctid FROM callback_actions
                    WHERE organization_id = $1
                      AND expires_at < now() - make_interval(days => $2)
                    LIMIT $3
                )
                DELETE FROM callback_actions AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
            "heartbeats": """
                WITH doomed AS (
                    SELECT ctid FROM worker_heartbeats
                    WHERE organization_id = $1
                      AND heartbeat_at < now() - make_interval(days => $2)
                    LIMIT $3
                )
                DELETE FROM worker_heartbeats AS item USING doomed
                WHERE item.ctid = doomed.ctid
            """,
        }
        deleted: dict[str, int] = {}
        async with self._pool.acquire() as connection:
            for name, statement in statements.items():
                result = await connection.execute(
                    statement,
                    organization_id,
                    retention_days,
                    batch_size,
                )
                deleted[name] = int(result.rsplit(" ", 1)[-1])
        return deleted
