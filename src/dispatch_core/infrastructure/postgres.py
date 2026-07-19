from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from types import TracebackType
from typing import Any

import asyncpg

from dispatch_core.domain.errors import ConcurrencyConflict, NotFound
from dispatch_core.domain.events import DomainEvent
from dispatch_core.domain.tracking import (
    LocationSource,
    TrackingPoint,
    TrackingSession,
    TrackingStatus,
)
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    PoolMode,
    PoolResponse,
    PoolResponseStatus,
    WorkOrder,
    WorkOrderStatus,
)


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class PostgresDatabase:
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                self._dsn,
                min_size=self._min_size,
                max_size=self._max_size,
                command_timeout=30,
            )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def health(self) -> bool:
        if self.pool is None:
            return False
        try:
            async with self.pool.acquire() as connection:
                return await connection.fetchval("SELECT 1") == 1
        except (asyncpg.PostgresError, OSError):
            return False

    async def migrate(self, directory: Path) -> tuple[str, ...]:
        if self.pool is None:
            raise RuntimeError("database is not connected")
        applied: list[str] = []
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            # Migration discovery is a bounded startup operation, not event-loop work.
            for path in sorted(directory.glob("*.sql")):  # noqa: ASYNC240
                version = path.name
                exists = await connection.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE version = $1",
                    version,
                )
                if exists:
                    continue
                sql = path.read_text(encoding="utf-8")
                async with connection.transaction():
                    await connection.execute(sql)
                    await connection.execute(
                        "INSERT INTO schema_migrations(version) VALUES ($1)",
                        version,
                    )
                applied.append(version)
        return tuple(applied)


class PostgresUnitOfWorkFactory:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def __call__(self) -> PostgresUnitOfWork:
        return PostgresUnitOfWork(self._pool)


class PostgresUnitOfWork:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._connection: asyncpg.Connection | None = None
        self._transaction: asyncpg.Transaction | None = None
        self._committed = False
        self.orders: PostgresOrderRepository
        self.tracking: PostgresTrackingRepository
        self.outbox: PostgresOutbox

    async def __aenter__(self) -> PostgresUnitOfWork:
        self._connection = await self._pool.acquire()
        self._transaction = self._connection.transaction()
        await self._transaction.start()
        self.orders = PostgresOrderRepository(self._connection)
        self.tracking = PostgresTrackingRepository(self._connection)
        self.outbox = PostgresOutbox(self._connection)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if self._transaction is not None:
                if exc_type is None and self._committed:
                    await self._transaction.commit()
                else:
                    await self._transaction.rollback()
        finally:
            if self._connection is not None:
                await self._pool.release(self._connection)
            self._connection = None
            self._transaction = None

    async def commit(self) -> None:
        if self._connection is None:
            raise RuntimeError("unit of work is not active")
        self._committed = True


class PostgresOrderRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        row = await self._connection.fetchrow(
            """
            SELECT * FROM work_orders
            WHERE organization_id = $1 AND id = $2
            FOR UPDATE
            """,
            organization_id,
            order_id,
        )
        if row is None:
            raise NotFound(f"work order {order_id!r} was not found")
        responses = await self._connection.fetch(
            """
            SELECT executor_id, status, responded_at
            FROM pool_responses
            WHERE organization_id = $1 AND work_order_id = $2
            ORDER BY responded_at, executor_id
            """,
            organization_id,
            order_id,
        )
        return _order_from_rows(row, responses)

    async def save(
        self, order: WorkOrder, *, expected_version: int | None
    ) -> None:
        try:
            if expected_version is None:
                result = await self._connection.execute(
                    """
                    INSERT INTO work_orders (
                        organization_id, id, work_type, source, details,
                        requester_id, coordinator_id, evidence_requirements,
                        status, pool_mode, assignee_id, report, version,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5::jsonb, $6, $7, $8::jsonb,
                        $9, $10, $11, $12::jsonb, $13, $14, $15
                    )
                    ON CONFLICT (organization_id, id) DO NOTHING
                    """,
                    *_order_values(order),
                )
                if result != "INSERT 0 1":
                    raise ConcurrencyConflict(
                        f"work order {order.id!r} already exists"
                    )
            else:
                result = await self._connection.execute(
                    """
                    UPDATE work_orders SET
                        work_type = $3,
                        source = $4,
                        details = $5::jsonb,
                        requester_id = $6,
                        coordinator_id = $7,
                        evidence_requirements = $8::jsonb,
                        status = $9,
                        pool_mode = $10,
                        assignee_id = $11,
                        report = $12::jsonb,
                        version = $13,
                        created_at = $14,
                        updated_at = $15
                    WHERE organization_id = $1 AND id = $2 AND version = $16
                    """,
                    *_order_values(order),
                    expected_version,
                )
                if result != "UPDATE 1":
                    raise ConcurrencyConflict(
                        f"work order {order.id!r} changed concurrently"
                    )
            await self._replace_responses(order)
        except asyncpg.UniqueViolationError as exc:
            raise ConcurrencyConflict(
                "database uniqueness constraint rejected concurrent assignment"
            ) from exc

    async def _replace_responses(self, order: WorkOrder) -> None:
        await self._connection.execute(
            """
            DELETE FROM pool_responses
            WHERE organization_id = $1 AND work_order_id = $2
            """,
            order.organization_id,
            order.id,
        )
        if not order.pool_responses:
            return
        await self._connection.executemany(
            """
            INSERT INTO pool_responses (
                organization_id, work_order_id, executor_id, status, responded_at
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            [
                (
                    order.organization_id,
                    order.id,
                    response.executor_id,
                    response.status.value,
                    response.responded_at,
                )
                for response in order.pool_responses.values()
            ],
        )


class PostgresTrackingRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def get(
        self, organization_id: str, session_id: str
    ) -> TrackingSession:
        row = await self._connection.fetchrow(
            """
            SELECT * FROM tracking_sessions
            WHERE organization_id = $1 AND id = $2
            FOR UPDATE
            """,
            organization_id,
            session_id,
        )
        if row is None:
            raise NotFound(f"tracking session {session_id!r} was not found")
        points = await self._connection.fetch(
            """
            SELECT * FROM tracking_points
            WHERE organization_id = $1 AND session_id = $2
            ORDER BY sequence_no
            """,
            organization_id,
            session_id,
        )
        return _tracking_from_rows(row, points)

    async def find_active_for_order(
        self, organization_id: str, order_id: str
    ) -> TrackingSession | None:
        session_id = await self._connection.fetchval(
            """
            SELECT id FROM tracking_sessions
            WHERE organization_id = $1 AND work_order_id = $2 AND status = 'active'
            FOR UPDATE
            """,
            organization_id,
            order_id,
        )
        if session_id is None:
            return None
        return await self.get(organization_id, session_id)

    async def save(
        self, session: TrackingSession, *, expected_version: int | None
    ) -> None:
        try:
            if expected_version is None:
                result = await self._connection.execute(
                    """
                    INSERT INTO tracking_sessions (
                        organization_id, id, work_order_id, executor_id,
                        status, version
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (organization_id, id) DO NOTHING
                    """,
                    session.organization_id,
                    session.id,
                    session.work_order_id,
                    session.executor_id,
                    session.status.value,
                    session.version,
                )
                if result != "INSERT 0 1":
                    raise ConcurrencyConflict(
                        f"tracking session {session.id!r} already exists"
                    )
            else:
                result = await self._connection.execute(
                    """
                    UPDATE tracking_sessions SET
                        work_order_id = $3,
                        executor_id = $4,
                        status = $5,
                        version = $6
                    WHERE organization_id = $1 AND id = $2 AND version = $7
                    """,
                    session.organization_id,
                    session.id,
                    session.work_order_id,
                    session.executor_id,
                    session.status.value,
                    session.version,
                    expected_version,
                )
                if result != "UPDATE 1":
                    raise ConcurrencyConflict(
                        f"tracking session {session.id!r} changed concurrently"
                    )
            await self._append_new_points(session)
        except asyncpg.UniqueViolationError as exc:
            raise ConcurrencyConflict(
                "database uniqueness constraint rejected tracking session"
            ) from exc

    async def _append_new_points(self, session: TrackingSession) -> None:
        persisted_count = await self._connection.fetchval(
            """
            SELECT count(*) FROM tracking_points
            WHERE organization_id = $1 AND session_id = $2
            """,
            session.organization_id,
            session.id,
        )
        if persisted_count > len(session.points):
            raise ConcurrencyConflict(
                f"tracking session {session.id!r} lost persisted points"
            )
        new_points = session.points[persisted_count:]
        if not new_points:
            return
        await self._connection.executemany(
            """
            INSERT INTO tracking_points (
                organization_id, session_id, sequence_no, latitude, longitude,
                captured_at, ingested_at, source, accuracy_m, source_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            [
                (
                    session.organization_id,
                    session.id,
                    sequence_no,
                    point.latitude,
                    point.longitude,
                    point.captured_at,
                    point.ingested_at,
                    point.source.value,
                    point.accuracy_m,
                    point.source_event_id,
                )
                for sequence_no, point in enumerate(
                    new_points,
                    start=persisted_count + 1,
                )
            ],
        )


class PostgresOutbox:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def add(self, events: tuple[DomainEvent, ...]) -> None:
        if not events:
            return
        await self._connection.executemany(
            """
            INSERT INTO outbox_events (
                event_id, organization_id, aggregate_type, aggregate_id,
                aggregate_version, event_name, payload, occurred_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            ON CONFLICT (event_id) DO NOTHING
            """,
            [
                (
                    event.event_id,
                    event.organization_id,
                    event.aggregate_type,
                    event.aggregate_id,
                    event.aggregate_version,
                    event.name,
                    _json_text(dict(event.payload)),
                    event.occurred_at,
                )
                for event in events
            ],
        )


def _order_values(order: WorkOrder) -> tuple[Any, ...]:
    report = None
    if order.report is not None:
        report = {
            "photo_refs": list(order.report.photo_refs),
            "comment": order.report.comment,
            "signature_ref": order.report.signature_ref,
            "customer_code": order.report.customer_code,
        }
    requirements = {
        "minimum_photos": order.evidence_requirements.minimum_photos,
        "comment_required": order.evidence_requirements.comment_required,
        "signature_required": order.evidence_requirements.signature_required,
        "customer_code_required": order.evidence_requirements.customer_code_required,
    }
    return (
        order.organization_id,
        order.id,
        order.work_type,
        order.source,
        _json_text(dict(order.details)),
        order.requester_id,
        order.coordinator_id,
        _json_text(requirements),
        order.status.value,
        order.pool_mode.value if order.pool_mode is not None else None,
        order.assignee_id,
        _json_text(report) if report is not None else None,
        order.version,
        order.created_at,
        order.updated_at,
    )


def _order_from_rows(
    row: asyncpg.Record, responses: Iterable[asyncpg.Record]
) -> WorkOrder:
    requirements_data = _json_value(row["evidence_requirements"])
    report_data = _json_value(row["report"])
    report = None
    if report_data is not None:
        report = CompletionReport(
            photo_refs=tuple(report_data.get("photo_refs", ())),
            comment=report_data.get("comment"),
            signature_ref=report_data.get("signature_ref"),
            customer_code=report_data.get("customer_code"),
        )
    pool_responses = {
        item["executor_id"]: PoolResponse(
            executor_id=item["executor_id"],
            status=PoolResponseStatus(item["status"]),
            responded_at=item["responded_at"],
        )
        for item in responses
    }
    return WorkOrder(
        id=row["id"],
        organization_id=row["organization_id"],
        work_type=row["work_type"],
        source=row["source"],
        details=_json_value(row["details"]),
        requester_id=row["requester_id"],
        coordinator_id=row["coordinator_id"],
        evidence_requirements=EvidenceRequirements(**requirements_data),
        status=WorkOrderStatus(row["status"]),
        pool_mode=PoolMode(row["pool_mode"]) if row["pool_mode"] else None,
        assignee_id=row["assignee_id"],
        pool_responses=pool_responses,
        report=report,
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _tracking_from_rows(
    row: asyncpg.Record, points: Iterable[asyncpg.Record]
) -> TrackingSession:
    return TrackingSession(
        id=row["id"],
        organization_id=row["organization_id"],
        work_order_id=row["work_order_id"],
        executor_id=row["executor_id"],
        status=TrackingStatus(row["status"]),
        points=[
            TrackingPoint(
                latitude=item["latitude"],
                longitude=item["longitude"],
                captured_at=item["captured_at"],
                ingested_at=item["ingested_at"],
                source=LocationSource(item["source"]),
                accuracy_m=item["accuracy_m"],
                source_event_id=item["source_event_id"],
            )
            for item in points
        ],
        version=row["version"],
    )
