from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from secrets import compare_digest
from typing import Protocol

import asyncpg

from dispatch_core.domain.errors import NotFound
from dispatch_core.domain.tracking import TrackingPoint, TrackingStatus
from dispatch_core.domain.work_order import WorkOrder

from .async_memory import AsyncMemoryStore
from .postgres import PostgresOrderRepository


class OrderReader(Protocol):
    async def get(self, organization_id: str, order_id: str) -> WorkOrder: ...


@dataclass(frozen=True, slots=True)
class PublicTrackingView:
    public_number: str
    work_type: str
    address: str | None
    client_location: PublicMapPoint | None
    master_name: str | None
    order_status: str
    tracking_status: TrackingStatus
    point_count: int
    latest_point: TrackingPoint | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PublicMapPoint:
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class LocationSubmissionTarget:
    organization_id: str
    session_id: str
    executor_id: str


class TrackingViewReader(Protocol):
    async def resolve(self, public_token: str) -> PublicTrackingView | None: ...

    async def resolve_location_sender(
        self, location_token: str
    ) -> LocationSubmissionTarget | None: ...


class AsyncMemoryOrderReader:
    def __init__(self, store: AsyncMemoryStore) -> None:
        self._store = store

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        order = self._store.orders.get((organization_id, order_id))
        if order is None:
            raise NotFound(f"work order {order_id!r} was not found")
        return deepcopy(order)


class AsyncMemoryTrackingViewReader:
    def __init__(self, store: AsyncMemoryStore) -> None:
        self._store = store

    async def resolve(self, public_token: str) -> PublicTrackingView | None:
        for session in self._store.tracking_sessions.values():
            if (
                session.status is not TrackingStatus.ACTIVE
                or session.public_token is None
                or not compare_digest(session.public_token, public_token)
            ):
                continue
            order = self._store.orders.get(
                (session.organization_id, session.work_order_id)
            )
            if order is None:
                return None
            return PublicTrackingView(
                public_number=order.public_number,
                work_type=order.work_type,
                address=_public_address(order.details),
                client_location=_public_service_location(order.details),
                master_name=session.executor_id,
                order_status=order.status.value,
                tracking_status=session.status,
                point_count=session.point_count,
                latest_point=deepcopy(session.latest_point()),
                updated_at=session.updated_at,
            )
        return None

    async def resolve_location_sender(
        self, location_token: str
    ) -> LocationSubmissionTarget | None:
        for session in self._store.tracking_sessions.values():
            if (
                session.status is TrackingStatus.ACTIVE
                and session.location_token is not None
                and compare_digest(session.location_token, location_token)
            ):
                return LocationSubmissionTarget(
                    organization_id=session.organization_id,
                    session_id=session.id,
                    executor_id=session.executor_id,
                )
        return None


class PostgresOrderReader:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        async with self._pool.acquire() as connection:
            return await PostgresOrderRepository(connection).get_for_read(
                organization_id, order_id
            )


class PostgresTrackingViewReader:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def resolve(self, public_token: str) -> PublicTrackingView | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT tracking.status AS tracking_status,
                       tracking.updated_at,
                       orders.public_number,
                       orders.work_type,
                       orders.status AS order_status,
                       orders.details,
                       executor.display_name AS master_name,
                       latest.latitude,
                       latest.longitude,
                       latest.captured_at,
                       latest.ingested_at,
                       latest.source,
                       latest.accuracy_m,
                       latest.source_event_id,
                       tracking.point_count
                FROM tracking_sessions AS tracking
                JOIN work_orders AS orders
                  ON orders.organization_id = tracking.organization_id
                 AND orders.id = tracking.work_order_id
                LEFT JOIN actors AS executor
                  ON executor.organization_id = tracking.organization_id
                 AND executor.id = tracking.executor_id
                LEFT JOIN LATERAL (
                    SELECT point.latitude, point.longitude, point.captured_at,
                           point.ingested_at, point.source, point.accuracy_m,
                           point.source_event_id
                    FROM tracking_points AS point
                    WHERE point.organization_id = tracking.organization_id
                      AND point.session_id = tracking.id
                    ORDER BY point.sequence_no DESC
                    LIMIT 1
                ) AS latest ON true
                WHERE tracking.public_token = $1
                  AND tracking.status = 'active'
                """,
                public_token,
            )
        if row is None:
            return None
        point = None
        if row["latitude"] is not None:
            from dispatch_core.domain.tracking import LocationSource

            point = TrackingPoint(
                latitude=row["latitude"],
                longitude=row["longitude"],
                captured_at=row["captured_at"],
                ingested_at=row["ingested_at"],
                source=LocationSource(row["source"]),
                accuracy_m=row["accuracy_m"],
                source_event_id=row["source_event_id"],
            )
        details = row["details"]
        return PublicTrackingView(
            public_number=row["public_number"],
            work_type=row["work_type"],
            address=_public_address(details),
            client_location=_public_service_location(details),
            master_name=row["master_name"],
            order_status=row["order_status"],
            tracking_status=TrackingStatus(row["tracking_status"]),
            point_count=row["point_count"],
            latest_point=point,
            updated_at=row["updated_at"],
        )

    async def resolve_location_sender(
        self, location_token: str
    ) -> LocationSubmissionTarget | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT organization_id, id, executor_id
                FROM tracking_sessions
                WHERE location_token = $1 AND status = 'active'
                """,
                location_token,
            )
        if row is None:
            return None
        return LocationSubmissionTarget(
            organization_id=row["organization_id"],
            session_id=row["id"],
            executor_id=row["executor_id"],
        )


def _public_address(details: object) -> str | None:
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except json.JSONDecodeError:
            return None
    if not isinstance(details, Mapping):
        return None
    for key in ("address", "destination"):
        value = details.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _public_service_location(details: object) -> PublicMapPoint | None:
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except json.JSONDecodeError:
            return None
    if not isinstance(details, Mapping):
        return None
    location = details.get("service_location")
    if not isinstance(location, Mapping):
        return None
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    if (
        isinstance(latitude, bool)
        or isinstance(longitude, bool)
        or not isinstance(latitude, (int, float))
        or not isinstance(longitude, (int, float))
    ):
        return None
    latitude = float(latitude)
    longitude = float(longitude)
    if (
        not isfinite(latitude)
        or not isfinite(longitude)
        or not -90 <= latitude <= 90
        or not -180 <= longitude <= 180
    ):
        return None
    return PublicMapPoint(latitude=latitude, longitude=longitude)
