from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from typing import Any

from .errors import InvalidTransition
from .events import DomainEvent


class TrackingStatus(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class LocationSource(StrEnum):
    TELEGRAM = "telegram"
    MAX = "max"
    WEB = "web"
    MOBILE = "mobile"
    IMPORT = "import"


@dataclass(frozen=True, slots=True)
class TrackingPoint:
    latitude: float
    longitude: float
    captured_at: datetime
    ingested_at: datetime
    source: LocationSource
    accuracy_m: float | None = None
    source_event_id: str | None = None

    def __post_init__(self) -> None:
        if not isfinite(self.latitude) or not -90 <= self.latitude <= 90:
            raise ValueError("latitude must be finite and between -90 and 90")
        if not isfinite(self.longitude) or not -180 <= self.longitude <= 180:
            raise ValueError("longitude must be finite and between -180 and 180")
        if self.accuracy_m is not None and (
            not isfinite(self.accuracy_m) or self.accuracy_m < 0
        ):
            raise ValueError("accuracy_m must be a non-negative finite number")
        if self.source_event_id is not None and not self.source_event_id.strip():
            raise ValueError("source_event_id cannot be blank")


@dataclass(slots=True)
class TrackingSession:
    id: str
    organization_id: str
    work_order_id: str
    executor_id: str
    status: TrackingStatus = TrackingStatus.ACTIVE
    points: list[TrackingPoint] = field(default_factory=list)
    version: int = 0
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    @classmethod
    def start(
        cls,
        *,
        session_id: str,
        organization_id: str,
        work_order_id: str,
        executor_id: str,
        now: datetime | None = None,
    ) -> TrackingSession:
        if not all((session_id, organization_id, work_order_id, executor_id)):
            raise ValueError("tracking session identifiers are required")
        session = cls(session_id, organization_id, work_order_id, executor_id)
        session._record("tracking.started", {}, now)
        return session

    def add_point(self, point: TrackingPoint) -> None:
        if point.source_event_id is not None and any(
            current.source_event_id == point.source_event_id
            for current in self.points
        ):
            return
        if self.status is not TrackingStatus.ACTIVE:
            raise InvalidTransition("tracking session is not active")
        if self.points and point.captured_at < self.points[-1].captured_at:
            raise ValueError("tracking points must be ordered by capture time")
        self.points.append(point)
        self._record(
            "tracking.point_recorded",
            {
                "captured_at": point.captured_at.isoformat(),
                "source": point.source.value,
                "source_event_id": point.source_event_id,
            },
            point.ingested_at,
        )

    def complete(self, *, now: datetime | None = None) -> None:
        if self.status is not TrackingStatus.ACTIVE:
            raise InvalidTransition("tracking session is not active")
        self.status = TrackingStatus.COMPLETED
        self._record("tracking.completed", {"point_count": len(self.points)}, now)

    def latest_point(self) -> TrackingPoint | None:
        return self.points[-1] if self.points else None

    def pull_events(self) -> tuple[DomainEvent, ...]:
        events = tuple(self._events)
        self._events.clear()
        return events

    def _record(
        self,
        name: str,
        payload: Mapping[str, Any],
        now: datetime | None,
    ) -> None:
        instant = now or datetime.now(UTC)
        self.version += 1
        self._events.append(
            DomainEvent.create(
                organization_id=self.organization_id,
                aggregate_type="tracking_session",
                aggregate_id=self.id,
                aggregate_version=self.version,
                name=name,
                payload=payload,
                occurred_at=instant,
            )
        )
