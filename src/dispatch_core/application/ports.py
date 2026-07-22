from __future__ import annotations

from types import TracebackType
from typing import Protocol

from dispatch_core.domain.events import DomainEvent
from dispatch_core.domain.tracking import TrackingSession
from dispatch_core.domain.work_order import WorkOrder


class WorkOrderRepository(Protocol):
    def get(self, organization_id: str, order_id: str) -> WorkOrder: ...

    def save(self, order: WorkOrder, *, expected_version: int | None) -> None: ...


class TrackingRepository(Protocol):
    def get(
        self, organization_id: str, session_id: str
    ) -> TrackingSession: ...

    def find_active_for_order(
        self, organization_id: str, order_id: str
    ) -> TrackingSession | None: ...

    def save(
        self, session: TrackingSession, *, expected_version: int | None
    ) -> None: ...


class Outbox(Protocol):
    def add(self, events: tuple[DomainEvent, ...]) -> None: ...


class UnitOfWork(Protocol):
    orders: WorkOrderRepository
    tracking: TrackingRepository
    outbox: Outbox

    def __enter__(self) -> UnitOfWork: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> UnitOfWork: ...
