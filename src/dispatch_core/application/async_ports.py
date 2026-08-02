from __future__ import annotations

from types import TracebackType
from typing import Protocol

from dispatch_core.domain.events import DomainEvent
from dispatch_core.domain.tracking import TrackingSession
from dispatch_core.domain.work_order import WorkOrder


class AsyncWorkOrderRepository(Protocol):
    async def allocate_public_number(self, organization_id: str) -> str: ...

    async def get(self, organization_id: str, order_id: str) -> WorkOrder: ...

    async def save(self, order: WorkOrder, *, expected_version: int | None) -> None: ...


class AsyncTrackingRepository(Protocol):
    async def get(self, organization_id: str, session_id: str) -> TrackingSession: ...

    async def find_active_for_order(
        self, organization_id: str, order_id: str
    ) -> TrackingSession | None: ...

    async def has_source_event(
        self,
        organization_id: str,
        source: str,
        source_event_id: str,
    ) -> bool: ...

    async def save(
        self, session: TrackingSession, *, expected_version: int | None
    ) -> None: ...


class AsyncOutbox(Protocol):
    async def add(self, events: tuple[DomainEvent, ...]) -> None: ...


class AsyncUnitOfWork(Protocol):
    orders: AsyncWorkOrderRepository
    tracking: AsyncTrackingRepository
    outbox: AsyncOutbox

    async def __aenter__(self) -> AsyncUnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...


class AsyncUnitOfWorkFactory(Protocol):
    def __call__(self) -> AsyncUnitOfWork: ...
