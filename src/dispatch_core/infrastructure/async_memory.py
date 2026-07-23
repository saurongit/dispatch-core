from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from types import TracebackType

from dispatch_core.domain.errors import ConcurrencyConflict, NotFound
from dispatch_core.domain.events import DomainEvent
from dispatch_core.domain.order_numbers import format_order_number
from dispatch_core.domain.tracking import TrackingSession, TrackingStatus
from dispatch_core.domain.work_order import WorkOrder


@dataclass(slots=True)
class AsyncMemoryStore:
    orders: dict[tuple[str, str], WorkOrder] = field(default_factory=dict)
    tracking_sessions: dict[tuple[str, str], TrackingSession] = field(
        default_factory=dict
    )
    outbox_events: list[DomainEvent] = field(default_factory=list)
    order_number_counters: dict[str, int] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AsyncMemoryUnitOfWorkFactory:
    def __init__(self, store: AsyncMemoryStore | None = None) -> None:
        self.store = store or AsyncMemoryStore()

    def __call__(self) -> AsyncMemoryUnitOfWork:
        return AsyncMemoryUnitOfWork(self.store)


class AsyncMemoryUnitOfWork:
    def __init__(self, store: AsyncMemoryStore) -> None:
        self._store = store
        self._staged_orders: dict[tuple[str, str], tuple[WorkOrder, int | None]] = {}
        self._staged_tracking: dict[
            tuple[str, str], tuple[TrackingSession, int | None]
        ] = {}
        self._staged_events: list[DomainEvent] = []
        self._committed = False
        self.orders = _AsyncOrderRepository(self)
        self.tracking = _AsyncTrackingRepository(self)
        self.outbox = _AsyncOutbox(self)

    async def __aenter__(self) -> AsyncMemoryUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None or not self._committed:
            self._clear()

    async def commit(self) -> None:
        async with self._store.lock:
            self._check_versions()
            self._check_executor_capacity()
            for key, (order, _) in self._staged_orders.items():
                self._store.orders[key] = deepcopy(order)
            for key, (session, _) in self._staged_tracking.items():
                self._store.tracking_sessions[key] = deepcopy(session)
            self._store.outbox_events.extend(deepcopy(self._staged_events))
            self._committed = True
            self._clear()

    def _check_versions(self) -> None:
        self._check_collection_versions(self._store.orders, self._staged_orders)
        self._check_collection_versions(
            self._store.tracking_sessions,
            self._staged_tracking,
        )

    def _check_executor_capacity(self) -> None:
        active_statuses = {"assigned", "accepted", "en_route", "in_progress"}
        merged = dict(self._store.orders)
        merged.update({key: order for key, (order, _) in self._staged_orders.items()})
        occupied: set[tuple[str, str]] = set()
        for order in merged.values():
            if order.assignee_id is None or order.status.value not in active_statuses:
                continue
            key = (order.organization_id, order.assignee_id)
            if key in occupied:
                raise ConcurrencyConflict(
                    "executor already has another active work order"
                )
            occupied.add(key)

    @staticmethod
    def _check_collection_versions(
        stored: dict[tuple[str, str], WorkOrder]
        | dict[tuple[str, str], TrackingSession],
        staged: dict[tuple[str, str], tuple[WorkOrder, int | None]]
        | dict[tuple[str, str], tuple[TrackingSession, int | None]],
    ) -> None:
        for key, (_, expected_version) in staged.items():
            current = stored.get(key)
            if expected_version is None:
                if current is not None:
                    raise ConcurrencyConflict(f"aggregate {key!r} already exists")
            elif current is None or current.version != expected_version:
                actual = None if current is None else current.version
                raise ConcurrencyConflict(
                    f"aggregate {key!r} expected version "
                    f"{expected_version}, found {actual}"
                )

    def _clear(self) -> None:
        self._staged_orders.clear()
        self._staged_tracking.clear()
        self._staged_events.clear()


class _AsyncOrderRepository:
    def __init__(self, unit_of_work: AsyncMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    async def allocate_public_number(self, organization_id: str) -> str:
        async with self._uow._store.lock:
            sequence = (
                self._uow._store.order_number_counters.get(organization_id, 0) + 1
            )
            self._uow._store.order_number_counters[organization_id] = sequence
        return format_order_number(sequence)

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        order = self._uow._store.orders.get((organization_id, order_id))
        if order is None:
            raise NotFound(f"work order {order_id!r} was not found")
        return deepcopy(order)

    async def save(self, order: WorkOrder, *, expected_version: int | None) -> None:
        snapshot = deepcopy(order)
        snapshot.pull_events()
        self._uow._staged_orders[(order.organization_id, order.id)] = (
            snapshot,
            expected_version,
        )


class _AsyncTrackingRepository:
    def __init__(self, unit_of_work: AsyncMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    async def get(self, organization_id: str, session_id: str) -> TrackingSession:
        session = self._uow._store.tracking_sessions.get((organization_id, session_id))
        if session is None:
            raise NotFound(f"tracking session {session_id!r} was not found")
        return deepcopy(session)

    async def find_active_for_order(
        self, organization_id: str, order_id: str
    ) -> TrackingSession | None:
        matches = [
            session
            for (stored_organization_id, _), session in (
                self._uow._store.tracking_sessions.items()
            )
            if stored_organization_id == organization_id
            and session.work_order_id == order_id
            and session.status is TrackingStatus.ACTIVE
        ]
        if len(matches) > 1:
            raise RuntimeError("more than one active tracking session for work order")
        return deepcopy(matches[0]) if matches else None

    async def save(
        self, session: TrackingSession, *, expected_version: int | None
    ) -> None:
        snapshot = deepcopy(session)
        snapshot.pull_events()
        self._uow._staged_tracking[(session.organization_id, session.id)] = (
            snapshot,
            expected_version,
        )


class _AsyncOutbox:
    def __init__(self, unit_of_work: AsyncMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    async def add(self, events: tuple[DomainEvent, ...]) -> None:
        self._uow._staged_events.extend(deepcopy(events))
