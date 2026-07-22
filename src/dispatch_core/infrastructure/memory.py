from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from threading import RLock
from types import TracebackType

from dispatch_core.domain.errors import ConcurrencyConflict, NotFound
from dispatch_core.domain.events import DomainEvent
from dispatch_core.domain.tracking import TrackingSession, TrackingStatus
from dispatch_core.domain.work_order import WorkOrder


@dataclass(slots=True)
class MemoryStore:
    orders: dict[str, WorkOrder] = field(default_factory=dict)
    tracking_sessions: dict[tuple[str, str], TrackingSession] = field(
        default_factory=dict
    )
    outbox_events: list[DomainEvent] = field(default_factory=list)
    lock: RLock = field(default_factory=RLock)


class _OrderRepository:
    def __init__(self, unit_of_work: InMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    def get(self, organization_id: str, order_id: str) -> WorkOrder:
        stored = self._uow._store.orders.get(order_id)
        if stored is None:
            raise NotFound(f"work order {order_id!r} was not found")
        if stored.organization_id != organization_id:
            raise NotFound(f"work order {order_id!r} was not found")
        return deepcopy(stored)

    def save(self, order: WorkOrder, *, expected_version: int | None) -> None:
        snapshot = deepcopy(order)
        snapshot.pull_events()
        self._uow._staged_orders[order.id] = (snapshot, expected_version)


class _TrackingRepository:
    def __init__(self, unit_of_work: InMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    def get(
        self, organization_id: str, session_id: str
    ) -> TrackingSession:
        stored = self._uow._store.tracking_sessions.get(
            (organization_id, session_id)
        )
        if stored is None:
            raise NotFound(f"tracking session {session_id!r} was not found")
        return deepcopy(stored)

    def find_active_for_order(
        self, organization_id: str, order_id: str
    ) -> TrackingSession | None:
        matches = [
            item
            for (stored_org, _), item in (
                self._uow._store.tracking_sessions.items()
            )
            if stored_org == organization_id
            and item.work_order_id == order_id
            and item.status is TrackingStatus.ACTIVE
        ]
        if len(matches) > 1:
            raise RuntimeError("more than one active tracking session for work order")
        return deepcopy(matches[0]) if matches else None

    def save(
        self, session: TrackingSession, *, expected_version: int | None
    ) -> None:
        snapshot = deepcopy(session)
        snapshot.pull_events()
        self._uow._staged_tracking[(session.organization_id, session.id)] = (
            snapshot,
            expected_version,
        )


class _Outbox:
    def __init__(self, unit_of_work: InMemoryUnitOfWork) -> None:
        self._uow = unit_of_work

    def add(self, events: tuple[DomainEvent, ...]) -> None:
        self._uow._staged_events.extend(deepcopy(events))


class InMemoryUnitOfWork:
    def __init__(self, store: MemoryStore) -> None:
        self._store = store
        self._staged_orders: dict[str, tuple[WorkOrder, int | None]] = {}
        self._staged_tracking: dict[str, tuple[TrackingSession, int | None]] = {}
        self._staged_events: list[DomainEvent] = []
        self._committed = False
        self.orders = _OrderRepository(self)
        self.tracking = _TrackingRepository(self)
        self.outbox = _Outbox(self)

    def __enter__(self) -> InMemoryUnitOfWork:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None or not self._committed:
            self._clear()

    def commit(self) -> None:
        with self._store.lock:
            self._check_versions()
            self._check_executor_capacity()
            for aggregate_id, (order, _) in self._staged_orders.items():
                self._store.orders[aggregate_id] = deepcopy(order)
            for aggregate_id, (session, _) in self._staged_tracking.items():
                self._store.tracking_sessions[aggregate_id] = deepcopy(session)
            self._store.outbox_events.extend(deepcopy(self._staged_events))
            self._committed = True
            self._clear()

    def _check_versions(self) -> None:
        self._check_collection_versions(self._store.orders, self._staged_orders)
        self._check_collection_versions(
            self._store.tracking_sessions, self._staged_tracking
        )

    def _check_executor_capacity(self) -> None:
        active_statuses = {"assigned", "accepted", "en_route", "in_progress"}
        merged = dict(self._store.orders)
        merged.update(
            {order.id: order for order, _ in self._staged_orders.values()}
        )
        occupied: set[str] = set()
        for order in merged.values():
            if order.assignee_id is None or order.status.value not in active_statuses:
                continue
            key = f"{order.organization_id}:{order.assignee_id}"
            if key in occupied:
                raise ConcurrencyConflict(
                    "executor already has another active work order"
                )
            occupied.add(key)

    @staticmethod
    def _check_collection_versions(
        stored: dict[str, WorkOrder] | dict[tuple[str, str], TrackingSession],
        staged: dict[str, tuple[WorkOrder, int | None]]
        | dict[tuple[str, str], tuple[TrackingSession, int | None]],
    ) -> None:
        for aggregate_id, (_, expected_version) in staged.items():
            current = stored.get(aggregate_id)
            if expected_version is None:
                if current is not None:
                    raise ConcurrencyConflict(
                        f"aggregate {aggregate_id!r} already exists"
                    )
            elif current is None or current.version != expected_version:
                actual = None if current is None else current.version
                raise ConcurrencyConflict(
                    f"aggregate {aggregate_id!r} expected version "
                    f"{expected_version}, found {actual}"
                )

    def _clear(self) -> None:
        self._staged_orders.clear()
        self._staged_tracking.clear()
        self._staged_events.clear()


class InMemoryUnitOfWorkFactory:
    def __init__(self, store: MemoryStore | None = None) -> None:
        self.store = store or MemoryStore()

    def __call__(self) -> InMemoryUnitOfWork:
        return InMemoryUnitOfWork(self.store)
