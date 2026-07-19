from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from dispatch_core.domain.tracking import (
    LocationSource,
    TrackingPoint,
    TrackingSession,
)
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    PoolMode,
    WorkOrder,
)

from .ports import UnitOfWorkFactory


class DispatchService:
    """Transport-neutral application API for the first vertical slice."""

    def __init__(self, unit_of_work: UnitOfWorkFactory) -> None:
        self._unit_of_work = unit_of_work

    def create_order(
        self,
        *,
        organization_id: str,
        work_type: str,
        source: str,
        details: Mapping[str, Any],
        requester_id: str | None = None,
        evidence_requirements: EvidenceRequirements | None = None,
        order_id: str | None = None,
    ) -> WorkOrder:
        order = WorkOrder.create(
            order_id=order_id or str(uuid4()),
            organization_id=organization_id,
            work_type=work_type,
            source=source,
            details=details,
            requester_id=requester_id,
            evidence_requirements=evidence_requirements,
        )
        with self._unit_of_work() as uow:
            uow.orders.save(order, expected_version=None)
            uow.outbox.add(order.pull_events())
            uow.commit()
        return order

    def claim_coordination(self, order_id: str, coordinator_id: str) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.claim_coordination(coordinator_id),
        )

    def publish_pool(
        self,
        order_id: str,
        mode: PoolMode,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.publish_pool(mode, actor_id=actor_id),
        )

    def express_interest(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.express_interest(executor_id),
        )

    def withdraw_interest(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.withdraw_interest(executor_id),
        )

    def claim_first(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.claim_first(executor_id),
        )

    def assign_order(
        self,
        order_id: str,
        executor_id: str,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.assign(executor_id, actor_id=actor_id),
        )

    def accept_order(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id, lambda order: order.accept(executor_id)
        )

    def reject_assignment(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.reject_assignment(executor_id),
        )

    def cancel_order(
        self,
        order_id: str,
        reason: str,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        return self._change_order(
            order_id,
            lambda order: order.cancel(reason, actor_id=actor_id),
        )

    def start_travel(
        self, order_id: str, executor_id: str, *, session_id: str | None = None
    ) -> tuple[WorkOrder, TrackingSession]:
        with self._unit_of_work() as uow:
            order = uow.orders.get(order_id)
            expected_order_version = order.version
            order.start_travel(executor_id)
            session = TrackingSession.start(
                session_id=session_id or str(uuid4()),
                organization_id=order.organization_id,
                work_order_id=order.id,
                executor_id=executor_id,
            )
            uow.orders.save(order, expected_version=expected_order_version)
            uow.tracking.save(session, expected_version=None)
            uow.outbox.add(order.pull_events() + session.pull_events())
            uow.commit()
            return order, session

    def record_location(
        self,
        *,
        session_id: str,
        latitude: float,
        longitude: float,
        source: LocationSource,
        captured_at: datetime | None = None,
        accuracy_m: float | None = None,
        source_event_id: str | None = None,
    ) -> TrackingSession:
        with self._unit_of_work() as uow:
            session = uow.tracking.get(session_id)
            expected_version = session.version
            now = datetime.now(UTC)
            session.add_point(
                TrackingPoint(
                    latitude=latitude,
                    longitude=longitude,
                    captured_at=captured_at or now,
                    ingested_at=now,
                    source=source,
                    accuracy_m=accuracy_m,
                    source_event_id=source_event_id,
                )
            )
            uow.tracking.save(session, expected_version=expected_version)
            uow.outbox.add(session.pull_events())
            uow.commit()
            return session

    def start_work(self, order_id: str, executor_id: str) -> WorkOrder:
        return self._change_order(
            order_id, lambda order: order.start_work(executor_id)
        )

    def complete_order(
        self, order_id: str, executor_id: str, report: CompletionReport
    ) -> WorkOrder:
        with self._unit_of_work() as uow:
            order = uow.orders.get(order_id)
            expected_order_version = order.version
            order.complete(executor_id, report)
            uow.orders.save(order, expected_version=expected_order_version)
            session = uow.tracking.find_active_for_order(order_id)
            events = list(order.pull_events())
            if session is not None:
                expected_session_version = session.version
                session.complete()
                uow.tracking.save(
                    session, expected_version=expected_session_version
                )
                events.extend(session.pull_events())
            uow.outbox.add(tuple(events))
            uow.commit()
            return order

    def _change_order(self, order_id: str, change: Any) -> WorkOrder:
        with self._unit_of_work() as uow:
            order = uow.orders.get(order_id)
            expected_version = order.version
            change(order)
            uow.orders.save(order, expected_version=expected_version)
            uow.outbox.add(order.pull_events())
            uow.commit()
            return order
