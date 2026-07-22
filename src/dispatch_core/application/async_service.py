from __future__ import annotations

from collections.abc import Callable, Mapping
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

from .async_ports import AsyncUnitOfWorkFactory

OrderChange = Callable[[WorkOrder], None]


class AsyncDispatchService:
    """Transactional application commands for durable deployments."""

    def __init__(self, unit_of_work: AsyncUnitOfWorkFactory) -> None:
        self._unit_of_work = unit_of_work

    async def create_order(
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
        async with self._unit_of_work() as uow:
            await uow.orders.save(order, expected_version=None)
            await uow.outbox.add(order.pull_events())
            await uow.commit()
        return order

    async def claim_coordination(
        self, organization_id: str, order_id: str, coordinator_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.claim_coordination(coordinator_id),
        )

    async def publish_pool(
        self,
        organization_id: str,
        order_id: str,
        mode: PoolMode,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.publish_pool(mode, actor_id=actor_id),
        )

    async def express_interest(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.express_interest(executor_id),
        )

    async def withdraw_interest(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.withdraw_interest(executor_id),
        )

    async def claim_first(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.claim_first(executor_id),
        )

    async def assign_order(
        self,
        organization_id: str,
        order_id: str,
        executor_id: str,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.assign(executor_id, actor_id=actor_id),
        )

    async def accept_order(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.accept(executor_id),
        )

    async def reject_assignment(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.reject_assignment(executor_id),
        )

    async def start_travel(
        self,
        organization_id: str,
        order_id: str,
        executor_id: str,
        *,
        session_id: str | None = None,
    ) -> tuple[WorkOrder, TrackingSession]:
        async with self._unit_of_work() as uow:
            order = await uow.orders.get(organization_id, order_id)
            expected_order_version = order.version
            order.start_travel(executor_id)
            session = TrackingSession.start(
                session_id=session_id or str(uuid4()),
                organization_id=organization_id,
                work_order_id=order.id,
                executor_id=executor_id,
            )
            await uow.orders.save(order, expected_version=expected_order_version)
            await uow.tracking.save(session, expected_version=None)
            await uow.outbox.add(order.pull_events() + session.pull_events())
            await uow.commit()
        return order, session

    async def record_location(
        self,
        *,
        organization_id: str,
        executor_id: str,
        session_id: str,
        latitude: float,
        longitude: float,
        source: LocationSource,
        captured_at: datetime | None = None,
        accuracy_m: float | None = None,
        source_event_id: str | None = None,
    ) -> TrackingSession:
        async with self._unit_of_work() as uow:
            session = await uow.tracking.get(organization_id, session_id)
            if session.executor_id != executor_id:
                from dispatch_core.domain.errors import InvalidTransition

                raise InvalidTransition(
                    "session does not belong to this executor"
                )
            expected_version = session.version
            now = datetime.now(UTC)
            observed_at = captured_at or now
            latest = session.latest_point()
            if latest is not None and observed_at < latest.captured_at:
                observed_at = now
            session.add_point(
                TrackingPoint(
                    latitude=latitude,
                    longitude=longitude,
                    captured_at=observed_at,
                    ingested_at=now,
                    source=source,
                    accuracy_m=accuracy_m,
                    source_event_id=source_event_id,
                )
            )
            await uow.tracking.save(session, expected_version=expected_version)
            await uow.outbox.add(session.pull_events())
            await uow.commit()
        return session

    async def start_work(
        self, organization_id: str, order_id: str, executor_id: str
    ) -> WorkOrder:
        return await self._change_order(
            organization_id,
            order_id,
            lambda order: order.start_work(executor_id),
        )

    async def complete_order(
        self,
        organization_id: str,
        order_id: str,
        executor_id: str,
        report: CompletionReport,
    ) -> WorkOrder:
        async with self._unit_of_work() as uow:
            order = await uow.orders.get(organization_id, order_id)
            expected_order_version = order.version
            order.complete(executor_id, report)
            await uow.orders.save(order, expected_version=expected_order_version)
            session = await uow.tracking.find_active_for_order(
                organization_id, order_id
            )
            events = list(order.pull_events())
            if session is not None:
                expected_session_version = session.version
                session.complete()
                await uow.tracking.save(
                    session, expected_version=expected_session_version
                )
                events.extend(session.pull_events())
            await uow.outbox.add(tuple(events))
            await uow.commit()
        return order

    async def cancel_order(
        self,
        organization_id: str,
        order_id: str,
        reason: str,
        *,
        actor_id: str | None = None,
    ) -> WorkOrder:
        async with self._unit_of_work() as uow:
            order = await uow.orders.get(organization_id, order_id)
            expected_order_version = order.version
            order.cancel(reason, actor_id=actor_id)
            await uow.orders.save(order, expected_version=expected_order_version)
            session = await uow.tracking.find_active_for_order(
                organization_id, order_id
            )
            events = list(order.pull_events())
            if session is not None:
                expected_session_version = session.version
                session.cancel(reason)
                await uow.tracking.save(
                    session, expected_version=expected_session_version
                )
                events.extend(session.pull_events())
            await uow.outbox.add(tuple(events))
            await uow.commit()
        return order

    async def _change_order(
        self,
        organization_id: str,
        order_id: str,
        change: OrderChange,
    ) -> WorkOrder:
        async with self._unit_of_work() as uow:
            order = await uow.orders.get(organization_id, order_id)
            expected_version = order.version
            change(order)
            await uow.orders.save(order, expected_version=expected_version)
            await uow.outbox.add(order.pull_events())
            await uow.commit()
        return order
