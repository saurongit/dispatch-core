from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from .errors import EvidenceMissing, InvalidTransition
from .events import DomainEvent


class PoolMode(StrEnum):
    """How an executor is selected from an open pool."""

    CURATED = "curated"
    FIRST_CLAIM = "first_claim"


class PoolResponseStatus(StrEnum):
    INTERESTED = "interested"
    SELECTED = "selected"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"


class WorkOrderStatus(StrEnum):
    SUBMITTED = "submitted"
    POOL_OPEN = "pool_open"
    ASSIGNED = "assigned"
    ACCEPTED = "accepted"
    EN_ROUTE = "en_route"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED}


@dataclass(frozen=True, slots=True)
class EvidenceRequirements:
    minimum_photos: int = 0
    comment_required: bool = False
    signature_required: bool = False
    customer_code_required: bool = False

    def __post_init__(self) -> None:
        if self.minimum_photos < 0:
            raise ValueError("minimum_photos cannot be negative")


@dataclass(frozen=True, slots=True)
class CompletionReport:
    photo_refs: tuple[str, ...] = ()
    comment: str | None = None
    signature_ref: str | None = None
    customer_code: str | None = None


@dataclass(frozen=True, slots=True)
class PoolResponse:
    executor_id: str
    status: PoolResponseStatus
    responded_at: datetime


@dataclass(slots=True)
class WorkOrder:
    id: str
    organization_id: str
    work_type: str
    source: str
    details: Mapping[str, Any]
    requester_id: str | None = None
    coordinator_id: str | None = None
    evidence_requirements: EvidenceRequirements = field(
        default_factory=EvidenceRequirements
    )
    status: WorkOrderStatus = WorkOrderStatus.SUBMITTED
    pool_mode: PoolMode | None = None
    assignee_id: str | None = None
    pool_responses: dict[str, PoolResponse] = field(default_factory=dict)
    report: CompletionReport | None = None
    version: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        if not self.id or not self.organization_id or not self.work_type:
            raise ValueError("id, organization_id and work_type are required")
        self.details = MappingProxyType(deepcopy(dict(self.details)))
        self.pool_responses = dict(self.pool_responses)

    def __deepcopy__(self, memo: dict[int, Any]) -> WorkOrder:
        copied = WorkOrder(
            id=self.id,
            organization_id=self.organization_id,
            work_type=self.work_type,
            source=self.source,
            details=deepcopy(dict(self.details), memo),
            requester_id=self.requester_id,
            coordinator_id=self.coordinator_id,
            evidence_requirements=self.evidence_requirements,
            status=self.status,
            pool_mode=self.pool_mode,
            assignee_id=self.assignee_id,
            pool_responses=deepcopy(self.pool_responses, memo),
            report=self.report,
            version=self.version,
            created_at=self.created_at,
            updated_at=self.updated_at,
            _events=deepcopy(self._events, memo),
        )
        memo[id(self)] = copied
        return copied

    @classmethod
    def create(
        cls,
        *,
        order_id: str,
        organization_id: str,
        work_type: str,
        source: str,
        details: Mapping[str, Any],
        requester_id: str | None = None,
        evidence_requirements: EvidenceRequirements | None = None,
        now: datetime | None = None,
    ) -> WorkOrder:
        instant = now or datetime.now(UTC)
        order = cls(
            id=order_id,
            organization_id=organization_id,
            work_type=work_type,
            source=source,
            details=details,
            requester_id=requester_id,
            evidence_requirements=evidence_requirements or EvidenceRequirements(),
            created_at=instant,
            updated_at=instant,
        )
        order._record(
            "work_order.submitted",
            {"source": source, "requester_id": requester_id},
            instant,
        )
        return order

    def claim_coordination(
        self, coordinator_id: str, *, now: datetime | None = None
    ) -> None:
        self._require_not_terminal()
        if not coordinator_id:
            raise ValueError("coordinator_id is required")
        if self.coordinator_id == coordinator_id:
            return
        if self.coordinator_id is not None:
            raise InvalidTransition("work order is coordinated by another actor")
        self.coordinator_id = coordinator_id
        self._record(
            "work_order.coordination_claimed",
            {"coordinator_id": coordinator_id},
            now,
        )

    def publish_pool(
        self,
        mode: PoolMode,
        *,
        actor_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        self._require_status(WorkOrderStatus.SUBMITTED)
        self._require_coordinator_if_present(actor_id)
        self.pool_mode = mode
        self.status = WorkOrderStatus.POOL_OPEN
        self._record(
            "work_order.pool_published",
            {"mode": mode.value, "actor_id": actor_id},
            now,
        )

    def express_interest(
        self, executor_id: str, *, now: datetime | None = None
    ) -> None:
        self._require_pool_mode(PoolMode.CURATED)
        self._require_executor_id(executor_id)
        current = self.pool_responses.get(executor_id)
        if current and current.status is PoolResponseStatus.INTERESTED:
            return
        instant = now or datetime.now(UTC)
        self.pool_responses[executor_id] = PoolResponse(
            executor_id=executor_id,
            status=PoolResponseStatus.INTERESTED,
            responded_at=instant,
        )
        self._record(
            "work_order.pool_interest_recorded",
            {"executor_id": executor_id},
            instant,
        )

    def withdraw_interest(
        self, executor_id: str, *, now: datetime | None = None
    ) -> None:
        self._require_pool_mode(PoolMode.CURATED)
        current = self.pool_responses.get(executor_id)
        if current is None or current.status is not PoolResponseStatus.INTERESTED:
            raise InvalidTransition("executor has no active interest")
        instant = now or datetime.now(UTC)
        self.pool_responses[executor_id] = PoolResponse(
            executor_id=executor_id,
            status=PoolResponseStatus.WITHDRAWN,
            responded_at=instant,
        )
        self._record(
            "work_order.pool_interest_withdrawn",
            {"executor_id": executor_id},
            instant,
        )

    def claim_first(
        self, executor_id: str, *, now: datetime | None = None
    ) -> None:
        self._require_pool_mode(PoolMode.FIRST_CLAIM)
        self._require_executor_id(executor_id)
        instant = now or datetime.now(UTC)
        self.assignee_id = executor_id
        self.status = WorkOrderStatus.ASSIGNED
        self.pool_responses[executor_id] = PoolResponse(
            executor_id=executor_id,
            status=PoolResponseStatus.SELECTED,
            responded_at=instant,
        )
        self._record(
            "work_order.first_claim_won",
            {"executor_id": executor_id},
            instant,
        )

    def assign(
        self,
        executor_id: str,
        *,
        actor_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        self._require_status(WorkOrderStatus.SUBMITTED, WorkOrderStatus.POOL_OPEN)
        self._require_coordinator_if_present(actor_id)
        self._require_executor_id(executor_id)
        instant = now or datetime.now(UTC)
        self.assignee_id = executor_id
        self.status = WorkOrderStatus.ASSIGNED
        if self.pool_mode is PoolMode.CURATED:
            self._resolve_curated_responses(executor_id, instant)
        self._record(
            "work_order.assigned",
            {"executor_id": executor_id, "actor_id": actor_id},
            instant,
        )

    def accept(self, executor_id: str, *, now: datetime | None = None) -> None:
        self._require_status(WorkOrderStatus.ASSIGNED)
        self._require_assignee(executor_id)
        self.status = WorkOrderStatus.ACCEPTED
        self._record("work_order.accepted", {"executor_id": executor_id}, now)

    def reject_assignment(
        self, executor_id: str, *, now: datetime | None = None
    ) -> None:
        self._require_status(WorkOrderStatus.ASSIGNED)
        self._require_assignee(executor_id)
        instant = now or datetime.now(UTC)
        response = self.pool_responses.get(executor_id)
        if response is not None:
            self.pool_responses[executor_id] = PoolResponse(
                executor_id=executor_id,
                status=PoolResponseStatus.WITHDRAWN,
                responded_at=instant,
            )
        self.assignee_id = None
        self.status = (
            WorkOrderStatus.POOL_OPEN
            if self.pool_mode is not None
            else WorkOrderStatus.SUBMITTED
        )
        self._record(
            "work_order.assignment_rejected",
            {"executor_id": executor_id},
            instant,
        )

    def start_travel(self, executor_id: str, *, now: datetime | None = None) -> None:
        self._require_assignee(executor_id)
        self._require_status(WorkOrderStatus.ACCEPTED)
        self.status = WorkOrderStatus.EN_ROUTE
        self._record("work_order.travel_started", {"executor_id": executor_id}, now)

    def start_work(self, executor_id: str, *, now: datetime | None = None) -> None:
        self._require_assignee(executor_id)
        self._require_status(WorkOrderStatus.ACCEPTED, WorkOrderStatus.EN_ROUTE)
        self.status = WorkOrderStatus.IN_PROGRESS
        self._record("work_order.started", {"executor_id": executor_id}, now)

    def complete(
        self,
        executor_id: str,
        report: CompletionReport,
        *,
        now: datetime | None = None,
    ) -> None:
        self._require_assignee(executor_id)
        self._require_status(WorkOrderStatus.IN_PROGRESS)
        self._validate_report(report)
        self.report = report
        self.status = WorkOrderStatus.COMPLETED
        self._record(
            "work_order.completed",
            {"executor_id": executor_id, "photo_count": len(report.photo_refs)},
            now,
        )

    def cancel(
        self,
        reason: str,
        *,
        actor_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        self._require_not_terminal()
        self._require_coordinator_if_present(actor_id)
        if not reason or not reason.strip():
            raise ValueError("cancellation reason is required")
        self.status = WorkOrderStatus.CANCELLED
        self._record(
            "work_order.cancelled",
            {"reason": reason.strip(), "actor_id": actor_id},
            now,
        )

    def interested_executor_ids(self) -> tuple[str, ...]:
        return tuple(
            executor_id
            for executor_id, response in self.pool_responses.items()
            if response.status is PoolResponseStatus.INTERESTED
        )

    def pull_events(self) -> tuple[DomainEvent, ...]:
        events = tuple(self._events)
        self._events.clear()
        return events

    def _resolve_curated_responses(
        self, selected_executor_id: str, instant: datetime
    ) -> None:
        for executor_id, response in tuple(self.pool_responses.items()):
            if response.status is not PoolResponseStatus.INTERESTED:
                continue
            status = (
                PoolResponseStatus.SELECTED
                if executor_id == selected_executor_id
                else PoolResponseStatus.REJECTED
            )
            self.pool_responses[executor_id] = PoolResponse(
                executor_id=executor_id,
                status=status,
                responded_at=instant,
            )

    def _validate_report(self, report: CompletionReport) -> None:
        missing: list[str] = []
        rules = self.evidence_requirements
        if len(report.photo_refs) < rules.minimum_photos:
            missing.append(f"at least {rules.minimum_photos} photo(s)")
        if rules.comment_required and not (report.comment and report.comment.strip()):
            missing.append("comment")
        if rules.signature_required and not report.signature_ref:
            missing.append("signature")
        if rules.customer_code_required and not report.customer_code:
            missing.append("customer code")
        if missing:
            raise EvidenceMissing("missing completion evidence: " + ", ".join(missing))

    def _require_not_terminal(self) -> None:
        if self.status in TERMINAL_STATUSES:
            raise InvalidTransition(f"work order is already {self.status.value}")

    def _require_coordinator_if_present(self, actor_id: str | None) -> None:
        if self.coordinator_id is not None and actor_id != self.coordinator_id:
            raise InvalidTransition("only the assigned coordinator may act")

    def _require_assignee(self, executor_id: str) -> None:
        if self.assignee_id != executor_id:
            raise InvalidTransition(
                "only the assigned executor may perform this action"
            )

    @staticmethod
    def _require_executor_id(executor_id: str) -> None:
        if not executor_id:
            raise ValueError("executor_id is required")

    def _require_pool_mode(self, mode: PoolMode) -> None:
        self._require_status(WorkOrderStatus.POOL_OPEN)
        if self.pool_mode is not mode:
            raise InvalidTransition(
                f"pool mode is {self.pool_mode}; expected {mode.value}"
            )

    def _require_status(self, *allowed: WorkOrderStatus) -> None:
        if self.status not in allowed:
            choices = ", ".join(item.value for item in allowed)
            raise InvalidTransition(
                f"status {self.status.value} is not allowed; expected one of: {choices}"
            )

    def _record(
        self,
        name: str,
        payload: Mapping[str, Any],
        now: datetime | None,
    ) -> None:
        instant = now or datetime.now(UTC)
        self.version += 1
        self.updated_at = instant
        self._events.append(
            DomainEvent.create(
                organization_id=self.organization_id,
                aggregate_type="work_order",
                aggregate_id=self.id,
                aggregate_version=self.version,
                name=name,
                payload=payload,
                occurred_at=instant,
            )
        )
