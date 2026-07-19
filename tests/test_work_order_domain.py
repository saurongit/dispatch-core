from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from dispatch_core.domain.errors import InvalidTransition
from dispatch_core.domain.work_order import (
    CompletionReport,
    PoolMode,
    PoolResponseStatus,
    WorkOrder,
    WorkOrderStatus,
)


def make_order(**overrides: object) -> WorkOrder:
    values: dict[str, object] = {
        "order_id": "order-1",
        "organization_id": "org-1",
        "work_type": "repair",
        "source": "phone",
        "details": {"asset": "lift-42"},
        "now": datetime(2026, 7, 19, 8, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return WorkOrder.create(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("order_id", ""),
        ("organization_id", ""),
        ("work_type", ""),
    ],
)
def test_create_requires_core_identifiers(field: str, value: str) -> None:
    with pytest.raises(ValueError):
        make_order(**{field: value})


def test_create_copies_and_freezes_details() -> None:
    details = {"nested": {"priority": "urgent"}}
    order = make_order(details=details)
    details["nested"]["priority"] = "routine"

    assert order.details["nested"]["priority"] == "urgent"
    with pytest.raises(TypeError):
        order.details["new"] = "value"  # type: ignore[index]


def test_deepcopy_preserves_read_only_details_and_pending_events() -> None:
    order = make_order()
    copied = deepcopy(order)

    assert copied is not order
    assert dict(copied.details) == dict(order.details)
    assert copied.pull_events() == order.pull_events()
    with pytest.raises(TypeError):
        copied.details["asset"] = "changed"  # type: ignore[index]


def test_submitted_event_contains_source_and_requester() -> None:
    order = make_order(requester_id="resident-7")

    event = order.pull_events()[0]
    assert event.name == "work_order.submitted"
    assert event.aggregate_version == 1
    assert event.payload == {
        "source": "phone",
        "requester_id": "resident-7",
    }


@pytest.mark.parametrize("mode", list(PoolMode))
def test_publish_pool_supports_each_configured_mode(mode: PoolMode) -> None:
    order = make_order()
    order.pull_events()

    order.publish_pool(mode)

    assert order.status is WorkOrderStatus.POOL_OPEN
    assert order.pool_mode is mode
    assert order.pull_events()[0].payload["mode"] == mode.value


def test_curated_interest_is_idempotent() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    order.pull_events()
    instant = datetime(2026, 7, 19, 9, 0, tzinfo=UTC)

    order.express_interest("executor-1", now=instant)
    version = order.version
    first_response = order.pool_responses["executor-1"]
    order.express_interest("executor-1", now=instant + timedelta(minutes=1))

    assert order.version == version
    assert order.pool_responses["executor-1"] == first_response
    assert len(order.pull_events()) == 1


def test_withdrawn_executor_can_express_interest_again() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    order.express_interest("executor-1")
    order.withdraw_interest("executor-1")
    order.express_interest("executor-1")

    assert order.pool_responses["executor-1"].status is PoolResponseStatus.INTERESTED


def test_withdraw_without_interest_is_rejected() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)

    with pytest.raises(InvalidTransition, match="no active interest"):
        order.withdraw_interest("executor-1")


@pytest.mark.parametrize("executor_id", ["executor-1", "executor-2", "crew-a"])
def test_first_claim_assigns_clicking_executor(executor_id: str) -> None:
    order = make_order()
    order.publish_pool(PoolMode.FIRST_CLAIM)

    order.claim_first(executor_id)

    assert order.status is WorkOrderStatus.ASSIGNED
    assert order.assignee_id == executor_id
    assert order.pool_responses[executor_id].status is PoolResponseStatus.SELECTED


def test_second_first_claim_is_rejected_by_state_machine() -> None:
    order = make_order()
    order.publish_pool(PoolMode.FIRST_CLAIM)
    order.claim_first("executor-1")

    with pytest.raises(InvalidTransition):
        order.claim_first("executor-2")


def test_curated_selection_resolves_every_active_interest() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    for executor_id in ("executor-1", "executor-2", "executor-3"):
        order.express_interest(executor_id)

    order.assign("executor-2")

    assert order.pool_responses["executor-2"].status is PoolResponseStatus.SELECTED
    assert order.pool_responses["executor-1"].status is PoolResponseStatus.REJECTED
    assert order.pool_responses["executor-3"].status is PoolResponseStatus.REJECTED


def test_operator_can_direct_assign_executor_without_pool_interest() -> None:
    order = make_order()
    order.claim_coordination("operator-1")
    order.publish_pool(PoolMode.CURATED, actor_id="operator-1")

    order.assign("known-executor", actor_id="operator-1")

    assert order.assignee_id == "known-executor"
    assert order.status is WorkOrderStatus.ASSIGNED


def test_assignment_rejection_reopens_existing_pool() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    order.express_interest("executor-1")
    order.assign("executor-1")

    order.reject_assignment("executor-1")

    assert order.status is WorkOrderStatus.POOL_OPEN
    assert order.assignee_id is None
    assert order.pool_responses["executor-1"].status is PoolResponseStatus.WITHDRAWN


def test_direct_assignment_rejection_returns_to_submitted() -> None:
    order = make_order()
    order.assign("executor-1")

    order.reject_assignment("executor-1")

    assert order.status is WorkOrderStatus.SUBMITTED
    assert order.assignee_id is None


@pytest.mark.parametrize(
    "action",
    [
        lambda order: order.publish_pool(PoolMode.CURATED, actor_id="operator-2"),
        lambda order: order.assign("executor-1", actor_id="operator-2"),
        lambda order: order.cancel("duplicate", actor_id="operator-2"),
    ],
    ids=["publish_pool", "assign", "cancel"],
)
def test_claimed_coordinator_blocks_other_operator_actions(action: object) -> None:
    order = make_order()
    order.claim_coordination("operator-1")

    with pytest.raises(InvalidTransition, match="assigned coordinator"):
        action(order)  # type: ignore[operator]


def test_coordination_claim_is_idempotent_for_same_operator() -> None:
    order = make_order()
    order.pull_events()
    order.claim_coordination("operator-1")
    version = order.version
    order.claim_coordination("operator-1")

    assert order.version == version
    assert len(order.pull_events()) == 1


def test_coordination_cannot_be_stolen() -> None:
    order = make_order()
    order.claim_coordination("operator-1")

    with pytest.raises(InvalidTransition, match="another actor"):
        order.claim_coordination("operator-2")


@pytest.mark.parametrize(
    ("begin_travel", "expected_events"),
    [
        (False, ["work_order.started"]),
        (True, ["work_order.travel_started", "work_order.started"]),
    ],
)
def test_valid_executor_lifecycle(
    begin_travel: bool, expected_events: list[str]
) -> None:
    order = make_order()
    order.assign("executor-1")
    order.accept("executor-1")
    order.pull_events()
    if begin_travel:
        order.start_travel("executor-1")
    order.start_work("executor-1")

    assert order.status is WorkOrderStatus.IN_PROGRESS
    assert [event.name for event in order.pull_events()] == expected_events


@pytest.mark.parametrize("wrong_executor", ["executor-2", "", "operator-1"])
@pytest.mark.parametrize(
    "action",
    [
        lambda order, actor: order.accept(actor),
        lambda order, actor: order.reject_assignment(actor),
    ],
    ids=["accept", "reject"],
)
def test_only_assignee_can_answer_assignment(
    wrong_executor: str, action: object
) -> None:
    order = make_order()
    order.assign("executor-1")

    with pytest.raises(InvalidTransition, match="assigned executor"):
        action(order, wrong_executor)  # type: ignore[operator]


@pytest.mark.parametrize(
    "terminal", [WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED]
)
@pytest.mark.parametrize(
    "action",
    [
        lambda order: order.claim_coordination("operator-1"),
        lambda order: order.cancel("again"),
    ],
    ids=["coordinate", "cancel"],
)
def test_terminal_orders_reject_general_mutations(
    terminal: WorkOrderStatus, action: object
) -> None:
    order = make_order()
    if terminal is WorkOrderStatus.COMPLETED:
        order.assign("executor-1")
        order.accept("executor-1")
        order.start_work("executor-1")
        order.complete("executor-1", CompletionReport())
    else:
        order.cancel("not needed")

    with pytest.raises(InvalidTransition, match="already"):
        action(order)  # type: ignore[operator]


@pytest.mark.parametrize("reason", ["", " ", "\t", "\n"])
def test_cancel_requires_non_blank_reason(reason: str) -> None:
    order = make_order()
    with pytest.raises(ValueError, match="reason"):
        order.cancel(reason)


def test_interested_executor_ids_excludes_non_active_responses() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    order.express_interest("executor-1")
    order.express_interest("executor-2")
    order.withdraw_interest("executor-2")

    assert order.interested_executor_ids() == ("executor-1",)


def test_event_versions_are_strictly_increasing() -> None:
    order = make_order()
    order.publish_pool(PoolMode.CURATED)
    order.express_interest("executor-1")
    order.assign("executor-1")
    order.accept("executor-1")

    events = order.pull_events()
    assert [event.aggregate_version for event in events] == list(range(1, 6))
    assert order.version == 5


def test_pull_events_drains_pending_events() -> None:
    order = make_order()
    assert len(order.pull_events()) == 1
    assert order.pull_events() == ()
