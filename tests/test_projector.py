from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from dispatch_core.infrastructure.messaging import PendingDomainEvent
from dispatch_core.messaging.projector import PostgresNotificationProjector


def travel_event() -> PendingDomainEvent:
    return PendingDomainEvent(
        event_id="event-1",
        organization_id="org-1",
        aggregate_type="work_order",
        aggregate_id="order-1",
        aggregate_version=7,
        name="work_order.travel_started",
        payload={"executor_id": "master-1"},
        occurred_at=datetime(2026, 7, 22, 9, 0, tzinfo=UTC),
        attempts=1,
    )


def lifecycle_event(name: str) -> PendingDomainEvent:
    return PendingDomainEvent(
        event_id=f"event-{name}",
        organization_id="org-1",
        aggregate_type="work_order",
        aggregate_id="order-1",
        aggregate_version=8,
        name=name,
        payload={},
        occurred_at=datetime(2026, 7, 22, 10, 0, tzinfo=UTC),
        attempts=1,
    )


def test_travel_projection_requests_location_and_sends_fragment_link() -> None:
    projector = PostgresNotificationProjector(
        cast(Any, None),
        public_base_url="https://dispatch.example",
    )
    order = {
        "assignee_id": "master-1",
        "coordinator_id": "operator-1",
        "requester_id": "client-1",
    }

    plans = projector._plans(
        travel_event(),
        cast(Any, order),
        "Заявка: ремонт",
        public_token="t" * 43,
        location_token="l" * 43,
    )

    location_plan = next(plan for plan in plans if plan.purpose == "travel:location")
    client_plan = next(plan for plan in plans if plan.purpose == "travel:client")
    browser_plan = next(
        plan for plan in plans if plan.purpose == "travel:browser_location"
    )
    assert location_plan.actor_ids == ("master-1",)
    assert location_plan.buttons[0].request_location
    assert client_plan.actor_ids == ("client-1",)
    assert client_plan.buttons[0].url == (
        "https://dispatch.example/track#" + "t" * 43
    )
    assert "?" not in client_plan.buttons[0].url
    assert browser_plan.actor_ids == ("master-1",)
    assert browser_plan.buttons[0].url == (
        "https://dispatch.example/track/share#" + "l" * 43
    )


def test_travel_projection_omits_client_link_without_public_base_url() -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": "master-1",
        "coordinator_id": None,
        "requester_id": "client-1",
    }

    plans = projector._plans(
        travel_event(),
        cast(Any, order),
        "Заявка: ремонт",
        public_token="t" * 43,
        location_token="l" * 43,
    )

    assert any(plan.purpose == "travel:location" for plan in plans)
    assert not any(plan.purpose == "travel:client" for plan in plans)
    assert not any(plan.purpose == "travel:browser_location" for plan in plans)


def test_completed_projection_notifies_coordinator_and_requester() -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": "master-1",
        "coordinator_id": "operator-1",
        "requester_id": "client-1",
    }

    plans = projector._plans(
        lifecycle_event("work_order.completed"),
        cast(Any, order),
        "Заявка: ремонт",
    )

    assert [(plan.purpose, plan.actor_ids, plan.delivery_role) for plan in plans] == [
        ("completed:operator", ("operator-1",), "operator"),
        ("completed:client", ("client-1",), "client"),
    ]


def test_completed_projection_falls_back_to_operator_role() -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": "master-1",
        "coordinator_id": None,
        "requester_id": None,
    }

    plans = projector._plans(
        lifecycle_event("work_order.completed"),
        cast(Any, order),
        "Заявка: ремонт",
    )

    assert len(plans) == 1
    assert plans[0].roles == ("operator",)
    assert plans[0].purpose == "completed:operator"


@pytest.mark.parametrize(
    ("coordinator", "expected_actor_ids", "expected_roles"),
    [
        ("operator-1", ("operator-1",), ()),
        (None, (), ("operator",)),
    ],
)
def test_assignment_rejected_projection_returns_to_responsible_operator(
    coordinator: str | None,
    expected_actor_ids: tuple[str, ...],
    expected_roles: tuple[str, ...],
) -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": None,
        "coordinator_id": coordinator,
        "requester_id": "client-1",
    }

    plans = projector._plans(
        lifecycle_event("work_order.assignment_rejected"),
        cast(Any, order),
        "Заявка: ремонт",
    )

    assert len(plans) == 1
    assert plans[0].actor_ids == expected_actor_ids
    assert plans[0].roles == expected_roles
    assert plans[0].purpose == "rejected"


def test_cancelled_projection_notifies_assignee_and_requester() -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": "master-1",
        "coordinator_id": "operator-1",
        "requester_id": "client-1",
    }

    plans = projector._plans(
        lifecycle_event("work_order.cancelled"),
        cast(Any, order),
        "Заявка: ремонт",
    )

    assert [(plan.purpose, plan.actor_ids, plan.delivery_role) for plan in plans] == [
        ("cancelled:master", ("master-1",), "master"),
        ("cancelled:client", ("client-1",), "client"),
    ]


def test_unknown_or_non_order_event_produces_no_notifications() -> None:
    projector = PostgresNotificationProjector(cast(Any, None))
    order = {
        "assignee_id": None,
        "coordinator_id": None,
        "requester_id": None,
    }
    unknown = lifecycle_event("work_order.unknown")
    foreign = PendingDomainEvent(
        event_id="foreign-event",
        organization_id="org-1",
        aggregate_type="tracking",
        aggregate_id="tracking-1",
        aggregate_version=1,
        name="tracking.started",
        payload={},
        occurred_at=datetime(2026, 7, 22, 10, 0, tzinfo=UTC),
        attempts=1,
    )

    assert projector._plans(unknown, cast(Any, order), "card") == ()
    assert projector._plans(foreign, cast(Any, order), "card") == ()
    assert projector._plans(unknown, None, "card") == ()
