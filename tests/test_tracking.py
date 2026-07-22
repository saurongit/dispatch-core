from __future__ import annotations

from datetime import UTC, datetime
from math import inf, nan

import pytest

from dispatch_core.domain.errors import InvalidTransition
from dispatch_core.domain.tracking import (
    LocationSource,
    TrackingPoint,
    TrackingSession,
    TrackingStatus,
)


def start_session(
    *,
    public_token: str | None = None,
    location_token: str | None = None,
) -> TrackingSession:
    return TrackingSession.start(
        session_id="tracking-1",
        organization_id="org-1",
        work_order_id="order-1",
        executor_id="master-1",
        public_token=public_token,
        location_token=location_token,
        now=datetime(2026, 7, 22, 8, 0, tzinfo=UTC),
    )


def test_start_mints_high_entropy_public_token() -> None:
    first = start_session()
    second = TrackingSession.start(
        session_id="tracking-2",
        organization_id="org-1",
        work_order_id="order-2",
        executor_id="master-1",
    )

    assert first.public_token is not None
    assert len(first.public_token) >= 43
    assert second.public_token is not None
    assert first.public_token != second.public_token
    assert first.location_token is not None
    assert len(first.location_token) >= 43
    assert second.location_token is not None
    assert first.location_token != second.location_token
    assert first.public_token != first.location_token
    assert first.pull_events()[0].payload == {
        "browser_location": True,
        "public_tracking": True,
    }


@pytest.mark.parametrize("token", ["", "short", " " * 43])
def test_start_rejects_weak_or_blank_public_token(token: str) -> None:
    with pytest.raises(ValueError, match="public tracking token"):
        start_session(public_token=token)


@pytest.mark.parametrize("token", ["", "short", " " * 43])
def test_start_rejects_weak_or_blank_location_token(token: str) -> None:
    with pytest.raises(ValueError, match="location submission token"):
        start_session(location_token=token)


@pytest.mark.parametrize(
    ("terminal_action", "expected_status", "expected_event"),
    [
        (
            lambda session: session.complete(),
            TrackingStatus.COMPLETED,
            "tracking.completed",
        ),
        (
            lambda session: session.cancel("client cancelled"),
            TrackingStatus.CANCELLED,
            "tracking.cancelled",
        ),
    ],
)
def test_terminal_transition_revokes_public_access(
    terminal_action,
    expected_status: TrackingStatus,
    expected_event: str,
) -> None:
    session = start_session(public_token="a" * 43)
    session.pull_events()

    terminal_action(session)

    assert session.status is expected_status
    assert session.public_token is None
    assert session.location_token is None
    assert session.pull_events()[0].name == expected_event


def test_terminal_session_rejects_new_points() -> None:
    session = start_session()
    session.cancel("cancelled")

    with pytest.raises(InvalidTransition, match="not active"):
        session.add_point(
            TrackingPoint(
                latitude=53.75,
                longitude=87.1,
                captured_at=datetime.now(UTC),
                ingested_at=datetime.now(UTC),
                source=LocationSource.TELEGRAM,
            )
        )


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"latitude": nan}, "latitude"),
        ({"latitude": inf}, "latitude"),
        ({"latitude": 91}, "latitude"),
        ({"longitude": -181}, "longitude"),
        ({"longitude": inf}, "longitude"),
        ({"accuracy_m": -1}, "accuracy"),
        ({"accuracy_m": inf}, "accuracy"),
        ({"source_event_id": "  "}, "source_event_id"),
    ],
)
def test_tracking_point_rejects_invalid_measurements(
    values: dict[str, object], message: str
) -> None:
    data: dict[str, object] = {
        "latitude": 53.75,
        "longitude": 87.1,
        "captured_at": datetime.now(UTC),
        "ingested_at": datetime.now(UTC),
        "source": LocationSource.MOBILE,
    }
    data.update(values)
    with pytest.raises(ValueError, match=message):
        TrackingPoint(**data)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field",
    ["session_id", "organization_id", "work_order_id", "executor_id"],
)
def test_start_requires_every_identifier(field: str) -> None:
    values = {
        "session_id": "tracking-1",
        "organization_id": "org-1",
        "work_order_id": "order-1",
        "executor_id": "master-1",
    }
    values[field] = ""
    with pytest.raises(ValueError, match="identifiers"):
        TrackingSession.start(**values)


def test_capability_tokens_must_be_distinct() -> None:
    with pytest.raises(ValueError, match="distinct"):
        start_session(public_token="a" * 43, location_token="a" * 43)

    with pytest.raises(ValueError, match="distinct"):
        TrackingSession(
            id="tracking-1",
            organization_id="org-1",
            work_order_id="order-1",
            executor_id="master-1",
            public_token="a" * 43,
            location_token="a" * 43,
        )


def test_rehydrated_session_validates_each_capability() -> None:
    common = {
        "id": "tracking-1",
        "organization_id": "org-1",
        "work_order_id": "order-1",
        "executor_id": "master-1",
    }
    with pytest.raises(ValueError, match="public tracking token"):
        TrackingSession(**common, public_token="weak")
    with pytest.raises(ValueError, match="location submission token"):
        TrackingSession(**common, location_token="weak")


def test_duplicate_source_event_is_ignored_without_new_domain_event() -> None:
    session = start_session()
    session.pull_events()
    instant = datetime.now(UTC)
    first = TrackingPoint(
        latitude=53.75,
        longitude=87.1,
        captured_at=instant,
        ingested_at=instant,
        source=LocationSource.MAX,
        source_event_id="max:1",
    )
    session.add_point(first)
    session.pull_events()

    session.add_point(first)

    assert session.latest_point() == first
    assert session.pull_events() == ()


def test_domain_rejects_out_of_order_point_without_service_normalization() -> None:
    session = start_session()
    instant = datetime.now(UTC)
    session.add_point(
        TrackingPoint(
            latitude=53.75,
            longitude=87.1,
            captured_at=instant,
            ingested_at=instant,
            source=LocationSource.TELEGRAM,
        )
    )

    with pytest.raises(ValueError, match="ordered"):
        session.add_point(
            TrackingPoint(
                latitude=53.76,
                longitude=87.11,
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
                ingested_at=instant,
                source=LocationSource.WEB,
            )
        )


@pytest.mark.parametrize("action", ["complete", "cancel"])
def test_terminal_transition_cannot_be_repeated(action: str) -> None:
    session = start_session()
    getattr(session, action)()
    with pytest.raises(InvalidTransition, match="not active"):
        getattr(session, action)()
