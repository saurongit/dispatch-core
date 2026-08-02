from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from dispatch_core.api.schemas import BrowserLocationInput
from dispatch_core.api.tracking_page import render_location_share_page
from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.domain.tracking import LocationSource
from dispatch_core.infrastructure.async_memory import AsyncMemoryUnitOfWorkFactory


def test_browser_location_rejects_future_device_timestamp() -> None:
    with pytest.raises(ValidationError, match="too far in the future"):
        BrowserLocationInput(
            latitude=53.75,
            longitude=87.1,
            captured_at=datetime.now(UTC) + timedelta(hours=1),
            event_id="future-fix-1",
        )


def test_location_page_retries_transient_server_failures() -> None:
    page = render_location_share_page("nonce")

    assert "[401, 403, 404, 409, 410].includes(response.status)" in page
    assert "Повторяем автоматически" in page
    assert "lastAttemptAt = now" in page


@pytest.mark.asyncio
async def test_future_existing_point_does_not_block_following_location() -> None:
    factory = AsyncMemoryUnitOfWorkFactory()
    service = AsyncDispatchService(factory)
    order = await service.create_order(
        organization_id="org-1",
        order_id="order-1",
        work_type="repair",
        source="test",
        details={},
    )
    await service.assign_order("org-1", order.id, "master-1")
    await service.accept_order("org-1", order.id, "master-1")
    _, session = await service.start_travel(
        "org-1",
        order.id,
        "master-1",
        session_id="tracking-1",
    )
    future = datetime.now(UTC) + timedelta(days=1)
    await service.record_location(
        organization_id="org-1",
        executor_id="master-1",
        session_id=session.id,
        latitude=53.75,
        longitude=87.1,
        source=LocationSource.IMPORT,
        captured_at=future,
        source_event_id="poisoned-point",
    )

    updated = await service.record_location(
        organization_id="org-1",
        executor_id="master-1",
        session_id=session.id,
        latitude=53.76,
        longitude=87.11,
        source=LocationSource.WEB,
        captured_at=datetime.now(UTC),
        source_event_id="current-point",
    )

    assert len(updated.points) == 2
    assert updated.latest_point() is not None
    assert updated.latest_point().captured_at == future
