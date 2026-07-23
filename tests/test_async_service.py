from __future__ import annotations

import asyncio

import pytest

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.domain.errors import ConcurrencyConflict
from dispatch_core.infrastructure.async_memory import AsyncMemoryUnitOfWorkFactory


def submission() -> dict[str, object]:
    return {
        "organization_id": "org-1",
        "order_id": "intake-submission-1",
        "work_type": "repair",
        "source": "telegram:7001",
        "details": {
            "phone": "+7 999 000-00-01",
            "address": "Ленина 1",
            "service_keys": ["repair"],
        },
        "requester_id": "telegram:7001",
    }


@pytest.mark.asyncio
async def test_create_order_once_converges_concurrent_identical_submissions() -> None:
    factory = AsyncMemoryUnitOfWorkFactory()
    service = AsyncDispatchService(factory)

    first, second = await asyncio.gather(
        service.create_order_once(**submission()),
        service.create_order_once(**submission()),
    )

    assert first.id == second.id == "intake-submission-1"
    assert len(factory.store.orders) == 1
    assert [event.name for event in factory.store.outbox_events] == [
        "work_order.submitted"
    ]


@pytest.mark.asyncio
async def test_create_order_once_rejects_same_id_with_different_intent() -> None:
    service = AsyncDispatchService(AsyncMemoryUnitOfWorkFactory())
    await service.create_order_once(**submission())
    changed = submission()
    changed["details"] = {
        "phone": "+7 999 000-00-01",
        "address": "ДРУГОЙ АДРЕС",
        "service_keys": ["repair"],
    }

    with pytest.raises(ConcurrencyConflict, match="different submission"):
        await service.create_order_once(**changed)
