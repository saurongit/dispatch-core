from __future__ import annotations

import asyncio

import pytest

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.domain.order_numbers import format_order_number
from dispatch_core.infrastructure.async_memory import AsyncMemoryUnitOfWorkFactory


@pytest.mark.parametrize(
    ("sequence", "expected"),
    [
        (1, "A000"),
        (1_000, "A999"),
        (1_001, "B000"),
        (26_000, "Z999"),
        (26_001, "X000K29"),
    ],
)
def test_public_number_format_preserves_core_dr_short_numbers(
    sequence: int,
    expected: str,
) -> None:
    assert format_order_number(sequence) == expected


@pytest.mark.parametrize("sequence", [0, -1, -10])
def test_public_number_rejects_non_positive_sequences(sequence: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        format_order_number(sequence)


@pytest.mark.asyncio
async def test_concurrent_order_creation_allocates_unique_public_numbers() -> None:
    factory = AsyncMemoryUnitOfWorkFactory()
    service = AsyncDispatchService(factory)

    orders = await asyncio.gather(
        *(
            service.create_order(
                order_id=f"order-{index}",
                organization_id="org-1",
                work_type="repair",
                source="test",
                details={"index": index},
            )
            for index in range(500)
        )
    )

    numbers = [order.public_number for order in orders]
    assert len(numbers) == len(set(numbers)) == 500
    assert {"A000", "A499"}.issubset(numbers)


@pytest.mark.asyncio
async def test_idempotent_retry_returns_the_original_public_number() -> None:
    service = AsyncDispatchService(AsyncMemoryUnitOfWorkFactory())
    values = {
        "order_id": "submission-1",
        "organization_id": "org-1",
        "work_type": "repair",
        "source": "telegram:7001",
        "details": {"phone": "+79990000000"},
    }

    first, retried = await asyncio.gather(
        service.create_order_once(**values),
        service.create_order_once(**values),
    )

    assert first.public_number == retried.public_number


@pytest.mark.asyncio
async def test_public_number_sequence_is_scoped_to_an_organization() -> None:
    service = AsyncDispatchService(AsyncMemoryUnitOfWorkFactory())
    first, second = await asyncio.gather(
        service.create_order(
            order_id="one",
            organization_id="org-1",
            work_type="repair",
            source="test",
            details={},
        ),
        service.create_order(
            order_id="two",
            organization_id="org-2",
            work_type="repair",
            source="test",
            details={},
        ),
    )

    assert first.public_number == second.public_number == "A000"
