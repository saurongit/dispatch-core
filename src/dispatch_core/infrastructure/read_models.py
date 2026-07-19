from __future__ import annotations

from copy import deepcopy
from typing import Protocol

import asyncpg

from dispatch_core.domain.errors import NotFound
from dispatch_core.domain.work_order import WorkOrder

from .async_memory import AsyncMemoryStore
from .postgres import PostgresOrderRepository


class OrderReader(Protocol):
    async def get(self, organization_id: str, order_id: str) -> WorkOrder: ...


class AsyncMemoryOrderReader:
    def __init__(self, store: AsyncMemoryStore) -> None:
        self._store = store

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        order = self._store.orders.get((organization_id, order_id))
        if order is None:
            raise NotFound(f"work order {order_id!r} was not found")
        return deepcopy(order)


class PostgresOrderReader:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        async with self._pool.acquire() as connection:
            return await PostgresOrderRepository(connection).get(
                organization_id, order_id
            )
