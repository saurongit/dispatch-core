from __future__ import annotations

from dispatch_core.infrastructure.messaging import PostgresInboxStore
from dispatch_core.messaging.models import Provider

from .max import MaxTransport
from .telegram import TelegramTransport


class DurablePollingReceiver:
    """Accepts raw updates durably before acknowledging a provider cursor."""

    def __init__(self, inbox: PostgresInboxStore) -> None:
        self._inbox = inbox

    async def telegram_once(
        self,
        transport: TelegramTransport,
        *,
        organization_id: str,
        consumer_key: str,
        timeout_seconds: int = 30,
    ) -> int:
        stored_cursor = await self._inbox.get_cursor(
            Provider.TELEGRAM, consumer_key
        )
        offset = int(stored_cursor) if stored_cursor is not None else None
        updates, next_offset = await transport.get_updates(
            offset=offset,
            timeout_seconds=timeout_seconds,
        )
        if next_offset is None:
            return 0
        events = [
            (transport.external_event_id(update), update) for update in updates
        ]
        return await self._inbox.accept_poll_batch(
            provider=Provider.TELEGRAM,
            organization_id=organization_id,
            consumer_key=consumer_key,
            events=events,
            next_cursor=str(next_offset),
        )

    async def max_once(
        self,
        transport: MaxTransport,
        *,
        organization_id: str,
        consumer_key: str,
        timeout_seconds: int = 30,
    ) -> int:
        stored_cursor = await self._inbox.get_cursor(Provider.MAX, consumer_key)
        marker = int(stored_cursor) if stored_cursor is not None else None
        updates, next_marker = await transport.get_updates(
            marker=marker,
            timeout_seconds=timeout_seconds,
        )
        if next_marker is None:
            return 0
        events = [
            (transport.external_event_id(update), update) for update in updates
        ]
        return await self._inbox.accept_poll_batch(
            provider=Provider.MAX,
            organization_id=organization_id,
            consumer_key=consumer_key,
            events=events,
            next_cursor=str(next_marker),
        )
