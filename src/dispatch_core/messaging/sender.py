from __future__ import annotations

import logging

from dispatch_core.infrastructure.messaging import PostgresOutboundStore
from dispatch_core.messaging.models import Provider
from dispatch_core.transports.contracts import Transport

logger = logging.getLogger(__name__)


class OutboundSender:
    """Claims durable messages, performs network I/O, then records the result."""

    def __init__(
        self,
        store: PostgresOutboundStore,
        transports: dict[Provider, Transport],
    ) -> None:
        self._store = store
        self._transports = transports

    async def run_once(self, provider: Provider, *, limit: int = 50) -> int:
        transport = self._transports.get(provider)
        if transport is None:
            return 0
        messages = await self._store.claim(provider, limit=limit)
        delivered = 0
        for message in messages:
            try:
                result = await transport.send(message)
            except Exception as exc:
                logger.exception(
                    "send failed for %s:%s to %s",
                    provider.value,
                    message.deduplication_key,
                    message.recipient_id,
                )
                await self._store.mark_failed(
                    message,
                    f"{type(exc).__name__}: {exc}",
                )
                continue
            await self._store.mark_delivered(
                message,
                result.external_message_id,
            )
            delivered += 1
        return delivered
