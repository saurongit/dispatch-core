from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe
from typing import Any

import asyncpg

from dispatch_core.messaging.models import (
    CallbackAction,
    InboundEnvelope,
    OutboundButton,
    OutboundEnvelope,
    Provider,
)


@dataclass(frozen=True, slots=True)
class PendingDomainEvent:
    event_id: str
    organization_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    name: str
    payload: dict[str, Any]
    occurred_at: datetime
    attempts: int


def retry_delay(
    attempts: int,
    stable_key: str,
    *,
    base_seconds: float = 1.0,
    maximum_seconds: float = 300.0,
    jitter_ratio: float = 0.2,
) -> float:
    """Deterministic exponential backoff with bounded per-item jitter."""
    if attempts < 1:
        raise ValueError("attempts must be at least one")
    if base_seconds <= 0 or maximum_seconds <= 0:
        raise ValueError("retry delays must be positive")
    if not 0 <= jitter_ratio <= 1:
        raise ValueError("jitter_ratio must be between zero and one")
    exponential = min(base_seconds * (2 ** (attempts - 1)), maximum_seconds)
    digest = sha256(f"{stable_key}:{attempts}".encode()).digest()
    fraction = int.from_bytes(digest[:8], "big") / (2**64 - 1)
    jitter = exponential * jitter_ratio * ((fraction * 2) - 1)
    return max(0.001, min(maximum_seconds, exponential + jitter))


class PostgresInboxStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def accept(
        self,
        *,
        provider: Provider,
        external_event_id: str,
        organization_id: str,
        payload: dict[str, Any],
        consumer_key: str = "",
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                INSERT INTO inbox_events (
                    provider, external_event_id, organization_id, payload,
                    consumer_key
                ) VALUES ($1, $2, $3, $4::jsonb, $5)
                ON CONFLICT (
                    organization_id, provider, consumer_key, external_event_id
                ) DO NOTHING
                """,
                provider.value,
                external_event_id,
                organization_id,
                _json_text(payload),
                consumer_key,
            )
        return result == "INSERT 0 1"

    async def accept_poll_batch(
        self,
        *,
        provider: Provider,
        organization_id: str,
        consumer_key: str,
        cursor_key: str | None = None,
        events: Sequence[tuple[str, dict[str, Any]]],
        next_cursor: str,
    ) -> int:
        """Persist the raw batch before atomically advancing its provider cursor."""
        inserted = 0
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                for external_event_id, payload in events:
                    result = await connection.execute(
                        """
                        INSERT INTO inbox_events (
                            provider, external_event_id, organization_id, payload,
                            consumer_key
                        ) VALUES ($1, $2, $3, $4::jsonb, $5)
                        ON CONFLICT (
                            organization_id, provider, consumer_key,
                            external_event_id
                        ) DO NOTHING
                        """,
                        provider.value,
                        external_event_id,
                        organization_id,
                        _json_text(payload),
                        consumer_key,
                    )
                    inserted += int(result == "INSERT 0 1")
                await connection.execute(
                    """
                    INSERT INTO inbox_cursors (
                        provider, consumer_key, cursor_value, updated_at
                    ) VALUES ($1, $2, $3, now())
                    ON CONFLICT (provider, consumer_key) DO UPDATE SET
                        cursor_value = EXCLUDED.cursor_value,
                        updated_at = EXCLUDED.updated_at
                    """,
                    provider.value,
                    cursor_key or consumer_key,
                    next_cursor,
                )
        return inserted

    async def get_cursor(self, provider: Provider, consumer_key: str) -> str | None:
        async with self._pool.acquire() as connection:
            return await connection.fetchval(
                """
                SELECT cursor_value FROM inbox_cursors
                WHERE provider = $1 AND consumer_key = $2
                """,
                provider.value,
                consumer_key,
            )

    async def claim(
        self,
        *,
        organization_id: str | None = None,
        consumer_key: str = "",
        limit: int = 50,
        stale_after_seconds: int = 120,
    ) -> tuple[InboundEnvelope, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("claim limit must be between 1 and 1000")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    WITH candidates AS (
                        SELECT organization_id, provider, consumer_key,
                               external_event_id
                        FROM inbox_events
                        WHERE consumer_key = $3
                          AND ($4::text IS NULL OR organization_id = $4)
                          AND (
                              (
                                  status = 'pending'
                                  AND next_attempt_at <= now()
                              ) OR (
                                  status = 'processing'
                                  AND claimed_at
                                      < now() - make_interval(secs => $2)
                              )
                          )
                        ORDER BY received_at, provider, external_event_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT $1
                    )
                    UPDATE inbox_events AS item SET
                        status = 'processing',
                        attempts = item.attempts + 1,
                        claimed_at = now(),
                        last_error = NULL
                    FROM candidates
                    WHERE item.organization_id = candidates.organization_id
                      AND item.provider = candidates.provider
                      AND item.consumer_key = candidates.consumer_key
                      AND item.external_event_id = candidates.external_event_id
                    RETURNING item.*
                    """,
                    limit,
                    stale_after_seconds,
                    consumer_key,
                    organization_id,
                )
        return tuple(
            InboundEnvelope(
                provider=Provider(row["provider"]),
                external_event_id=row["external_event_id"],
                organization_id=row["organization_id"],
                payload=_json_value(row["payload"]),
                consumer_key=row["consumer_key"],
                attempts=row["attempts"],
            )
            for row in rows
        )

    async def mark_processed(self, item: InboundEnvelope) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE inbox_events SET
                    status = 'processed', processed_at = now(), claimed_at = NULL
                WHERE provider = $1 AND external_event_id = $2
                  AND organization_id = $3 AND consumer_key = $4
                  AND status = 'processing'
                """,
                item.provider.value,
                item.external_event_id,
                item.organization_id,
                item.consumer_key,
            )

    async def mark_failed(
        self,
        item: InboundEnvelope,
        error: str,
        *,
        max_attempts: int = 10,
    ) -> None:
        is_dead = item.attempts >= max_attempts
        delay = retry_delay(item.attempts, item.external_event_id)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE inbox_events SET
                    status = $3,
                    next_attempt_at = now() + make_interval(secs => $4),
                    claimed_at = NULL,
                    last_error = $5
                WHERE provider = $1 AND external_event_id = $2
                  AND organization_id = $6 AND consumer_key = $7
                  AND status = 'processing'
                """,
                item.provider.value,
                item.external_event_id,
                "dead" if is_dead else "pending",
                delay,
                error[:2000],
                item.organization_id,
                item.consumer_key,
            )


class PostgresOutboxStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def claim_events(
        self,
        *,
        limit: int = 50,
        stale_after_seconds: int = 120,
    ) -> tuple[PendingDomainEvent, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("claim limit must be between 1 and 1000")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    WITH candidates AS (
                        SELECT event_id
                        FROM outbox_events
                        WHERE (
                            status = 'pending' AND next_attempt_at <= now()
                        ) OR (
                            status = 'processing'
                            AND claimed_at < now() - make_interval(secs => $2)
                        )
                        ORDER BY occurred_at, event_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT $1
                    )
                    UPDATE outbox_events AS item SET
                        status = 'processing',
                        attempts = item.attempts + 1,
                        claimed_at = now(),
                        last_error = NULL
                    FROM candidates
                    WHERE item.event_id = candidates.event_id
                    RETURNING item.*
                    """,
                    limit,
                    stale_after_seconds,
                )
        return tuple(
            PendingDomainEvent(
                event_id=row["event_id"],
                organization_id=row["organization_id"],
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                aggregate_version=row["aggregate_version"],
                name=row["event_name"],
                payload=_json_value(row["payload"]),
                occurred_at=row["occurred_at"],
                attempts=row["attempts"],
            )
            for row in rows
        )

    async def mark_projected(self, event_id: str) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE outbox_events SET
                    status = 'delivered', delivered_at = now(), claimed_at = NULL
                WHERE event_id = $1 AND status = 'processing'
                """,
                event_id,
            )

    async def mark_failed(
        self,
        event: PendingDomainEvent,
        error: str,
        *,
        max_attempts: int = 10,
    ) -> None:
        is_dead = event.attempts >= max_attempts
        delay = retry_delay(event.attempts, event.event_id)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE outbox_events SET
                    status = $2,
                    next_attempt_at = now() + make_interval(secs => $3),
                    claimed_at = NULL,
                    last_error = $4
                WHERE event_id = $1 AND status = 'processing'
                """,
                event.event_id,
                "dead" if is_dead else "pending",
                delay,
                error[:2000],
            )


class PostgresOutboundStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def enqueue(
        self,
        *,
        deduplication_key: str,
        organization_id: str,
        provider: Provider,
        recipient_id: str,
        text: str,
        buttons: Sequence[OutboundButton] = (),
        consumer_key: str = "",
    ) -> bool:
        encoded_buttons = [_button_to_dict(button) for button in buttons]
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                INSERT INTO outbound_messages (
                    deduplication_key, organization_id, provider,
                    recipient_id, text_body, buttons, consumer_key
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (deduplication_key) DO NOTHING
                """,
                deduplication_key,
                organization_id,
                provider.value,
                recipient_id,
                text,
                _json_text(encoded_buttons),
                consumer_key,
            )
        return result == "INSERT 0 1"

    async def claim(
        self,
        provider: Provider,
        *,
        consumer_key: str = "",
        consumer_keys: Sequence[str] | None = None,
        limit: int = 50,
        stale_after_seconds: int = 120,
    ) -> tuple[OutboundEnvelope, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("claim limit must be between 1 and 1000")
        routed_consumer_keys = tuple(dict.fromkeys(consumer_keys or (consumer_key,)))
        if not routed_consumer_keys:
            raise ValueError("at least one outbound consumer key is required")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    WITH candidates AS (
                        SELECT id
                        FROM outbound_messages
                        WHERE provider = $1
                          AND consumer_key = ANY($4::text[]) AND (
                            (status = 'pending' AND next_attempt_at <= now())
                            OR (
                                status = 'processing'
                                AND claimed_at < now() - make_interval(secs => $3)
                            )
                        )
                        ORDER BY created_at, id
                        FOR UPDATE SKIP LOCKED
                        LIMIT $2
                    )
                    UPDATE outbound_messages AS item SET
                        status = 'processing',
                        attempts = item.attempts + 1,
                        claimed_at = now(),
                        last_error = NULL
                    FROM candidates
                    WHERE item.id = candidates.id
                    RETURNING item.*
                    """,
                    provider.value,
                    limit,
                    stale_after_seconds,
                    list(routed_consumer_keys),
                )
        return tuple(_outbound_from_row(row) for row in rows)

    async def mark_delivered(
        self, message: OutboundEnvelope, external_message_id: str | None
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE outbound_messages SET
                    status = 'delivered', delivered_at = now(), claimed_at = NULL,
                    external_message_id = $2
                WHERE id = $1 AND status = 'processing'
                """,
                message.message_id,
                external_message_id,
            )

    async def mark_failed(
        self,
        message: OutboundEnvelope,
        error: str,
        *,
        max_attempts: int = 10,
    ) -> None:
        is_dead = message.attempts >= max_attempts
        delay = retry_delay(message.attempts, message.deduplication_key)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE outbound_messages SET
                    status = $2,
                    next_attempt_at = now() + make_interval(secs => $3),
                    claimed_at = NULL,
                    last_error = $4
                WHERE id = $1 AND status = 'processing'
                """,
                message.message_id,
                "dead" if is_dead else "pending",
                delay,
                error[:2000],
            )


class PostgresCallbackStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(
        self,
        *,
        organization_id: str,
        action: str,
        payload: dict[str, Any],
        allowed_role: str | None,
        lifetime: timedelta = timedelta(days=7),
    ) -> CallbackAction:
        now = datetime.now(UTC)
        callback = CallbackAction.create(
            token=token_urlsafe(18),
            organization_id=organization_id,
            action=action,
            payload=payload,
            allowed_role=allowed_role,
            expires_at=now + lifetime,
        )
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO callback_actions (
                    token, organization_id, action, payload,
                    allowed_role, expires_at
                ) VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                """,
                callback.token,
                callback.organization_id,
                callback.action,
                _json_text(dict(callback.payload)),
                callback.allowed_role,
                callback.expires_at,
            )
        return callback

    async def resolve(
        self,
        *,
        token: str,
        organization_id: str,
        actor_role: str,
        claim_key: str | None = None,
    ) -> CallbackAction | None:
        stable_claim_key = claim_key or token_urlsafe(18)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE callback_actions SET
                    revoked_at = COALESCE(revoked_at, now()),
                    claim_key = COALESCE(claim_key, $4)
                WHERE token = $1
                  AND organization_id = $2
                  AND expires_at > now()
                  AND (
                      revoked_at IS NULL
                      OR (claim_key = $4 AND completed_at IS NULL)
                  )
                  AND (
                      allowed_role IS NULL
                      OR allowed_role = $3
                      OR $3 = 'admin'
                  )
                RETURNING *
                """,
                token,
                organization_id,
                actor_role,
                stable_claim_key,
            )
        if row is None:
            return None
        return CallbackAction.create(
            token=row["token"],
            organization_id=row["organization_id"],
            action=row["action"],
            payload=_json_value(row["payload"]),
            allowed_role=row["allowed_role"],
            expires_at=row["expires_at"],
        )

    async def complete(
        self,
        *,
        token: str,
        organization_id: str,
        claim_key: str,
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE callback_actions SET completed_at = now()
                WHERE token = $1 AND organization_id = $2
                  AND claim_key = $3 AND completed_at IS NULL
                """,
                token,
                organization_id,
                claim_key,
            )
        return result == "UPDATE 1"

    async def peek_action(
        self,
        *,
        token: str,
        organization_id: str,
        claim_key: str | None = None,
    ) -> str | None:
        """Identify a callback route without authorizing or consuming it."""
        async with self._pool.acquire() as connection:
            action = await connection.fetchval(
                """
                SELECT action FROM callback_actions
                WHERE token = $1 AND organization_id = $2
                  AND (
                      revoked_at IS NULL
                      OR (claim_key = $3 AND completed_at IS NULL)
                  )
                  AND expires_at > now()
                """,
                token,
                organization_id,
                claim_key,
            )
        return str(action) if action is not None else None


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _button_to_dict(button: OutboundButton) -> dict[str, Any]:
    return {
        "text": button.text,
        "callback_token": button.callback_token,
        "url": button.url,
        "request_location": button.request_location,
        "row": button.row,
    }


def _outbound_from_row(row: asyncpg.Record) -> OutboundEnvelope:
    buttons_data = _json_value(row["buttons"])
    return OutboundEnvelope(
        message_id=row["id"],
        deduplication_key=row["deduplication_key"],
        organization_id=row["organization_id"],
        provider=Provider(row["provider"]),
        recipient_id=row["recipient_id"],
        text=row["text_body"],
        buttons=tuple(OutboundButton(**item) for item in buttons_data),
        attempts=row["attempts"],
        consumer_key=row.get("consumer_key") or "",
    )
