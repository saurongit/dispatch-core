from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any

import httpx

from dispatch_core.messaging.models import OutboundEnvelope, Provider

from .common import decode_callback, encode_callback, group_buttons, stable_payload_id
from .contracts import EventKind, InboundEvent, SendResult

logger = logging.getLogger(__name__)

_MAX_ATTACHMENT_READY_RETRIES = 4
_MAX_ATTACHMENT_READY_DELAYS = (0, 1, 2, 4)
_MAX_TEXT_LENGTH_LIMIT = 4000
_MAX_CHAT_REQUESTS_PER_SECOND = 2


class MaxTransport:
    provider = Provider.MAX

    def __init__(
        self,
        token: str,
        *,
        signing_secret: str = "",
        api_base: str = "https://platform-api2.max.ru",
        client: httpx.AsyncClient | None = None,
        proxy: str | None = None,
        requests_per_second: int = 25,
    ) -> None:
        if not token:
            raise ValueError("MAX bot token is required")
        if not 1 <= requests_per_second <= 30:
            raise ValueError("MAX request rate must be between 1 and 30")
        self._token = token
        self._signing_secret = signing_secret
        self._api_base = api_base.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            proxy=proxy,
            headers={"Authorization": token},
            timeout=httpx.Timeout(40),
        )
        self._rate = requests_per_second
        self._rate_lock = asyncio.Lock()
        self._request_times: deque[float] = deque()
        self._chat_request_times: defaultdict[str, deque[float]] = defaultdict(deque)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def external_event_id(self, payload: dict[str, Any]) -> str:
        for candidate in self._identity_candidates(payload):
            if candidate:
                return f"max:{candidate}"
        return stable_payload_id(self.provider.value, payload)

    def parse(self, payload: dict[str, Any]) -> tuple[InboundEvent, ...]:
        updates = payload.get("updates")
        if isinstance(updates, list):
            events: list[InboundEvent] = []
            for item in updates:
                if isinstance(item, dict):
                    events.extend(self._parse_one(item))
            return tuple(events)
        return self._parse_one(payload)

    def _parse_one(self, item: dict[str, Any]) -> tuple[InboundEvent, ...]:
        update_type = item.get("update_type") or item.get("type") or "message"
        external_event_id = self.external_event_id(item)
        if update_type == "message_callback":
            callback = item.get("callback")
            callback = callback if isinstance(callback, dict) else {}
            user_id = self._user_id(callback) or self._user_id(item)
            token = decode_callback(
                callback.get("payload") or callback.get("data"),
                signing_secret=self._signing_secret,
            )
            if not user_id or not token:
                return ()
            return (
                InboundEvent(
                    provider=self.provider,
                    external_event_id=external_event_id,
                    external_user_id=user_id,
                    chat_id=user_id,
                    kind=EventKind.CALLBACK,
                    callback_token=token,
                    callback_id=str(callback.get("callback_id") or "") or None,
                    raw=item,
                ),
            )
        if update_type == "bot_started":
            user_id = self._user_id(item)
            if not user_id:
                return ()
            return (
                self._event(
                    external_event_id,
                    user_id,
                    EventKind.START,
                    item,
                    text=self._text(item) or "/start",
                ),
            )
        if update_type not in {"message_created", "message"}:
            return ()
        message = item.get("message")
        message = message if isinstance(message, dict) else item
        user_id = self._user_id(message) or self._user_id(item)
        if not user_id:
            return ()
        text = self._text(message)
        body = message.get("body")
        body = body if isinstance(body, dict) else {}
        attachments = body.get("attachments")
        attachments = attachments if isinstance(attachments, list) else []
        events: list[InboundEvent] = []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            attachment_type = attachment.get("type")
            attachment_payload = attachment.get("payload")
            attachment_payload = (
                attachment_payload if isinstance(attachment_payload, dict) else {}
            )
            if attachment_type == "image" and attachment_payload.get("url"):
                events.append(
                    self._event(
                        external_event_id,
                        user_id,
                        EventKind.PHOTO,
                        item,
                        text=text,
                        media_id=str(attachment_payload["url"]),
                    )
                )
            elif attachment_type == "location":
                try:
                    latitude = float(attachment["latitude"])
                    longitude = float(attachment["longitude"])
                except (KeyError, TypeError, ValueError):
                    continue
                events.append(
                    self._event(
                        external_event_id,
                        user_id,
                        EventKind.LOCATION,
                        item,
                        text=text,
                        latitude=latitude,
                        longitude=longitude,
                    )
                )
        if events:
            return tuple(events)
        return (
            self._event(
                external_event_id,
                user_id,
                EventKind.MESSAGE,
                item,
                text=text,
            ),
        )

    async def get_updates(
        self,
        *,
        marker: int | None,
        timeout_seconds: int = 30,
        limit: int = 100,
    ) -> tuple[tuple[dict[str, Any], ...], int | None]:
        parameters: dict[str, Any] = {
            "timeout": timeout_seconds,
            "limit": limit,
        }
        if marker is not None:
            parameters["marker"] = marker
        response = await self._request(
            "GET",
            "/updates",
            params=parameters,
            request_timeout=timeout_seconds + 15,
        )
        data = self._response_object(response, operation="updates")
        updates = data.get("updates") or []
        if not isinstance(updates, list):
            raise RuntimeError("MAX updates field is not a list")
        items = tuple(item for item in updates if isinstance(item, dict))
        next_marker = data.get("marker")
        if next_marker is not None:
            try:
                next_marker = int(next_marker)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("MAX marker is not an integer") from exc
        return items, next_marker if next_marker is not None else marker

    async def send(self, message: OutboundEnvelope) -> SendResult:
        text = message.text
        if len(text) > _MAX_TEXT_LENGTH_LIMIT:
            logger.warning(
                "truncating MAX message from %d to %d chars",
                len(text),
                _MAX_TEXT_LENGTH_LIMIT,
            )
            text = text[:_MAX_TEXT_LENGTH_LIMIT]
        body: dict[str, Any] = {"text": text}
        if message.buttons:
            body["attachments"] = [
                {
                    "type": "inline_keyboard",
                    "payload": {
                        "buttons": [
                            [self._button(button) for button in row]
                            for row in group_buttons(message.buttons)
                        ]
                    },
                }
            ]
        for attempt in range(_MAX_ATTACHMENT_READY_RETRIES):
            response = await self._request(
                "POST",
                "/messages",
                params={"user_id": message.recipient_id},
                json=body,
                rate_limit_key=message.recipient_id,
            )
            data = self._response_object(
                response,
                operation="send message",
                allowed_error_codes=frozenset({"attachment.not.ready"}),
            )
            if data.get("code") == "attachment.not.ready":
                if attempt < _MAX_ATTACHMENT_READY_RETRIES - 1:
                    delay = _MAX_ATTACHMENT_READY_DELAYS[
                        min(attempt, len(_MAX_ATTACHMENT_READY_DELAYS) - 1)
                    ]
                    logger.info(
                        "MAX attachment not ready, retry %d/%d in %ds",
                        attempt + 1,
                        _MAX_ATTACHMENT_READY_RETRIES,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
            break
        if data.get("code"):
            raise RuntimeError(self._provider_error(data, operation="send message"))
        sent = data.get("message")
        sent = sent if isinstance(sent, dict) else data
        sent_body = sent.get("body")
        sent_body = sent_body if isinstance(sent_body, dict) else {}
        external_id = (
            sent_body.get("mid")
            or sent.get("message_id")
            or sent.get("id")
            or data.get("message_id")
        )
        if not external_id:
            raise RuntimeError("MAX send message response has no message id")
        return SendResult(external_message_id=str(external_id))

    async def answer_callback(
        self, callback_id: str, text: str | None = None
    ) -> None:
        body = {"notification": text} if text else {}
        response = await self._request(
            "POST",
            "/answers",
            params={"callback_id": callback_id},
            json=body,
        )
        self._response_object(response, operation="answer callback")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        request_timeout: float | None = None,
        rate_limit_key: str | None = None,
    ) -> httpx.Response:
        await self._wait_api_slot(rate_limit_key)
        response = await self._client.request(
            method,
            f"{self._api_base}{path}",
            params=params,
            json=json,
            headers={"Authorization": self._token},
            timeout=request_timeout,
        )
        if response.status_code == 429:
            retry_after = 30
            try:
                data = response.json()
                if isinstance(data, dict):
                    retry_after = data.get("retry_after", retry_after)
            except Exception:
                pass
            raise MaxRateLimitError(retry_after=retry_after)
        if response.status_code >= 500:
            raise RuntimeError(f"MAX server error HTTP {response.status_code}")
        if response.status_code >= 400:
            try:
                data = response.json()
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}
            raise RuntimeError(
                self._provider_error(
                    data,
                    operation=f"HTTP {response.status_code}",
                )
            )
        return response

    @classmethod
    def _response_object(
        cls,
        response: httpx.Response,
        *,
        operation: str,
        allowed_error_codes: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        try:
            data = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"MAX {operation} response is not valid JSON"
            ) from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"MAX {operation} response is not an object")
        code = data.get("code")
        if code and str(code) not in allowed_error_codes:
            raise RuntimeError(cls._provider_error(data, operation=operation))
        return data

    @staticmethod
    def _provider_error(data: dict[str, Any], *, operation: str) -> str:
        code = str(data.get("code") or "unknown_error")
        description = str(
            data.get("message")
            or data.get("description")
            or data.get("error")
            or "provider rejected the request"
        )
        return f"MAX {operation} error {code}: {description}"

    async def _wait_api_slot(self, rate_limit_key: str | None = None) -> None:
        while True:
            async with self._rate_lock:
                now = time.monotonic()
                cutoff = now - 1
                self._prune_request_times(self._request_times, cutoff)
                chat_times = (
                    self._chat_request_times[rate_limit_key]
                    if rate_limit_key is not None
                    else None
                )
                if chat_times is not None:
                    self._prune_request_times(chat_times, cutoff)
                global_delay = self._slot_delay(
                    self._request_times,
                    self._rate,
                    now,
                )
                chat_delay = self._slot_delay(
                    chat_times,
                    _MAX_CHAT_REQUESTS_PER_SECOND,
                    now,
                )
                delay = max(global_delay, chat_delay)
                if delay <= 0:
                    self._request_times.append(now)
                    if chat_times is not None:
                        chat_times.append(now)
                    if len(self._chat_request_times) > 1000:
                        self._chat_request_times = defaultdict(
                            deque,
                            {
                                key: values
                                for key, values in self._chat_request_times.items()
                                if values and values[-1] > cutoff
                            },
                        )
                    return
            await asyncio.sleep(delay)

    @staticmethod
    def _prune_request_times(values: deque[float], cutoff: float) -> None:
        while values and values[0] <= cutoff:
            values.popleft()

    @staticmethod
    def _slot_delay(
        values: deque[float] | None,
        limit: int,
        now: float,
    ) -> float:
        if values is None or len(values) < limit:
            return 0.0
        return max(0.0, 1 - (now - values[0]))

    @staticmethod
    def _identity_candidates(item: dict[str, Any]) -> tuple[object, ...]:
        callback = item.get("callback")
        callback = callback if isinstance(callback, dict) else {}
        message = item.get("message")
        message = message if isinstance(message, dict) else {}
        body = message.get("body")
        body = body if isinstance(body, dict) else {}
        return (
            item.get("update_id"),
            callback.get("callback_id"),
            body.get("mid"),
            message.get("message_id"),
            item.get("timestamp"),
        )

    @staticmethod
    def _user_id(item: dict[str, Any]) -> str:
        for key in ("user", "from", "sender"):
            user = item.get(key)
            if isinstance(user, dict):
                value = user.get("id") or user.get("user_id")
                if value is not None:
                    return str(value)
        value = item.get("user_id")
        return str(value) if value is not None else ""

    @staticmethod
    def _text(item: dict[str, Any]) -> str | None:
        text = item.get("text")
        body = item.get("body")
        if text is None and isinstance(body, dict):
            text = body.get("text")
        return str(text) if text is not None else None

    @staticmethod
    def _event(
        external_event_id: str,
        user_id: str,
        kind: EventKind,
        raw: dict[str, Any],
        **values: Any,
    ) -> InboundEvent:
        return InboundEvent(
            provider=Provider.MAX,
            external_event_id=external_event_id,
            external_user_id=user_id,
            chat_id=user_id,
            kind=kind,
            raw=raw,
            **values,
        )

    def _button(self, button: Any) -> dict[str, Any]:
        if button.callback_token is not None:
            return {
                "type": "callback",
                "text": button.text,
                "payload": encode_callback(
                    button.callback_token,
                    signing_secret=self._signing_secret,
                ),
            }
        if button.url is not None:
            return {"type": "link", "text": button.text, "url": button.url}
        if button.request_location:
            return {"type": "request_geo_location", "text": button.text}
        raise ValueError("unsupported MAX button")


class MaxRateLimitError(Exception):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after
        super().__init__(f"MAX rate limited: retry after {retry_after}s")
