from __future__ import annotations

import logging
from typing import Any

import httpx

from dispatch_core.messaging.models import OutboundEnvelope, Provider

from .common import decode_callback, encode_callback, group_buttons, stable_payload_id
from .contracts import EventKind, InboundEvent, SendResult

logger = logging.getLogger(__name__)

_TEXT_LENGTH_LIMIT = 4096


class TelegramTransport:
    provider = Provider.TELEGRAM

    def __init__(
        self,
        token: str,
        *,
        signing_secret: str = "",
        client: httpx.AsyncClient | None = None,
        proxy: str | None = None,
        timeout_seconds: float = 40,
    ) -> None:
        if not token:
            raise ValueError("Telegram bot token is required")
        self._token = token
        self._signing_secret = signing_secret
        self._api_base = "https://api.telegram.org"
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            proxy=proxy,
            headers={"Authorization": f"Bot {token}"},
            timeout=httpx.Timeout(timeout_seconds),
        )

    def _bot_url(self) -> str:
        return f"{self._api_base}/bot{self._token}"

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def external_event_id(self, payload: dict[str, Any]) -> str:
        update_id = payload.get("update_id")
        if isinstance(update_id, int):
            return f"telegram:{update_id}"
        return stable_payload_id(self.provider.value, payload)

    def parse(self, payload: dict[str, Any]) -> tuple[InboundEvent, ...]:
        external_event_id = self.external_event_id(payload)
        callback = payload.get("callback_query")
        if isinstance(callback, dict):
            token = decode_callback(
                callback.get("data"), signing_secret=self._signing_secret
            )
            actor = callback.get("from")
            message = callback.get("message")
            actor = actor if isinstance(actor, dict) else {}
            message = message if isinstance(message, dict) else {}
            chat = message.get("chat")
            chat = chat if isinstance(chat, dict) else {}
            user_id = str(actor.get("id") or "")
            chat_id = str(chat.get("id") or user_id)
            if token and user_id:
                return (
                    InboundEvent(
                        provider=self.provider,
                        external_event_id=external_event_id,
                        external_user_id=user_id,
                        chat_id=chat_id,
                        kind=EventKind.CALLBACK,
                        callback_token=token,
                        callback_id=str(callback.get("id") or "") or None,
                        raw=payload,
                    ),
                )
            return ()

        message = payload.get("message") or payload.get("edited_message")
        if not isinstance(message, dict):
            return ()
        actor = message.get("from")
        chat = message.get("chat")
        actor = actor if isinstance(actor, dict) else {}
        chat = chat if isinstance(chat, dict) else {}
        user_id = str(actor.get("id") or chat.get("id") or "")
        chat_id = str(chat.get("id") or user_id)
        if not user_id:
            return ()

        contact = message.get("contact")
        location = message.get("location")
        photos = message.get("photo")
        text = message.get("text")
        if isinstance(contact, dict) and contact.get("phone_number"):
            return (
                self._event(
                    external_event_id,
                    user_id,
                    chat_id,
                    EventKind.CONTACT,
                    payload,
                    text=str(contact["phone_number"]),
                ),
            )
        if isinstance(location, dict):
            try:
                latitude = float(location["latitude"])
                longitude = float(location["longitude"])
            except (KeyError, TypeError, ValueError):
                return ()
            return (
                self._event(
                    external_event_id,
                    user_id,
                    chat_id,
                    EventKind.LOCATION,
                    payload,
                    latitude=latitude,
                    longitude=longitude,
                ),
            )
        if isinstance(photos, list) and photos:
            largest = photos[-1]
            if isinstance(largest, dict) and largest.get("file_id"):
                return (
                    self._event(
                        external_event_id,
                        user_id,
                        chat_id,
                        EventKind.PHOTO,
                        payload,
                        text=str(message.get("caption") or "") or None,
                        media_id=str(largest["file_id"]),
                    ),
                )
        if isinstance(text, str):
            kind = EventKind.START if text.startswith("/start") else EventKind.MESSAGE
            return (
                self._event(
                    external_event_id,
                    user_id,
                    chat_id,
                    kind,
                    payload,
                    text=text,
                ),
            )
        return ()

    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout_seconds: int = 30,
        limit: int = 100,
    ) -> tuple[tuple[dict[str, Any], ...], int | None]:
        parameters: dict[str, Any] = {
            "timeout": timeout_seconds,
            "limit": limit,
            "allowed_updates": ["message", "edited_message", "callback_query"],
        }
        if offset is not None:
            parameters["offset"] = offset
        response = await self._client.post(
            f"{self._bot_url()}/getUpdates",
            json=parameters,
            timeout=timeout_seconds + 15,
        )
        data = self._check_response(response)
        if not isinstance(data, list):
            raise RuntimeError("Telegram getUpdates returned a non-list result")
        updates = tuple(item for item in data if isinstance(item, dict))
        update_ids = [
            item["update_id"]
            for item in updates
            if isinstance(item.get("update_id"), int)
        ]
        next_offset = max(update_ids) + 1 if update_ids else offset
        return updates, next_offset

    async def send(self, message: OutboundEnvelope) -> SendResult:
        text = message.text
        if len(text) > _TEXT_LENGTH_LIMIT:
            logger.warning(
                "truncating Telegram message from %d to %d chars",
                len(text),
                _TEXT_LENGTH_LIMIT,
            )
            text = text[:_TEXT_LENGTH_LIMIT]
        payload: dict[str, Any] = {
            "chat_id": message.recipient_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if message.buttons:
            payload["reply_markup"] = {
                "inline_keyboard": [
                    [self._button(button) for button in row]
                    for row in group_buttons(message.buttons)
                ]
            }
        response = await self._client.post(
            f"{self._bot_url()}/sendMessage",
            json=payload,
        )
        result = self._check_response(response)
        external_id = None
        if isinstance(result, dict) and result.get("message_id") is not None:
            external_id = str(result["message_id"])
        return SendResult(external_message_id=external_id)

    async def answer_callback(
        self, callback_id: str, text: str | None = None
    ) -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        response = await self._client.post(
            f"{self._bot_url()}/answerCallbackQuery",
            json=payload,
        )
        self._check_response(response)

    async def set_webhook(
        self,
        webhook_url: str,
        *,
        secret_token: str,
        drop_pending_updates: bool = False,
    ) -> None:
        payload = {
            "url": webhook_url,
            "secret_token": secret_token,
            "allowed_updates": ["message", "edited_message", "callback_query"],
            "drop_pending_updates": drop_pending_updates,
        }
        response = await self._client.post(
            f"{self._bot_url()}/setWebhook",
            json=payload,
        )
        self._check_response(response)

    async def set_my_commands(
        self,
        commands: tuple[tuple[str, str], ...],
        *,
        scope: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "commands": [
                {"command": command, "description": description}
                for command, description in commands
            ]
        }
        if scope is not None:
            payload["scope"] = scope
        response = await self._client.post(
            f"{self._bot_url()}/setMyCommands",
            json=payload,
        )
        self._check_response(response)

    def _event(
        self,
        external_event_id: str,
        user_id: str,
        chat_id: str,
        kind: EventKind,
        raw: dict[str, Any],
        **values: Any,
    ) -> InboundEvent:
        return InboundEvent(
            provider=Provider.TELEGRAM,
            external_event_id=external_event_id,
            external_user_id=user_id,
            chat_id=chat_id,
            kind=kind,
            raw=raw,
            **values,
        )

    def _button(self, button: Any) -> dict[str, Any]:
        if button.callback_token is not None:
            return {
                "text": button.text,
                "callback_data": encode_callback(
                    button.callback_token,
                    signing_secret=self._signing_secret,
                ),
            }
        if button.url is not None:
            return {"text": button.text, "url": button.url}
        if button.request_location:
            raise ValueError("Telegram location request requires a reply keyboard")
        raise ValueError("unsupported Telegram button")

    @staticmethod
    def _check_response(response: httpx.Response) -> Any:
        if response.status_code == 429:
            retry_after = None
            try:
                body = response.json()
                if isinstance(body, dict):
                    params = body.get("parameters")
                    if isinstance(params, dict):
                        retry_after = params.get("retry_after")
            except Exception:
                pass
            raise TelegramRateLimitError(
                retry_after=retry_after or 30,
                description="Too Many Requests",
            )
        if response.status_code >= 500:
            raise RuntimeError(
                f"Telegram server error HTTP {response.status_code}"
            )
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError(
                f"Telegram returned non-object response: {response.status_code}"
            )
        if not data.get("ok"):
            description = data.get("description", "unknown error")
            error_code = data.get("error_code", response.status_code)
            retry_params = data.get("parameters")
            if isinstance(retry_params, dict) and retry_params.get("retry_after"):
                raise TelegramRateLimitError(
                    retry_after=retry_params["retry_after"],
                    description=description,
                )
            raise RuntimeError(
                f"Telegram API error {error_code}: {description}"
            )
        return data.get("result")


class TelegramRateLimitError(Exception):
    def __init__(self, *, retry_after: int, description: str) -> None:
        self.retry_after = retry_after
        self.description = description
        super().__init__(f"rate limited: retry after {retry_after}s — {description}")
