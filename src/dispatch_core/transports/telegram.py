from __future__ import annotations

from typing import Any

import httpx

from dispatch_core.messaging.models import OutboundEnvelope, Provider

from .common import decode_callback, encode_callback, group_buttons, stable_payload_id
from .contracts import EventKind, InboundEvent, SendResult


class TelegramTransport:
    provider = Provider.TELEGRAM

    def __init__(
        self,
        token: str,
        *,
        client: httpx.AsyncClient | None = None,
        proxy: str | None = None,
        timeout_seconds: float = 40,
    ) -> None:
        if not token:
            raise ValueError("Telegram bot token is required")
        self._token = token
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            proxy=proxy,
            timeout=httpx.Timeout(timeout_seconds),
        )

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
            token = decode_callback(callback.get("data"))
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
            f"{self._base_url}/getUpdates",
            json=parameters,
            timeout=timeout_seconds + 15,
        )
        data = self._telegram_result(response)
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
        payload: dict[str, Any] = {
            "chat_id": message.recipient_id,
            "text": message.text,
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
            f"{self._base_url}/sendMessage",
            json=payload,
        )
        result = self._telegram_result(response)
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
            f"{self._base_url}/answerCallbackQuery",
            json=payload,
        )
        self._telegram_result(response)

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
            f"{self._base_url}/setWebhook",
            json=payload,
        )
        self._telegram_result(response)

    @staticmethod
    def _event(
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

    @staticmethod
    def _button(button: Any) -> dict[str, Any]:
        if button.callback_token is not None:
            return {
                "text": button.text,
                "callback_data": encode_callback(button.callback_token),
            }
        if button.url is not None:
            return {"text": button.text, "url": button.url}
        if button.request_location:
            raise ValueError("Telegram location request requires a reply keyboard")
        raise ValueError("unsupported Telegram button")

    @staticmethod
    def _telegram_result(response: httpx.Response) -> Any:
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not data.get("ok"):
            description = data.get("description") if isinstance(data, dict) else data
            raise RuntimeError(f"Telegram API rejected request: {description}")
        return data.get("result")
