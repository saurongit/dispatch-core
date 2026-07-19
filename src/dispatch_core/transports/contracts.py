from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from dispatch_core.messaging.models import OutboundEnvelope, Provider


class EventKind(StrEnum):
    MESSAGE = "message"
    CALLBACK = "callback"
    CONTACT = "contact"
    LOCATION = "location"
    PHOTO = "photo"
    START = "start"


@dataclass(frozen=True, slots=True)
class InboundEvent:
    provider: Provider
    external_event_id: str
    external_user_id: str
    chat_id: str
    kind: EventKind
    text: str | None = None
    callback_token: str | None = None
    callback_id: str | None = None
    media_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    raw: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.external_event_id:
            raise ValueError("external_event_id is required")
        if not self.external_user_id:
            raise ValueError("external_user_id is required")
        if not self.chat_id:
            raise ValueError("chat_id is required")
        if self.kind is EventKind.CALLBACK and not self.callback_token:
            raise ValueError("callback event requires a callback token")
        if self.kind is EventKind.LOCATION and (
            self.latitude is None or self.longitude is None
        ):
            raise ValueError("location event requires coordinates")
        if self.kind is EventKind.PHOTO and not self.media_id:
            raise ValueError("photo event requires media_id")


@dataclass(frozen=True, slots=True)
class SendResult:
    external_message_id: str | None = None


class Transport(Protocol):
    provider: Provider

    def external_event_id(self, payload: dict[str, Any]) -> str: ...

    def parse(self, payload: dict[str, Any]) -> tuple[InboundEvent, ...]: ...

    async def send(self, message: OutboundEnvelope) -> SendResult: ...

    async def answer_callback(
        self, callback_id: str, text: str | None = None
    ) -> None: ...

    async def close(self) -> None: ...
