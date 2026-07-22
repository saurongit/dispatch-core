from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any


class Provider(StrEnum):
    TELEGRAM = "telegram"
    MAX = "max"


@dataclass(frozen=True, slots=True)
class InboundEnvelope:
    provider: Provider
    external_event_id: str
    organization_id: str
    payload: dict[str, Any]
    consumer_key: str = ""
    attempts: int = 0

    def __post_init__(self) -> None:
        if not self.external_event_id or not self.organization_id:
            raise ValueError("inbound event and organization identifiers are required")


@dataclass(frozen=True, slots=True)
class OutboundButton:
    text: str
    callback_token: str | None = None
    url: str | None = None
    request_location: bool = False
    row: int = 0

    def __post_init__(self) -> None:
        destinations = sum(
            (
                self.callback_token is not None,
                self.url is not None,
                self.request_location,
            )
        )
        if not self.text:
            raise ValueError("button text is required")
        if destinations != 1:
            raise ValueError("button requires exactly one action")
        if self.row < 0:
            raise ValueError("button row cannot be negative")


@dataclass(frozen=True, slots=True)
class OutboundEnvelope:
    message_id: int
    deduplication_key: str
    organization_id: str
    provider: Provider
    recipient_id: str
    text: str
    buttons: tuple[OutboundButton, ...] = ()
    attempts: int = 0
    consumer_key: str = ""


@dataclass(frozen=True, slots=True)
class CallbackAction:
    token: str
    organization_id: str
    action: str
    payload: MappingProxyType[str, Any]
    allowed_role: str | None
    expires_at: datetime

    @classmethod
    def create(
        cls,
        *,
        token: str,
        organization_id: str,
        action: str,
        payload: dict[str, Any],
        allowed_role: str | None,
        expires_at: datetime,
    ) -> CallbackAction:
        return cls(
            token=token,
            organization_id=organization_id,
            action=action,
            payload=MappingProxyType(dict(payload)),
            allowed_role=allowed_role,
            expires_at=expires_at,
        )
