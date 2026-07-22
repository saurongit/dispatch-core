from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ReplyButton:
    text: str
    action: str
    payload: dict[str, Any] = field(default_factory=dict)
    allowed_role: str | None = None
    row: int = 0
    url: str | None = None
    request_location: bool = False

    def __post_init__(self) -> None:
        if not self.text:
            raise ValueError("button text is required")
        if self.row < 0:
            raise ValueError("button row cannot be negative")
        if self.url is not None and self.request_location:
            raise ValueError("button cannot combine URL and location request")
        if not self.action and self.url is None and not self.request_location:
            raise ValueError("button action is required")


@dataclass(frozen=True, slots=True)
class Reply:
    text: str
    buttons: tuple[ReplyButton, ...] = ()
