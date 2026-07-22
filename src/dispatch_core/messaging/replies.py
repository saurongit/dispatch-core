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


@dataclass(frozen=True, slots=True)
class Reply:
    text: str
    buttons: tuple[ReplyButton, ...] = ()
