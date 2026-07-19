from __future__ import annotations

import json
from collections import defaultdict
from hashlib import sha256
from typing import Any

from dispatch_core.messaging.models import OutboundButton

CALLBACK_PREFIX = "dc1:"


def encode_callback(token: str) -> str:
    if not token or len(token) > 48:
        raise ValueError("callback token length must be between 1 and 48")
    return CALLBACK_PREFIX + token


def decode_callback(value: object) -> str | None:
    if not isinstance(value, str) or not value.startswith(CALLBACK_PREFIX):
        return None
    token = value.removeprefix(CALLBACK_PREFIX)
    return token if token and len(token) <= 48 else None


def stable_payload_id(provider: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"{provider}:sha256:{sha256(encoded).hexdigest()}"


def group_buttons(
    buttons: tuple[OutboundButton, ...],
) -> list[list[OutboundButton]]:
    rows: defaultdict[int, list[OutboundButton]] = defaultdict(list)
    for button in buttons:
        rows[button.row].append(button)
    return [rows[row] for row in sorted(rows)]
