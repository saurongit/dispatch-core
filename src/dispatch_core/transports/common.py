from __future__ import annotations

import hmac
import json
import logging
from collections import defaultdict
from hashlib import sha256
from typing import Any

from dispatch_core.messaging.models import OutboundButton

logger = logging.getLogger(__name__)

CALLBACK_VERSION = "dc2"
_CALLBACK_LEGACY_VERSION = "dc1"
_HMAC_SECRET_LENGTH = 32


def encode_callback(token: str, *, signing_secret: str) -> str:
    if not token or len(token) > 48:
        raise ValueError("callback token length must be between 1 and 48")
    if not signing_secret:
        raise ValueError("signing_secret is required")
    sig = _hmac_hex(token, signing_secret)
    return f"{CALLBACK_VERSION}:{token}:{sig}"


def decode_callback(
    value: object, *, signing_secret: str | None = None
) -> str | None:
    if not isinstance(value, str):
        return None
    if value.startswith(f"{CALLBACK_VERSION}:"):
        return _decode_v2(value, signing_secret)
    if value.startswith(f"{_CALLBACK_LEGACY_VERSION}:"):
        return _decode_legacy(value)
    return None


def _decode_v2(value: str, signing_secret: str | None) -> str | None:
    parts = value.split(":", 2)
    if len(parts) != 3:
        return None
    _, token, sig = parts
    if not token or len(token) > 48:
        return None
    if not sig or len(sig) != 64:
        return None
    if not signing_secret:
        logger.warning("callback received dc2 token but no signing_secret configured")
        return None
    expected = _hmac_hex(token, signing_secret)
    if not hmac.compare_digest(sig, expected):
        logger.warning("callback token HMAC mismatch — possible tampering")
        return None
    return token


def _decode_legacy(value: str) -> str | None:
    token = value.removeprefix(f"{_CALLBACK_LEGACY_VERSION}:")
    if token and len(token) <= 48:
        logger.debug("accepted legacy dc1 callback token (no HMAC)")
        return token
    return None


def _hmac_hex(token: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        token.encode("utf-8"),
        sha256,
    ).hexdigest()


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
