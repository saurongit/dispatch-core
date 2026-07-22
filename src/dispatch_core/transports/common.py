from __future__ import annotations

import hmac
import json
import logging
import time
from collections import defaultdict
from hashlib import sha256
from typing import Any

from dispatch_core.messaging.models import OutboundButton

logger = logging.getLogger(__name__)

CALLBACK_VERSION = "dc2"
_CALLBACK_LEGACY_VERSION = "dc1"
_EXECUTOR_TOKEN_VERSION = "dt1"
_HMAC_SECRET_LENGTH = 32
_CALLBACK_DATA_MAX = 64  # Telegram limit
_CALLBACK_SIG_LEN = 16   # truncated HMAC-SHA256 hex (64-bit)


def encode_callback(token: str, *, signing_secret: str) -> str:
    if not token or len(token) > 38:
        raise ValueError("callback token length must be between 1 and 38")
    if ":" in token:
        raise ValueError("callback token must not contain ':'")
    if not signing_secret:
        raise ValueError("signing_secret is required")
    sig = _hmac_hex(token, signing_secret)[:_CALLBACK_SIG_LEN]
    result = f"{CALLBACK_VERSION}:{token}:{sig}"
    if len(result) > _CALLBACK_DATA_MAX:
        raise ValueError(
            f"callback data {len(result)} bytes exceeds Telegram limit "
            f"of {_CALLBACK_DATA_MAX} bytes"
        )
    return result


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
    if not token or len(token) > 38:
        return None
    # Accept truncated (16 hex) or legacy full (64 hex) signatures
    if not sig or len(sig) not in (_CALLBACK_SIG_LEN, 64):
        return None
    if not signing_secret:
        logger.warning("callback received dc2 token but no signing_secret configured")
        return None
    expected = _hmac_hex(token, signing_secret)
    if not hmac.compare_digest(sig, expected[: len(sig)]):
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


def encode_executor_token(
    organization_id: str,
    actor_id: str,
    *,
    signing_secret: str,
    ttl_seconds: int = 3600,
) -> tuple[str, int]:
    if not organization_id or not actor_id:
        raise ValueError("organization_id and actor_id are required")
    if ":" in organization_id or ":" in actor_id:
        raise ValueError("organization_id and actor_id must not contain ':'")
    expires_at = int(time.time()) + ttl_seconds
    payload = f"{organization_id}:{actor_id}:{expires_at}"
    sig = _hmac_hex(payload, signing_secret)
    token = (
        f"{_EXECUTOR_TOKEN_VERSION}:{organization_id}:{actor_id}"
        f":{expires_at}:{sig}"
    )
    return token, expires_at


def decode_executor_token(
    value: object,
    *,
    signing_secret: str | None = None,
) -> tuple[str, str] | None:
    if not isinstance(value, str):
        return None
    if not value.startswith(f"{_EXECUTOR_TOKEN_VERSION}:"):
        return None
    parts = value.split(":", 4)
    if len(parts) != 5:
        return None
    _, org_id, actor_id, expires_str, sig = parts
    if not org_id or not actor_id:
        return None
    if not sig or len(sig) != 64:
        return None
    try:
        expires_at = int(expires_str)
    except ValueError:
        return None
    if time.time() > expires_at:
        logger.warning("executor token has expired")
        return None
    if not signing_secret:
        logger.warning("executor token received but no signing_secret configured")
        return None
    payload = f"{org_id}:{actor_id}:{expires_at}"
    expected = _hmac_hex(payload, signing_secret)
    if not hmac.compare_digest(sig, expected):
        logger.warning("executor token HMAC mismatch — possible tampering")
        return None
    return org_id, actor_id
