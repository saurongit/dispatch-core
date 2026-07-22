from __future__ import annotations

from pathlib import Path

from dispatch_core.api.settings import Settings
from dispatch_core.messaging.models import Provider
from dispatch_core.transports.contracts import Transport
from dispatch_core.transports.max import MaxTransport
from dispatch_core.transports.telegram import TelegramTransport


def build_transports(
    settings: Settings,
    *,
    allowed_modes: set[str] | None = None,
) -> dict[Provider, Transport]:
    transports: dict[Provider, Transport] = {}
    telegram_token = _secret(
        settings.telegram_bot_token,
        settings.telegram_bot_token_file,
    )
    telegram_enabled = settings.telegram_receive_mode != "disabled" and (
        allowed_modes is None or settings.telegram_receive_mode in allowed_modes
    )
    if telegram_enabled and not telegram_token:
        raise RuntimeError("Telegram receive mode is enabled but token is missing")
    if telegram_enabled and telegram_token:
        transports[Provider.TELEGRAM] = TelegramTransport(
            telegram_token,
            signing_secret=_secret_value(settings.callback_signing_secret) or "",
            proxy=_secret_value(settings.telegram_proxy),
        )
    max_token = _secret(settings.max_bot_token, settings.max_bot_token_file)
    max_enabled = settings.max_receive_mode != "disabled" and (
        allowed_modes is None or settings.max_receive_mode in allowed_modes
    )
    if max_enabled and not max_token:
        raise RuntimeError("MAX receive mode is enabled but token is missing")
    if max_enabled and max_token:
        transports[Provider.MAX] = MaxTransport(
            max_token,
            signing_secret=_secret_value(settings.callback_signing_secret) or "",
            proxy=_secret_value(settings.max_proxy),
        )
    return transports


def _secret(value, file_path: Path | None) -> str | None:
    if value is not None:
        result = value.get_secret_value().strip()
        return result or None
    if file_path is None:
        return None
    result = file_path.read_text(encoding="utf-8").strip()
    return result or None


def _secret_value(value) -> str | None:
    if value is None:
        return None
    result = value.get_secret_value().strip()
    return result or None
