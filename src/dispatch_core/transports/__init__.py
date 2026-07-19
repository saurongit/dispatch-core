"""Provider adapters that normalize Telegram and MAX at the system edge."""

from .contracts import EventKind, InboundEvent, SendResult, Transport
from .max import MaxTransport
from .telegram import TelegramTransport

__all__ = [
    "EventKind",
    "InboundEvent",
    "MaxTransport",
    "SendResult",
    "TelegramTransport",
    "Transport",
]
