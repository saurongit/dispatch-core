from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import Protocol


class EgressMode(StrEnum):
    DIRECT = "direct"
    HTTP_PROXY = "http_proxy"
    SOCKS5 = "socks5"
    WIREPROXY = "wireproxy"


@dataclass(frozen=True, slots=True)
class EgressRoute:
    route_id: str
    mode: EgressMode
    priority: int
    endpoint: str | None = None
    secret_ref: str | None = None

    def __post_init__(self) -> None:
        if not self.route_id:
            raise ValueError("route_id is required")
        if self.priority < 0:
            raise ValueError("priority cannot be negative")
        if self.mode is EgressMode.DIRECT and self.endpoint is not None:
            raise ValueError("direct route must not define an endpoint")
        if self.mode is not EgressMode.DIRECT and not self.endpoint:
            raise ValueError("proxy route requires an endpoint")


@dataclass(frozen=True, slots=True)
class RouteHealth:
    route_id: str
    healthy: bool
    checked_at: datetime


class EgressPolicy:
    def choose(
        self,
        routes: Sequence[EgressRoute],
        health: Sequence[RouteHealth],
    ) -> EgressRoute | None:
        health_by_id = {item.route_id: item for item in health}
        candidates = (
            route
            for route in routes
            if route.route_id in health_by_id
            and health_by_id[route.route_id].healthy
        )
        return min(
            candidates,
            key=lambda item: (item.priority, item.route_id),
            default=None,
        )


@dataclass(frozen=True, slots=True)
class SignedConfigEnvelope:
    installation_id: str
    version: int
    issued_at: datetime
    expires_at: datetime
    payload_sha256: str
    key_id: str
    signature: str

    def validate_metadata(
        self,
        payload: bytes,
        *,
        expected_installation_id: str,
        minimum_version: int,
        now: datetime | None = None,
    ) -> None:
        instant = now or datetime.now(UTC)
        if self.installation_id != expected_installation_id:
            raise ValueError("configuration belongs to another installation")
        if self.version < minimum_version:
            raise ValueError("configuration rollback detected")
        if instant > self.expires_at:
            raise ValueError("configuration metadata has expired")
        if self.issued_at > instant:
            raise ValueError("configuration issue time is in the future")
        if sha256(payload).hexdigest() != self.payload_sha256:
            raise ValueError("configuration payload digest mismatch")


class ConfigSignatureVerifier(Protocol):
    def verify(self, envelope: SignedConfigEnvelope, payload: bytes) -> bool: ...


class RemoteConfigProvider(Protocol):
    def fetch(
        self, *, after_version: int
    ) -> tuple[SignedConfigEnvelope, bytes] | None: ...
