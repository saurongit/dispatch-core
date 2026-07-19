from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dispatch_core.messaging.models import Provider


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    organization_id: str
    actor_id: str
    role: str
    display_name: str
    provider: Provider
    external_user_id: str


class IdentityResolver(Protocol):
    async def resolve(
        self,
        *,
        organization_id: str,
        provider: Provider,
        external_user_id: str,
    ) -> ActorIdentity | None: ...
