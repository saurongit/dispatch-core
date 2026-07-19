from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class DomainEvent:
    event_id: str
    organization_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    name: str
    occurred_at: datetime
    payload: Mapping[str, Any]

    def __deepcopy__(self, memo: dict[int, Any]) -> DomainEvent:
        """Keep the public payload read-only while allowing repository snapshots."""
        copied = DomainEvent(
            event_id=self.event_id,
            organization_id=self.organization_id,
            aggregate_type=self.aggregate_type,
            aggregate_id=self.aggregate_id,
            aggregate_version=self.aggregate_version,
            name=self.name,
            occurred_at=self.occurred_at,
            payload=MappingProxyType(deepcopy(dict(self.payload), memo)),
        )
        memo[id(self)] = copied
        return copied

    @classmethod
    def create(
        cls,
        *,
        organization_id: str,
        aggregate_type: str,
        aggregate_id: str,
        aggregate_version: int,
        name: str,
        payload: Mapping[str, Any],
        occurred_at: datetime | None = None,
    ) -> DomainEvent:
        return cls(
            event_id=str(uuid4()),
            organization_id=organization_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            aggregate_version=aggregate_version,
            name=name,
            occurred_at=occurred_at or datetime.now(UTC),
            payload=MappingProxyType(dict(payload)),
        )
