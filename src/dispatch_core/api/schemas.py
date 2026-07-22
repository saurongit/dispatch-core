from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dispatch_core.domain.tracking import LocationSource
from dispatch_core.domain.work_order import PoolMode, WorkOrder
from dispatch_core.messaging.models import Provider

_MAX_DETAILS_KEYS = 50
_MAX_DETAIL_VALUE_LEN = 1000


class EvidenceRequirementsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    minimum_photos: int = Field(default=0, ge=0, le=20)
    comment_required: bool = False
    signature_required: bool = False
    customer_code_required: bool = False


class CreateOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    work_type: str = Field(min_length=1, max_length=100)
    source: str = Field(min_length=1, max_length=100)
    requester_id: str | None = Field(default=None, max_length=200)
    details: dict[str, Any] = Field(default_factory=dict)
    evidence: EvidenceRequirementsInput = Field(
        default_factory=EvidenceRequirementsInput
    )

    @model_validator(mode="after")
    def validate_details(self) -> CreateOrderInput:
        if len(self.details) > _MAX_DETAILS_KEYS:
            raise ValueError(
                f"details must not contain more than {_MAX_DETAILS_KEYS} keys"
            )
        for key, value in self.details.items():
            if isinstance(value, str) and len(value) > _MAX_DETAIL_VALUE_LEN:
                raise ValueError(
                    f"detail value for {key!r} exceeds {_MAX_DETAIL_VALUE_LEN} chars"
                )
        return self


class ActorInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor_id: str = Field(min_length=1, max_length=200)


class ActorCreateInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor_id: str = Field(min_length=1, max_length=200)
    role: str = Field(pattern="^(admin|operator|master|client)$")
    display_name: str = Field(min_length=1, max_length=200)
    provider: Provider | None = None
    external_user_id: str | None = Field(default=None, min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_identity_pair(self) -> ActorCreateInput:
        if (self.provider is None) != (self.external_user_id is None):
            raise ValueError(
                "provider and external_user_id must be specified together"
            )
        return self


class ActorResponse(BaseModel):
    actor_id: str
    role: str
    display_name: str
    provider: Provider | None
    external_user_id: str | None


class PublishPoolInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: PoolMode
    actor_id: str | None = Field(default=None, max_length=200)


class AssignInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_id: str = Field(min_length=1, max_length=200)
    actor_id: str | None = Field(default=None, max_length=200)


class TravelInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_id: str = Field(min_length=1, max_length=200)
    session_id: str | None = Field(default=None, min_length=1, max_length=200)


class LocationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    accuracy_m: float | None = Field(default=None, ge=0, le=100_000)
    source: LocationSource


class CompleteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_id: str = Field(min_length=1, max_length=200)
    photo_refs: tuple[str, ...] = Field(default=(), max_length=20)
    comment: str | None = Field(default=None, max_length=10_000)
    signature_ref: str | None = Field(default=None, max_length=2000)
    customer_code: str | None = Field(default=None, max_length=200)


class CancelInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)
    actor_id: str | None = Field(default=None, max_length=200)


class WorkOrderResponse(BaseModel):
    id: str
    organization_id: str
    work_type: str
    source: str
    details: dict[str, Any]
    requester_id: str | None
    coordinator_id: str | None
    status: str
    pool_mode: str | None
    assignee_id: str | None
    interested_executor_ids: tuple[str, ...]
    version: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, order: WorkOrder) -> WorkOrderResponse:
        return cls(
            id=order.id,
            organization_id=order.organization_id,
            work_type=order.work_type,
            source=order.source,
            details=dict(order.details),
            requester_id=order.requester_id,
            coordinator_id=order.coordinator_id,
            status=order.status.value,
            pool_mode=order.pool_mode.value if order.pool_mode else None,
            assignee_id=order.assignee_id,
            interested_executor_ids=order.interested_executor_ids(),
            version=order.version,
            created_at=order.created_at,
            updated_at=order.updated_at,
        )


class TravelStartedResponse(BaseModel):
    order: WorkOrderResponse
    tracking_session_id: str


class LocationRecordedResponse(BaseModel):
    tracking_session_id: str
    point_count: int
    version: int


class ExecutorTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    actor_id: str = Field(min_length=1, max_length=200)


class ExecutorTokenResponse(BaseModel):
    token: str
    expires_at: int
