"""End-to-end: admin configures a pack, publishes, then a client uses intake."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.messaging.config import (
    CONFIG_EVIDENCE_PHOTO_INC,
    CONFIG_FIELD_ADD,
    CONFIG_FIELD_REQUIRED,
    CONFIG_FIELD_TYPE,
    CONFIG_SERVICE_ADD,
    ConfigCoordinator,
)
from dispatch_core.messaging.intake import (
    INTAKE_CONFIRM,
    INTAKE_PICK_SERVICE,
    INTAKE_SERVICES_DONE,
    IntakeCoordinator,
)
from dispatch_core.messaging.models import Provider
from dispatch_core.packs.catalog import PackDefinition

# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


@dataclass
class FakePackStore:
    _draft: PackDefinition | None = None
    _active: PackDefinition | None = None

    async def active(self, organization_id: str) -> PackDefinition | None:
        return self._active

    async def draft(self, organization_id: str) -> PackDefinition | None:
        return self._draft

    async def ensure_draft(
        self, organization_id: str, *, seed: PackDefinition
    ) -> PackDefinition:
        if self._draft is None:
            self._draft = seed
        return self._draft

    async def update_draft(
        self, organization_id: str, definition: PackDefinition
    ) -> None:
        self._draft = definition

    async def publish_draft(self, organization_id: str) -> int:
        if self._draft is None:
            from dispatch_core.domain.errors import NotFound

            raise NotFound("no draft")
        self._active = self._draft
        self._draft = None
        return 1


@dataclass
class FakeSessionStore:
    store: dict[str, dict[str, Any]] = field(default_factory=dict)

    async def get(
        self, organization_id: str, actor_id: str
    ) -> dict[str, Any] | None:
        value = self.store.get(f"{organization_id}:{actor_id}")
        return dict(value) if value is not None else None

    async def put(
        self,
        *,
        organization_id: str,
        actor_id: str,
        provider: Provider,
        state: dict[str, Any],
    ) -> None:
        self.store[f"{organization_id}:{actor_id}"] = dict(state)

    async def clear(self, organization_id: str, actor_id: str) -> None:
        self.store.pop(f"{organization_id}:{actor_id}", None)


@dataclass
class FakeService:
    orders: list[dict[str, Any]] = field(default_factory=list)

    async def create_order(self, **values: Any) -> None:
        self.orders.append(values)


def _admin() -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="admin:100",
        role="admin",
        display_name="Admin",
        provider=Provider.TELEGRAM,
        external_user_id="100",
    )


def _client() -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="telegram:7001",
        role="requester",
        display_name="7001",
        provider=Provider.TELEGRAM,
        external_user_id="7001",
    )


# ---------------------------------------------------------------------------
# Test: full round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_publishes_pack_then_client_creates_order() -> None:
    packs = FakePackStore()

    # --- admin configures ---------------------------------------------------
    config = ConfigCoordinator(
        packs=packs,  # type: ignore[arg-type]
        sessions=FakeSessionStore(),
    )
    who = _admin()

    # brand
    await config.handle_text(who, "/brand")
    await config.handle_text(who, "PipeFix")
    await config.handle_text(who, "Здравствуйте!")
    await config.handle_text(who, "8-800")

    # service
    await config.handle_text(who, "/services")
    await config.handle_callback(who, CONFIG_SERVICE_ADD, {})
    await config.handle_text(who, "Ремонт")

    # field: Адрес (address, required)
    await config.handle_text(who, "/fields")
    await config.handle_callback(who, CONFIG_FIELD_ADD, {})
    await config.handle_text(who, "Адрес")
    await config.handle_callback(who, CONFIG_FIELD_TYPE, {"type": "address"})
    await config.handle_callback(who, CONFIG_FIELD_REQUIRED, {"required": True})

    # field: Описание (text, optional)
    await config.handle_callback(who, CONFIG_FIELD_ADD, {})
    await config.handle_text(who, "Описание")
    await config.handle_callback(who, CONFIG_FIELD_TYPE, {"type": "text"})
    await config.handle_callback(who, CONFIG_FIELD_REQUIRED, {"required": False})

    # evidence: bump photos to 2
    await config.handle_text(who, "/evidence")
    await config.handle_callback(who, CONFIG_EVIDENCE_PHOTO_INC, {})

    # publish
    reply = await config.handle_text(who, "/publish")
    assert "готово" in reply.text.lower()

    assert packs._active is not None
    assert packs._active.branding.name == "PipeFix"
    assert len(packs._active.service_catalog.categories) == 1
    assert len(packs._active.fields) == 2
    assert packs._active.evidence.minimum_photos == 1

    # --- client flow ---

    service = FakeService()
    client_sessions = FakeSessionStore()
    intake = IntakeCoordinator(
        packs=packs,  # type: ignore[arg-type]
        sessions=client_sessions,
        service=service,
    )
    guy = _client()

    reply = await intake.start(guy)
    assert "Здравствуйте!" in reply.text
    actions = {b.action for b in reply.buttons}
    assert INTAKE_PICK_SERVICE in actions
    assert INTAKE_SERVICES_DONE in actions

    # pick the service
    cat_key = packs._active.service_catalog.categories[0].key
    await intake.handle_callback(guy, INTAKE_PICK_SERVICE, {"service": cat_key})
    reply = await intake.handle_callback(guy, INTAKE_SERVICES_DONE, {})

    # fill fields (keys are auto-generated by _slug from Cyrillic labels)
    field_keys = [f.key for f in packs._active.ordered_fields()]
    address_key = field_keys[0]
    desc_key = field_keys[1]

    reply = await intake.handle_text(guy, "ул. Ленина 10")
    assert "Описание" in reply.text  # next optional field
    reply = await intake.handle_text(guy, "Течёт кран")
    assert INTAKE_CONFIRM in {b.action for b in reply.buttons}

    # confirm
    reply = await intake.handle_callback(guy, INTAKE_CONFIRM, {})
    assert "создана" in reply.text.lower()
    assert len(service.orders) == 1
    order = service.orders[0]
    assert order["work_type"] == cat_key
    assert order["details"][address_key] == "ул. Ленина 10"
    assert order["details"][desc_key] == "Течёт кран"
    assert order["evidence_requirements"].minimum_photos == 1
