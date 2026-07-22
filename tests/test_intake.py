from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.messaging.intake import (
    INTAKE_CANCEL,
    INTAKE_CONFIRM,
    INTAKE_PICK_SERVICE,
    INTAKE_SERVICES_DONE,
    IntakeCoordinator,
)
from dispatch_core.messaging.models import Provider
from dispatch_core.packs.catalog import PackDefinition, seed_definition


def identity() -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="telegram:7001",
        role="client",
        display_name="7001",
        provider=Provider.TELEGRAM,
        external_user_id="7001",
    )


@dataclass
class FakePacks:
    pack: PackDefinition | None

    async def active(self, organization_id: str) -> PackDefinition | None:
        return self.pack


@dataclass
class FakeSessions:
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


def coordinator(
    pack: PackDefinition | None = None,
) -> tuple[IntakeCoordinator, FakeSessions, FakeService]:
    sessions = FakeSessions()
    service = FakeService()
    target = IntakeCoordinator(
        packs=FakePacks(pack or seed_definition("field_service")),
        sessions=sessions,
        service=service,
    )
    return target, sessions, service


async def _go_to_services(target: IntakeCoordinator, who: ActorIdentity) -> None:
    """Navigate through hardcoded phone+address to the services step."""
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    await target.handle_text(who, "ул. Ленина 10")


@pytest.mark.asyncio
async def test_start_asks_for_phone() -> None:
    target, sessions, _ = coordinator()
    reply = await target.start(identity())
    assert "телефон" in reply.text.lower()
    actions = {button.action for button in reply.buttons}
    assert INTAKE_CANCEL in actions
    state = sessions.store["org-1:telegram:7001"]
    assert state["step"] == "phone"


@pytest.mark.asyncio
async def test_phone_validates_min_length() -> None:
    target, _, _ = coordinator()
    who = identity()
    await target.start(who)
    reply = await target.handle_text(who, "12")
    assert "телефон" in reply.text.lower() or "минимум" in reply.text.lower()


@pytest.mark.asyncio
async def test_phone_to_address_to_services() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    await target.start(who)
    reply = await target.handle_text(who, "+7 999 123 4567")
    assert "адрес" in reply.text.lower()
    state = sessions.store["org-1:telegram:7001"]
    assert state["step"] == "address"

    reply = await target.handle_text(who, "ул. Ленина 10")
    assert INTAKE_PICK_SERVICE in {b.action for b in reply.buttons}
    state = sessions.store["org-1:telegram:7001"]
    assert state["step"] == "services"


@pytest.mark.asyncio
async def test_multi_select_toggles_and_requires_a_choice() -> None:
    target, _, _ = coordinator()
    who = identity()
    await _go_to_services(target, who)
    empty = await target.handle_callback(identity(), INTAKE_SERVICES_DONE, {})
    assert "хотя бы одну" in empty.text
    picked = await target.handle_callback(
        identity(), INTAKE_PICK_SERVICE, {"service": "repair"}
    )
    assert any("✅" in button.text for button in picked.buttons)


@pytest.mark.asyncio
async def test_full_flow_creates_order_from_pack() -> None:
    target, sessions, service = coordinator()
    who = identity()
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    await target.handle_text(who, "Ленина 1")
    await target.handle_callback(who, INTAKE_PICK_SERVICE, {"service": "repair"})
    await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    reply = await target.handle_text(who, "-")
    # address pack field auto-skipped (filled by hardcoded step),
    # so "-" here skips the optional "asset" field → asks for "fault" (required)
    assert reply.text
    confirmation = await target.handle_text(who, "Течёт кран")
    assert "отправ" in confirmation.text.lower()
    assert {b.action for b in confirmation.buttons} == {
        INTAKE_CONFIRM,
        INTAKE_CANCEL,
    }
    done = await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert "отправлена" in done.text.lower()
    assert "org-1:telegram:7001" not in sessions.store
    assert len(service.orders) == 1
    order = service.orders[0]
    assert order["organization_id"] == "org-1"
    assert order["work_type"] == "repair"
    assert order["requester_id"] == "telegram:7001"
    assert order["details"]["phone"] == "+7 999 123 4567"
    assert order["details"]["address"] == "Ленина 1"
    assert "asset" not in order["details"]
    assert order["details"]["service_keys"] == ["repair"]
    assert order["evidence_requirements"].minimum_photos == 1


@pytest.mark.asyncio
async def test_cancel_clears_session() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    await target.start(who)
    reply = await target.handle_callback(who, INTAKE_CANCEL, {})
    assert "отменена" in reply.text.lower()
    assert "org-1:telegram:7001" not in sessions.store


@pytest.mark.asyncio
async def test_without_active_pack_reports_not_configured() -> None:
    target = IntakeCoordinator(
        packs=FakePacks(None),
        sessions=FakeSessions(),
        service=FakeService(),
    )
    reply = await target.start(identity())
    assert "не настроен" in reply.text.lower()


@pytest.mark.asyncio
async def test_confirm_rejected_when_in_services_step() -> None:
    target, _, _ = coordinator()
    who = identity()
    await _go_to_services(target, who)
    reply = await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert "сначала" in reply.text.lower()


@pytest.mark.asyncio
async def test_services_done_rejected_when_in_fields_step() -> None:
    target, _, _ = coordinator()
    who = identity()
    await _go_to_services(target, who)
    await target.handle_callback(who, INTAKE_PICK_SERVICE, {"service": "repair"})
    await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    state = target._sessions.store.get("org-1:telegram:7001", {})
    assert state.get("step") == "fields"
    reply = await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    assert "услуги" in reply.text.lower()


@pytest.mark.asyncio
async def test_confirm_clears_session_on_success() -> None:
    target, sessions, service = coordinator()
    who = identity()
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    await target.handle_text(who, "ул. Мира 5")
    await target.handle_callback(who, INTAKE_PICK_SERVICE, {"service": "repair"})
    await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    await target.handle_text(who, "-")  # skips optional "asset"
    await target.handle_text(who, "Течёт труба")
    done = await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert "отправлена" in done.text.lower()
    assert "org-1:telegram:7001" not in sessions.store
    assert len(service.orders) == 1


@pytest.mark.asyncio
async def test_text_truncated_to_500_chars() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    await target.start(who)
    long_phone = "1" * 600
    await target.handle_text(who, long_phone)
    state = sessions.store.get("org-1:telegram:7001", {})
    values = state.get("field_values", {})
    assert len(values.get("phone", "")) == 500


@pytest.mark.asyncio
async def test_address_validates_min_length() -> None:
    target, _, _ = coordinator()
    who = identity()
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    reply = await target.handle_text(who, "abc")
    assert "адрес" in reply.text.lower() or "минимум" in reply.text.lower()
