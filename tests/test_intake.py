from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.infrastructure.pack_store import PackRevision
from dispatch_core.messaging.intake import (
    INTAKE_CANCEL,
    INTAKE_CONFIRM,
    INTAKE_CONTINUE_AFTER_MAP,
    INTAKE_FIELD_CHOICE,
    INTAKE_PICK_SERVICE,
    INTAKE_REQUEST_LOCATION,
    INTAKE_SERVICES_DONE,
    INTAKE_TYPE_ADDRESS,
    IntakeCoordinator,
)
from dispatch_core.messaging.models import Provider
from dispatch_core.packs.catalog import (
    FieldDefinition,
    FieldType,
    PackDefinition,
    ServiceCatalog,
    ServiceCategory,
    seed_definition,
)


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
    version: int = 1
    revisions: dict[int, PackDefinition] = field(default_factory=dict)

    async def active(self, organization_id: str) -> PackDefinition | None:
        return self.pack

    async def active_revision(self, organization_id: str) -> PackRevision | None:
        if self.pack is None:
            return None
        self.revisions.setdefault(self.version, self.pack)
        return PackRevision(self.version, "active", self.pack)

    async def revision(self, organization_id: str, version: int) -> PackRevision | None:
        definition = self.revisions.get(version)
        if definition is None:
            return None
        state = "active" if version == self.version else "archived"
        return PackRevision(version, state, definition)


@dataclass
class FakeSessions:
    store: dict[str, dict[str, Any]] = field(default_factory=dict)
    clear_failures: int = 0

    async def get(self, organization_id: str, actor_id: str) -> dict[str, Any] | None:
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
        if self.clear_failures:
            self.clear_failures -= 1
            raise RuntimeError("session storage unavailable")
        self.store.pop(f"{organization_id}:{actor_id}", None)


@dataclass
class FakeService:
    orders: list[dict[str, Any]] = field(default_factory=list)
    result: object | None = None
    failures: int = 0

    async def create_order_once(self, **values: Any) -> object | None:
        self.orders.append(values)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("order storage unavailable")
        return self.result


def coordinator(
    pack: PackDefinition | None = None,
    *,
    public_base_url: str | None = None,
) -> tuple[IntakeCoordinator, FakeSessions, FakeService]:
    sessions = FakeSessions()
    service = FakeService()
    target = IntakeCoordinator(
        packs=FakePacks(pack or seed_definition("field_service")),
        sessions=sessions,
        service=service,
        public_base_url=public_base_url,
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
    assert state["pack_version"] == 1
    assert len(state["submission_id"]) == 36


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
async def test_address_card_warns_about_vpn_and_offers_all_methods() -> None:
    target, sessions, _ = coordinator(public_base_url="https://dispatch.example")
    who = identity()
    await target.start(who)

    reply = await target.handle_text(who, "+7 999 123 4567")

    assert "vpn" in reply.text.lower()
    assert "gps" in reply.text.lower()
    actions = {button.action for button in reply.buttons}
    assert {INTAKE_REQUEST_LOCATION, INTAKE_TYPE_ADDRESS} <= actions
    map_button = next(button for button in reply.buttons if button.url)
    assert map_button.url.startswith("https://dispatch.example/address#")
    assert "?" not in map_button.url
    assert len(map_button.url.rsplit("#", maxsplit=1)[1]) >= 43
    state = sessions.store["org-1:telegram:7001"]
    assert state["address_token"] not in reply.text


@pytest.mark.asyncio
async def test_native_location_advances_to_services_and_is_saved() -> None:
    target, sessions, _ = coordinator(public_base_url="https://dispatch.example")
    who = identity()
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    request = await target.handle_callback(who, INTAKE_REQUEST_LOCATION, {})
    assert len(request.buttons) == 1
    assert request.buttons[0].request_location

    reply = await target.handle_location(
        who,
        latitude=53.75,
        longitude=87.1,
        method="telegram",
    )

    assert INTAKE_PICK_SERVICE in {button.action for button in reply.buttons}
    state = sessions.store["org-1:telegram:7001"]
    assert state["step"] == "services"
    assert state["service_location"] == {
        "latitude": 53.75,
        "longitude": 87.1,
        "method": "telegram",
    }
    assert state["field_values"]["address"].startswith("Точка на карте")
    assert "address_token" not in state


@pytest.mark.asyncio
async def test_address_callbacks_reject_wrong_step_and_continue_after_map() -> None:
    target, sessions, _ = coordinator(public_base_url="https://dispatch.example")
    who = identity()
    wrong = await target.handle_callback(who, INTAKE_REQUEST_LOCATION, {})
    assert "телефон" in wrong.text.lower()
    await target.handle_text(who, "+7 999 123 4567")
    typed = await target.handle_callback(who, INTAKE_TYPE_ADDRESS, {})
    assert "город" in typed.text.lower()
    waiting = await target.handle_callback(who, INTAKE_CONTINUE_AFTER_MAP, {})
    assert "сначала" in waiting.text.lower()
    sessions.store["org-1:telegram:7001"]["step"] = "services"
    resumed = await target.handle_callback(who, INTAKE_CONTINUE_AFTER_MAP, {})
    assert INTAKE_PICK_SERVICE in {button.action for button in resumed.buttons}


@pytest.mark.asyncio
async def test_invalid_native_location_does_not_advance() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    await target.start(who)
    await target.handle_text(who, "+7 999 123 4567")
    reply = await target.handle_location(
        who,
        latitude=91,
        longitude=87.1,
    )
    assert "не удалось" in reply.text.lower()
    assert sessions.store["org-1:telegram:7001"]["step"] == "address"


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
async def test_confirm_failure_preserves_session_and_stable_submission_id() -> None:
    target, sessions, service = coordinator()
    who = identity()
    key = "org-1:telegram:7001"
    await target.start(who)
    submission_id = sessions.store[key]["submission_id"]
    sessions.store[key].update(
        {
            "step": "confirm",
            "selected": ["repair"],
            "field_values": {
                "phone": "79990000000",
                "address": "Ленина 1",
                "fault": "Течь",
            },
        }
    )
    service.failures = 1

    with pytest.raises(RuntimeError, match="order storage"):
        await target.handle_callback(who, INTAKE_CONFIRM, {})

    assert sessions.store[key]["step"] == "confirm"
    assert sessions.store[key]["submission_id"] == submission_id
    await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert [item["order_id"] for item in service.orders] == [
        submission_id,
        submission_id,
    ]
    assert key not in sessions.store


@pytest.mark.asyncio
async def test_confirm_retry_after_clear_failure_reuses_submission_id() -> None:
    target, sessions, service = coordinator()
    who = identity()
    key = "org-1:telegram:7001"
    await target.start(who)
    submission_id = sessions.store[key]["submission_id"]
    sessions.store[key].update(
        {
            "step": "confirm",
            "selected": ["repair"],
            "field_values": {
                "phone": "79990000000",
                "address": "Ленина 1",
                "fault": "Течь",
            },
        }
    )
    sessions.clear_failures = 1

    with pytest.raises(RuntimeError, match="session storage"):
        await target.handle_callback(who, INTAKE_CONFIRM, {})

    assert sessions.store[key]["submission_id"] == submission_id
    await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert [item["order_id"] for item in service.orders] == [
        submission_id,
        submission_id,
    ]
    assert key not in sessions.store


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


@pytest.mark.asyncio
async def test_text_without_session_restarts_and_confirm_text_shows_buttons() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    restarted = await target.handle_text(who, "hello")
    assert "телефон" in restarted.text.lower()
    sessions.store["org-1:telegram:7001"] = {
        "step": "confirm",
        "selected": ["repair"],
        "field_values": {},
    }
    confirmation = await target.handle_text(who, "ignored")
    assert {button.action for button in confirmation.buttons} == {
        INTAKE_CONFIRM,
        INTAKE_CANCEL,
    }


@pytest.mark.asyncio
async def test_active_intake_keeps_pack_revision_after_publish() -> None:
    original = seed_definition("field_service")
    packs = FakePacks(original)
    sessions = FakeSessions()
    target = IntakeCoordinator(packs=packs, sessions=sessions, service=FakeService())
    who = identity()
    await target.start(who)
    packs.version = 2
    packs.pack = replace(
        original,
        service_catalog=ServiceCatalog(
            categories=(ServiceCategory("new", "Новая услуга"),),
        ),
    )

    await target.handle_text(who, "79990000000")
    reply = await target.handle_text(who, "Ленина 10")

    labels = {button.text for button in reply.buttons}
    assert original.service_catalog.categories[0].label in labels
    assert "Новая услуга" not in labels
    assert sessions.store["org-1:telegram:7001"]["pack_version"] == 1


@pytest.mark.asyncio
async def test_callbacks_without_session_and_unknown_action_are_safe() -> None:
    target, _, _ = coordinator()
    who = identity()
    restarted = await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert "телефон" in restarted.text.lower()
    await target.start(who)
    wrong_step = await target.handle_callback(
        who, INTAKE_PICK_SERVICE, {"service": "repair"}
    )
    unknown = await target.handle_callback(who, "unknown", {})
    assert "сначала" in wrong_step.text.lower()
    assert "неизвест" in unknown.text.lower()


@pytest.mark.asyncio
async def test_unknown_service_and_toggle_off_are_handled() -> None:
    target, _, _ = coordinator()
    who = identity()
    await _go_to_services(target, who)
    missing = await target.handle_callback(
        who, INTAKE_PICK_SERVICE, {"service": "missing"}
    )
    await target.handle_callback(who, INTAKE_PICK_SERVICE, {"service": "repair"})
    toggled_off = await target.handle_callback(
        who, INTAKE_PICK_SERVICE, {"service": "repair"}
    )
    assert "недоступна" in missing.text.lower()
    assert not any("✅" in button.text for button in toggled_off.buttons)


@pytest.mark.asyncio
async def test_single_select_service_moves_directly_to_fields() -> None:
    base = seed_definition("field_service")
    single = replace(
        base,
        service_catalog=ServiceCatalog(
            categories=base.service_catalog.categories,
            multi_select=False,
        ),
    )
    target, sessions, _ = coordinator(single)
    who = identity()
    await _go_to_services(target, who)
    reply = await target.handle_callback(
        who, INTAKE_PICK_SERVICE, {"service": "repair"}
    )
    assert reply.text
    assert sessions.store["org-1:telegram:7001"]["step"] == "fields"


@pytest.mark.asyncio
async def test_enum_field_is_rendered_as_buttons_and_validated() -> None:
    base = seed_definition("field_service")
    enum_pack = replace(
        base,
        fields=(
            FieldDefinition(
                "address",
                "Адрес",
                FieldType.ADDRESS,
                required=True,
                order=1,
            ),
            FieldDefinition(
                "urgency",
                "Срочность",
                FieldType.ENUM,
                required=True,
                choices=("Обычная", "Срочная"),
                prompt="Выберите срочность",
                order=2,
            ),
        ),
    )
    target, sessions, _ = coordinator(enum_pack)
    who = identity()
    await _go_to_services(target, who)
    await target.handle_callback(who, INTAKE_PICK_SERVICE, {"service": "repair"})
    reply = await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    assert {button.action for button in reply.buttons} == {
        INTAKE_FIELD_CHOICE,
        INTAKE_CANCEL,
    }

    invalid = await target.handle_text(who, "Когда-нибудь")
    assert "предложенных" in invalid.text.lower()
    reply = await target.handle_callback(
        who,
        INTAKE_FIELD_CHOICE,
        {"value": "Срочная"},
    )
    assert INTAKE_CONFIRM in {button.action for button in reply.buttons}
    assert sessions.store["org-1:telegram:7001"]["field_values"]["urgency"] == (
        "Срочная"
    )


@pytest.mark.asyncio
async def test_field_state_recovers_from_invalid_indexes_and_required_blank() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    key = "org-1:telegram:7001"
    sessions.store[key] = {
        "step": "fields",
        "selected": ["repair"],
        "field_values": {"phone": "79990000000", "address": "Ленина 1"},
        "field_index": "bad",
    }
    malformed = await target.handle_text(who, "anything")
    assert "подтверд" in malformed.text.lower()

    sessions.store[key] = {
        "step": "fields",
        "selected": ["repair"],
        "field_values": {"phone": "79990000000", "address": "Ленина 1"},
        "field_index": 999,
    }
    out_of_range = await target.handle_text(who, "anything")
    assert "подтверд" in out_of_range.text.lower()

    sessions.store[key] = {
        "step": "fields",
        "selected": ["repair"],
        "field_values": {"phone": "79990000000", "address": "Ленина 1"},
        "field_index": 2,
    }
    required = await target.handle_text(who, "-")
    assert "обязательно" in required.text.lower()


@pytest.mark.asyncio
async def test_no_fields_and_already_filled_fields_go_to_confirmation() -> None:
    base = seed_definition("field_service")
    no_fields = replace(base, fields=())
    target, sessions, _ = coordinator(no_fields)
    who = identity()
    key = "org-1:telegram:7001"
    sessions.store[key] = {
        "step": "services",
        "selected": ["repair"],
        "field_values": {},
    }
    empty_fields = await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    assert "подтверд" in empty_fields.text.lower()

    target, sessions, _ = coordinator(base)
    sessions.store[key] = {
        "step": "services",
        "selected": ["repair"],
        "field_values": {
            "phone": "79990000000",
            "address": "Ленина 1",
            "asset": "A",
            "fault": "B",
        },
    }
    filled = await target.handle_callback(who, INTAKE_SERVICES_DONE, {})
    assert "подтверд" in filled.text.lower()


@pytest.mark.asyncio
async def test_confirm_without_service_restarts_intake() -> None:
    target, sessions, _ = coordinator()
    who = identity()
    sessions.store["org-1:telegram:7001"] = {
        "step": "confirm",
        "selected": [],
        "field_values": {},
    }
    reply = await target.handle_callback(who, INTAKE_CONFIRM, {})
    assert "телефон" in reply.text.lower()
