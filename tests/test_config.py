from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import NotFound
from dispatch_core.domain.work_order import EvidenceRequirements
from dispatch_core.messaging.config import (
    CONFIG_ADD_OPERATOR,
    CONFIG_BRAND,
    CONFIG_CANCEL,
    CONFIG_DEL_OPERATOR,
    CONFIG_DEL_OPERATOR_CONFIRM,
    CONFIG_EVIDENCE,
    CONFIG_EVIDENCE_CODE,
    CONFIG_EVIDENCE_COMMENT,
    CONFIG_EVIDENCE_PHOTO_DEC,
    CONFIG_EVIDENCE_PHOTO_INC,
    CONFIG_EVIDENCE_SIGNATURE,
    CONFIG_FIELD_ADD,
    CONFIG_FIELD_DELETE,
    CONFIG_FIELD_REQUIRED,
    CONFIG_FIELD_TYPE,
    CONFIG_FIELDS,
    CONFIG_LIST_OPERATORS,
    CONFIG_MENU,
    CONFIG_OWNER_NANO,
    CONFIG_OWNER_ROLES,
    CONFIG_PUBLISH,
    CONFIG_SERVICE_ADD,
    CONFIG_SERVICE_DELETE,
    CONFIG_SERVICES,
    ConfigCoordinator,
)
from dispatch_core.messaging.models import Provider
from dispatch_core.packs.catalog import (
    Branding,
    FieldDefinition,
    FieldType,
    PackDefinition,
    ServiceCatalog,
    ServiceCategory,
    blank_definition,
)


def admin() -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="admin:100",
        role="admin",
        display_name="Admin",
        provider=Provider.TELEGRAM,
        external_user_id="100",
    )


def _complete_pack() -> PackDefinition:
    return PackDefinition(
        branding=Branding(name="TestCo", greeting="Hi!", support="8-800"),
        service_catalog=ServiceCatalog(
            categories=(
                ServiceCategory("repair", "Ремонт"),
                ServiceCategory("install", "Установка"),
            ),
        ),
        fields=(
            FieldDefinition(
                "address",
                "Адрес",
                FieldType.ADDRESS,
                required=True,
                prompt="Введите адрес",
                order=1,
            ),
            FieldDefinition(
                "fault",
                "Неисправность",
                FieldType.TEXT,
                required=False,
                order=2,
            ),
        ),
        evidence=EvidenceRequirements(
            minimum_photos=1,
            comment_required=True,
            signature_required=False,
            customer_code_required=False,
        ),
    )


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class FakePackStore:
    _draft: PackDefinition | None = None
    _active: PackDefinition | None = None
    _published_version: int | None = None
    _publish_error: Exception | None = None
    update_failures: int = 0

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
        if self.update_failures:
            self.update_failures -= 1
            raise RuntimeError("pack storage unavailable")
        self._draft = definition

    async def publish_draft(self, organization_id: str) -> int:
        if self._publish_error is not None:
            raise self._publish_error
        if self._draft is None:
            raise NotFound("no draft")
        self._active = self._draft
        self._draft = None
        self._published_version = (self._published_version or 0) + 1
        return self._published_version


@dataclass
class FakeSessionStore:
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
class FakeIdentityStore:
    actors: dict[str, dict[str, Any]] = field(default_factory=dict)
    next_id: int = 1
    creation_requests: dict[str, str] = field(default_factory=dict)

    async def create_staff_actor(
        self,
        *,
        organization_id: str,
        role: str,
        name: str,
        phone: str | None = None,
        provider: Provider | None = None,
        external_user_id: str | None = None,
        request_key: str | None = None,
    ) -> dict[str, Any]:
        if request_key and request_key in self.creation_requests:
            return self.actors[self.creation_requests[request_key]]
        actor_id = f"operator:{self.next_id}"
        self.next_id += 1
        actor = {
            "actor_id": actor_id,
            "role": role,
            "name": name,
            "phone": phone,
            "bind_code": "1234",
            "display_name": name,
            "active": True,
            "has_bind_code": True,
            "channels": [],
            "roles": [role],
        }
        self.actors[actor_id] = actor
        if request_key:
            self.creation_requests[request_key] = actor_id
        return actor

    async def list_actors(
        self, *, organization_id: str, role: str
    ) -> list[dict[str, Any]]:
        return [
            {**actor, "id": actor_id}
            for actor_id, actor in self.actors.items()
            if role in actor["roles"] and actor["active"]
        ]

    async def get_actor(
        self, *, organization_id: str, actor_id: str
    ) -> dict[str, Any] | None:
        actor = self.actors.get(actor_id)
        return {**actor, "id": actor_id} if actor is not None else None

    async def delete_actor(self, *, organization_id: str, actor_id: str) -> bool:
        return self.actors.pop(actor_id, None) is not None

    async def revoke_role(
        self, *, organization_id: str, actor_id: str, role: str
    ) -> bool:
        actor = self.actors.get(actor_id)
        if actor is None or role not in actor["roles"]:
            return False
        actor["roles"].remove(role)
        if not actor["roles"]:
            self.actors.pop(actor_id)
        return True

    async def grant_role(
        self, *, organization_id: str, actor_id: str, role: str
    ) -> bool:
        actor = self.actors.get(actor_id)
        if actor is None:
            return False
        if role not in actor["roles"]:
            actor["roles"].append(role)
        return True


def coordinator(
    *,
    draft: PackDefinition | None = None,
    active: PackDefinition | None = None,
    publish_error: Exception | None = None,
    identities: FakeIdentityStore | None = None,
) -> tuple[ConfigCoordinator, FakePackStore, FakeSessionStore]:
    packs = FakePackStore(_draft=draft, _active=active, _publish_error=publish_error)
    sessions = FakeSessionStore()
    target = ConfigCoordinator(
        packs=packs,
        sessions=sessions,  # type: ignore[arg-type]
        identities=identities,  # type: ignore[arg-type]
    )
    return target, packs, sessions


# ---------------------------------------------------------------------------
# Menu
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_shows_menu_with_no_pack() -> None:
    target, _, _ = coordinator()
    reply = await target.start(admin())
    assert "конфигуратор" in reply.text.lower()
    assert "не создан" in reply.text.lower()
    actions = {b.action for b in reply.buttons}
    assert CONFIG_BRAND in actions
    assert CONFIG_SERVICES in actions
    assert CONFIG_FIELDS in actions
    assert CONFIG_EVIDENCE in actions
    assert CONFIG_PUBLISH in actions
    assert CONFIG_LIST_OPERATORS in actions


@pytest.mark.asyncio
async def test_admin_creates_lists_and_deletes_operator() -> None:
    identities = FakeIdentityStore()
    target, _, _ = coordinator(identities=identities)
    who = admin()

    reply = await target.handle_callback(who, CONFIG_ADD_OPERATOR, {})
    assert "имя оператора" in reply.text.lower()

    reply = await target.handle_text(who, "Анна")
    assert "код привязки: 1234" in reply.text.lower()

    reply = await target.handle_callback(who, CONFIG_LIST_OPERATORS, {})
    assert "Анна" in reply.text
    delete = next(
        button for button in reply.buttons if button.action == CONFIG_DEL_OPERATOR
    )

    reply = await target.handle_callback(who, delete.action, delete.payload)
    confirm = next(
        button
        for button in reply.buttons
        if button.action == CONFIG_DEL_OPERATOR_CONFIRM
    )
    reply = await target.handle_callback(who, confirm.action, confirm.payload)

    assert "операторов пока нет" in reply.text.lower()
    assert identities.actors == {}


@pytest.mark.asyncio
async def test_operator_creation_retry_does_not_duplicate_actor() -> None:
    identities = FakeIdentityStore()
    target, _, sessions = coordinator(identities=identities)
    who = admin()
    await target.handle_callback(who, CONFIG_ADD_OPERATOR, {})
    request_key = sessions.store["org-1:admin:100"]["request_key"]
    sessions.clear_failures = 1

    with pytest.raises(RuntimeError, match="session storage"):
        await target.handle_text(who, "Анна")

    assert len(identities.actors) == 1
    assert sessions.store["org-1:admin:100"]["request_key"] == request_key
    reply = await target.handle_text(who, "Анна")
    assert "код привязки: 1234" in reply.text.lower()
    assert len(identities.actors) == 1


@pytest.mark.asyncio
async def test_admin_can_enable_explicit_nano_role_preset() -> None:
    identities = FakeIdentityStore(
        actors={
            "admin:100": {
                "actor_id": "admin:100",
                "role": "admin",
                "roles": ["admin"],
                "name": "Admin",
                "display_name": "Admin",
                "phone": None,
                "bind_code": None,
                "active": True,
                "has_bind_code": False,
                "channels": ["telegram"],
            }
        }
    )
    target, _, _ = coordinator(identities=identities)
    who = admin()

    reply = await target.handle_callback(who, CONFIG_OWNER_ROLES, {})
    assert "администратор" in reply.text.lower()
    assert CONFIG_OWNER_NANO in {button.action for button in reply.buttons}

    reply = await target.handle_callback(who, CONFIG_OWNER_NANO, {})
    assert "нано-режим включён" in reply.text.lower()
    assert set(identities.actors["admin:100"]["roles"]) == {
        "admin",
        "operator",
        "master",
    }


@pytest.mark.asyncio
async def test_menu_shows_draft_state() -> None:
    target, _, _ = coordinator(draft=_complete_pack())
    reply = await target.start(admin())
    assert "черновик" in reply.text.lower()
    assert "TestCo" in reply.text


@pytest.mark.asyncio
async def test_menu_shows_active_state() -> None:
    target, _, _ = coordinator(active=_complete_pack())
    reply = await target.start(admin())
    assert "активный" in reply.text.lower()


# ---------------------------------------------------------------------------
# Text / command routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_text_without_flow_shows_menu() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "hello")
    assert "кнопкой" in reply.text.lower()


@pytest.mark.asyncio
async def test_command_routes_to_section() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "/brand")
    assert "название бренда" in reply.text.lower()


@pytest.mark.asyncio
async def test_start_command_shows_menu() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "/start")
    assert "конфигуратор" in reply.text.lower()


@pytest.mark.asyncio
async def test_unknown_command_shows_menu() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "/unknown")
    assert "неизвестная команда" in reply.text.lower()


@pytest.mark.asyncio
async def test_text_during_active_flow_is_consumed() -> None:
    target, _, sessions = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/brand")
    assert sessions.store["org-1:admin:100"]["flow"] == "brand"
    reply = await target.handle_text(admin(), "MyBrand")
    assert "приветствие" in reply.text.lower()


@pytest.mark.asyncio
async def test_command_during_flow_clears_session() -> None:
    target, _, sessions = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/brand")
    assert "org-1:admin:100" in sessions.store
    reply = await target.handle_text(admin(), "/services")
    assert "org-1:admin:100" not in sessions.store
    assert "услуги" in reply.text.lower()


# ---------------------------------------------------------------------------
# Brand flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_brand_full_flow() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/brand")
    reply = await target.handle_text(admin(), "Acme")
    assert "приветствие" in reply.text.lower()
    assert packs._draft is not None
    assert packs._draft.branding.name == "Acme"

    reply = await target.handle_text(admin(), "Welcome!")
    assert "контакт" in reply.text.lower()
    assert packs._draft.branding.greeting == "Welcome!"

    reply = await target.handle_text(admin(), "8-800")
    assert "обновлён" in reply.text.lower()
    assert packs._draft.branding.support == "8-800"
    assert "org-1:admin:100" not in sessions.store


@pytest.mark.asyncio
async def test_brand_skip_support_with_dash() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/brand")
    await target.handle_text(admin(), "Acme")
    await target.handle_text(admin(), "Hello")
    await target.handle_text(admin(), "-")
    assert packs._draft.branding.support == ""
    assert "org-1:admin:100" not in sessions.store


@pytest.mark.asyncio
async def test_brand_empty_name_rejected() -> None:
    target, _, sessions = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/brand")
    reply = await target.handle_text(admin(), "")
    assert "пустым" in reply.text.lower()
    assert sessions.store["org-1:admin:100"]["step"] == "name"


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_services_empty() -> None:
    target, _, _ = coordinator(draft=blank_definition())
    reply = await target.handle_text(admin(), "/services")
    assert "пока пусто" in reply.text.lower()


@pytest.mark.asyncio
async def test_services_add_and_delete() -> None:
    target, packs, _ = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/services")

    await target.handle_callback(admin(), CONFIG_SERVICE_ADD, {})
    reply = await target.handle_text(admin(), "Ремонт")
    assert "Ремонт" in reply.text
    assert packs._draft is not None
    assert len(packs._draft.service_catalog.categories) == 1
    key = packs._draft.service_catalog.categories[0].key

    reply = await target.handle_callback(admin(), CONFIG_SERVICE_DELETE, {"key": key})
    assert len(packs._draft.service_catalog.categories) == 0


@pytest.mark.asyncio
async def test_services_empty_label_rejected() -> None:
    target, _, sessions = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/services")
    await target.handle_callback(admin(), CONFIG_SERVICE_ADD, {})
    reply = await target.handle_text(admin(), "")
    assert "пустым" in reply.text.lower()
    assert sessions.store["org-1:admin:100"]["step"] == "label"


@pytest.mark.asyncio
async def test_service_save_failure_preserves_flow_for_inbox_retry() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_callback(admin(), CONFIG_SERVICE_ADD, {})
    packs.update_failures = 1

    with pytest.raises(RuntimeError, match="pack storage"):
        await target.handle_text(admin(), "Ремонт")

    assert sessions.store["org-1:admin:100"]["flow"] == "service_add"
    await target.handle_text(admin(), "Ремонт")
    assert packs._draft is not None
    assert [item.label for item in packs._draft.service_catalog.categories] == [
        "Ремонт"
    ]


@pytest.mark.asyncio
async def test_service_clear_failure_retry_does_not_duplicate_item() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_callback(admin(), CONFIG_SERVICE_ADD, {})
    sessions.clear_failures = 1

    with pytest.raises(RuntimeError, match="session storage"):
        await target.handle_text(admin(), "Ремонт")

    assert sessions.store["org-1:admin:100"]["flow"] == "service_add"
    await target.handle_text(admin(), "Ремонт")
    assert packs._draft is not None
    assert [item.label for item in packs._draft.service_catalog.categories] == [
        "Ремонт"
    ]


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fields_empty() -> None:
    target, _, _ = coordinator(draft=blank_definition())
    reply = await target.handle_text(admin(), "/fields")
    assert "пока пусто" in reply.text.lower()


@pytest.mark.asyncio
async def test_fields_add_full_flow() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_text(admin(), "/fields")

    await target.handle_callback(admin(), CONFIG_FIELD_ADD, {})
    reply = await target.handle_text(admin(), "Телефон")
    assert "тип поля" in reply.text.lower()
    assert sessions.store["org-1:admin:100"]["scratch"]["label"] == "Телефон"

    reply = await target.handle_callback(admin(), CONFIG_FIELD_TYPE, {"type": "text"})
    assert "обязательн" in reply.text.lower()

    reply = await target.handle_callback(
        admin(), CONFIG_FIELD_REQUIRED, {"required": True}
    )
    assert "Телефон" in reply.text
    assert packs._draft is not None
    assert len(packs._draft.fields) == 1
    assert packs._draft.fields[0].key == "field"
    assert packs._draft.fields[0].required is True
    assert "org-1:admin:100" not in sessions.store


@pytest.mark.asyncio
async def test_fields_delete() -> None:
    pack = PackDefinition(
        branding=Branding(name="X"),
        service_catalog=ServiceCatalog(categories=(ServiceCategory("a", "A"),)),
        fields=(FieldDefinition("phone", "Телефон", FieldType.TEXT),),
        evidence=EvidenceRequirements(),
    )
    target, packs, _ = coordinator(draft=pack)
    await target.handle_text(admin(), "/fields")
    reply = await target.handle_callback(admin(), CONFIG_FIELD_DELETE, {"key": "phone"})
    assert "пока пусто" in reply.text.lower()
    assert len(packs._draft.fields) == 0


@pytest.mark.asyncio
async def test_field_type_with_no_session_returns_fields() -> None:
    target, _, _ = coordinator(draft=blank_definition())
    reply = await target.handle_callback(admin(), CONFIG_FIELD_TYPE, {"type": "text"})
    assert "пол" in reply.text.lower()


@pytest.mark.asyncio
async def test_field_required_with_no_session_returns_fields() -> None:
    target, _, _ = coordinator(draft=blank_definition())
    reply = await target.handle_callback(
        admin(), CONFIG_FIELD_REQUIRED, {"required": True}
    )
    assert "пол" in reply.text.lower()


@pytest.mark.asyncio
async def test_field_clear_failure_retry_does_not_duplicate_item() -> None:
    target, packs, sessions = coordinator(draft=blank_definition())
    await target.handle_callback(admin(), CONFIG_FIELD_ADD, {})
    await target.handle_text(admin(), "Телефон")
    await target.handle_callback(admin(), CONFIG_FIELD_TYPE, {"type": "text"})
    sessions.clear_failures = 1

    with pytest.raises(RuntimeError, match="session storage"):
        await target.handle_callback(admin(), CONFIG_FIELD_REQUIRED, {"required": True})

    await target.handle_callback(admin(), CONFIG_FIELD_REQUIRED, {"required": True})
    assert packs._draft is not None
    assert [item.label for item in packs._draft.fields] == ["Телефон"]


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_evidence_shows_current_values() -> None:
    target, _, _ = coordinator(draft=_complete_pack())
    reply = await target.handle_text(admin(), "/evidence")
    assert "фото: минимум 1" in reply.text.lower()
    assert "комментарий: да" in reply.text.lower()
    assert "подпись: нет" in reply.text.lower()
    assert "код клиента: нет" in reply.text.lower()


@pytest.mark.asyncio
async def test_evidence_photo_increment() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_PHOTO_INC, {})
    assert packs._draft.evidence.minimum_photos == 2


@pytest.mark.asyncio
async def test_evidence_photo_decrement_floor() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    packs._draft = _complete_pack()
    # set to 0 first
    packs._draft = PackDefinition(
        branding=packs._draft.branding,
        service_catalog=packs._draft.service_catalog,
        fields=packs._draft.fields,
        evidence=EvidenceRequirements(minimum_photos=0),
    )
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_PHOTO_DEC, {})
    assert packs._draft.evidence.minimum_photos == 0  # floor at 0


@pytest.mark.asyncio
async def test_evidence_photo_increment_ceiling() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    packs._draft = PackDefinition(
        branding=packs._draft.branding,
        service_catalog=packs._draft.service_catalog,
        fields=packs._draft.fields,
        evidence=EvidenceRequirements(minimum_photos=10),
    )
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_PHOTO_INC, {})
    assert packs._draft.evidence.minimum_photos == 10  # ceiling at 10


@pytest.mark.asyncio
async def test_evidence_toggle_comment() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_COMMENT, {})
    assert packs._draft.evidence.comment_required is False
    await target.handle_callback(admin(), CONFIG_EVIDENCE_COMMENT, {})
    assert packs._draft.evidence.comment_required is True


@pytest.mark.asyncio
async def test_evidence_toggle_signature() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_SIGNATURE, {})
    assert packs._draft.evidence.signature_required is True


@pytest.mark.asyncio
async def test_evidence_toggle_code() -> None:
    target, packs, _ = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/evidence")
    await target.handle_callback(admin(), CONFIG_EVIDENCE_CODE, {})
    assert packs._draft.evidence.customer_code_required is True


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_uses_draft() -> None:
    target, _, _ = coordinator(draft=_complete_pack())
    reply = await target.handle_text(admin(), "/preview")
    assert "предпросмотр" in reply.text.lower()
    assert "Hi!" in reply.text
    assert "Ремонт" in reply.text


@pytest.mark.asyncio
async def test_preview_falls_back_to_active() -> None:
    target, _, _ = coordinator(active=_complete_pack())
    reply = await target.handle_text(admin(), "/preview")
    assert "Hi!" in reply.text
    assert "Ремонт" in reply.text


@pytest.mark.asyncio
async def test_preview_empty_returns_menu() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "/preview")
    assert "черновик пуст" in reply.text.lower()


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_success() -> None:
    target, _, _ = coordinator(draft=_complete_pack())
    reply = await target.handle_text(admin(), "/publish")
    assert "готово" in reply.text.lower()
    assert "версия 1" in reply.text.lower()


@pytest.mark.asyncio
async def test_publish_no_draft() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_text(admin(), "/publish")
    assert "нет черновика" in reply.text.lower()


@pytest.mark.asyncio
async def test_publish_validation_error() -> None:
    from dispatch_core.infrastructure.pack_store import PackValidationError

    target, _, _ = coordinator(
        draft=blank_definition(),
        publish_error=PackValidationError("нужно название бренда"),
    )
    reply = await target.handle_text(admin(), "/publish")
    assert "не удалось" in reply.text.lower()
    assert "название бренда" in reply.text


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_clears_session() -> None:
    target, _, sessions = coordinator(draft=_complete_pack())
    await target.handle_text(admin(), "/brand")
    assert "org-1:admin:100" in sessions.store
    reply = await target.handle_callback(admin(), CONFIG_CANCEL, {})
    assert "отменено" in reply.text.lower()
    assert "org-1:admin:100" not in sessions.store


@pytest.mark.asyncio
async def test_cancel_unknown_action_shows_menu() -> None:
    target, _, _ = coordinator()
    reply = await target.handle_callback(admin(), "unknown_action", {})
    assert "неизвестное действие" in reply.text.lower()


# ---------------------------------------------------------------------------
# Menu callback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_menu_callback_returns_menu() -> None:
    target, _, _ = coordinator(draft=_complete_pack())
    reply = await target.handle_callback(admin(), CONFIG_MENU, {})
    assert "конфигуратор" in reply.text.lower()
    assert "TestCo" in reply.text


# ---------------------------------------------------------------------------
# _slug helper
# ---------------------------------------------------------------------------


def test_slug_normalizes_label() -> None:
    from dispatch_core.messaging.config import _slug

    assert _slug("repair pipes", "service", set()) == "repair_pipes"


def test_slug_deduplicates() -> None:
    from dispatch_core.messaging.config import _slug

    result = _slug("repair", "field", {"repair"})
    assert result == "repair_2"


def test_slug_fallback_for_empty_label() -> None:
    from dispatch_core.messaging.config import _slug

    result = _slug("!!!", "field", set())
    assert result == "field"
