from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Coroutine
from typing import Any

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import NotFound
from dispatch_core.domain.work_order import EvidenceRequirements
from dispatch_core.infrastructure.pack_store import (
    PackValidationError,
    PostgresPackStore,
)
from dispatch_core.infrastructure.workflow_store import PostgresConfigSessionStore
from dispatch_core.messaging.cards import CardRenderer
from dispatch_core.messaging.replies import Reply, ReplyButton
from dispatch_core.packs.catalog import (
    FieldDefinition,
    FieldType,
    PackDefinition,
    ServiceCategory,
    blank_definition,
)

CONFIG_MENU = "config_menu"
CONFIG_BRAND = "config_brand"
CONFIG_SERVICES = "config_services"
CONFIG_SERVICE_ADD = "config_service_add"
CONFIG_SERVICE_DELETE = "config_service_delete"
CONFIG_FIELDS = "config_fields"
CONFIG_FIELD_ADD = "config_field_add"
CONFIG_FIELD_DELETE = "config_field_delete"
CONFIG_FIELD_TYPE = "config_field_type"
CONFIG_FIELD_REQUIRED = "config_field_required"
CONFIG_EVIDENCE = "config_evidence"
CONFIG_EVIDENCE_PHOTO_INC = "config_evidence_photo_inc"
CONFIG_EVIDENCE_PHOTO_DEC = "config_evidence_photo_dec"
CONFIG_EVIDENCE_COMMENT = "config_evidence_comment"
CONFIG_EVIDENCE_SIGNATURE = "config_evidence_signature"
CONFIG_EVIDENCE_CODE = "config_evidence_code"
CONFIG_PREVIEW = "config_preview"
CONFIG_PUBLISH = "config_publish"
CONFIG_CANCEL = "config_cancel"

CONFIG_ACTIONS = frozenset(
    {
        CONFIG_MENU,
        CONFIG_BRAND,
        CONFIG_SERVICES,
        CONFIG_SERVICE_ADD,
        CONFIG_SERVICE_DELETE,
        CONFIG_FIELDS,
        CONFIG_FIELD_ADD,
        CONFIG_FIELD_DELETE,
        CONFIG_FIELD_TYPE,
        CONFIG_FIELD_REQUIRED,
        CONFIG_EVIDENCE,
        CONFIG_EVIDENCE_PHOTO_INC,
        CONFIG_EVIDENCE_PHOTO_DEC,
        CONFIG_EVIDENCE_COMMENT,
        CONFIG_EVIDENCE_SIGNATURE,
        CONFIG_EVIDENCE_CODE,
        CONFIG_PREVIEW,
        CONFIG_PUBLISH,
        CONFIG_CANCEL,
    }
)

# Commands surfaced in the bot menu, in display order.
MENU_COMMANDS: tuple[tuple[str, str], ...] = (
    ("brand", "Бренд: имя, приветствие, контакт"),
    ("services", "Услуги: каталог заявок"),
    ("fields", "Поля заявки"),
    ("evidence", "Требования к закрытию"),
    ("preview", "Предпросмотр глазами клиента"),
    ("publish", "Опубликовать версию"),
)

_FIELD_TYPE_LABELS: tuple[tuple[FieldType, str], ...] = (
    (FieldType.TEXT, "Текст"),
    (FieldType.ADDRESS, "Адрес"),
    (FieldType.INTEGER, "Число"),
    (FieldType.ASSET_REFERENCE, "Объект"),
    (FieldType.ROUTE, "Маршрут"),
)

_CANCEL_BUTTON = ReplyButton("Отмена", CONFIG_CANCEL, {}, "admin")

_ConfigHandler = Callable[
    [ActorIdentity, dict[str, Any]],
    Coroutine[Any, Any, Reply],
]


class ConfigCoordinator:
    """In-messenger admin configurator that edits a draft pack until publish."""

    def __init__(
        self,
        *,
        packs: PostgresPackStore,
        sessions: PostgresConfigSessionStore,
    ) -> None:
        self._packs = packs
        self._sessions = sessions
        self._dispatch: dict[str, _ConfigHandler] = {
            CONFIG_CANCEL: self._on_cancel,
            CONFIG_MENU: self._on_menu,
            CONFIG_BRAND: self._on_brand,
            CONFIG_SERVICES: self._on_services,
            CONFIG_SERVICE_ADD: self._on_service_add,
            CONFIG_SERVICE_DELETE: self._on_service_delete,
            CONFIG_FIELDS: self._on_fields,
            CONFIG_FIELD_ADD: self._on_field_add,
            CONFIG_FIELD_DELETE: self._on_field_delete,
            CONFIG_FIELD_TYPE: self._on_field_type,
            CONFIG_FIELD_REQUIRED: self._on_field_required,
            CONFIG_EVIDENCE: self._on_evidence,
            CONFIG_PREVIEW: self._on_preview,
            CONFIG_PUBLISH: self._on_publish,
        }
        for action in _EVIDENCE_ACTIONS:
            self._dispatch[action] = self._make_evidence_toggle(action)

    async def start(self, identity: ActorIdentity) -> Reply:
        return await self._menu(identity)

    async def handle_text(self, identity: ActorIdentity, text: str) -> Reply:
        text = text.strip()
        session = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if session and session.get("flow"):
            if text.startswith("/"):
                await self._sessions.clear(
                    identity.organization_id, identity.actor_id
                )
                return await self._command(identity, text)
            return await self._consume(identity, session, text)
        if text.startswith("/"):
            return await self._command(identity, text)
        return await self._menu(identity, note="Выберите раздел кнопкой ниже.")

    async def handle_callback(
        self,
        identity: ActorIdentity,
        action: str,
        payload: dict[str, Any],
    ) -> Reply:
        handler = self._dispatch.get(action)
        if handler is None:
            return await self._menu(identity, note="Неизвестное действие.")
        return await handler(identity, payload)

    async def _on_cancel(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        await self._sessions.clear(
            identity.organization_id, identity.actor_id
        )
        return await self._menu(identity, note="Действие отменено.")

    async def _on_menu(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._menu(identity)

    async def _on_brand(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._brand(identity)

    async def _on_services(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._services(identity)

    async def _on_service_add(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._service_add(identity)

    async def _on_service_delete(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        return await self._service_delete(identity, payload)

    async def _on_fields(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._fields(identity)

    async def _on_field_add(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._field_add(identity)

    async def _on_field_delete(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        return await self._field_delete(identity, payload)

    async def _on_field_type(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        return await self._field_type(identity, payload)

    async def _on_field_required(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        return await self._field_required(identity, payload)

    async def _on_evidence(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._evidence(identity)

    def _make_evidence_toggle(self, action: str) -> _ConfigHandler:
        async def handler(
            identity: ActorIdentity, _payload: dict[str, Any]
        ) -> Reply:
            return await self._evidence_toggle(identity, action)
        return handler

    async def _on_preview(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._preview(identity)

    async def _on_publish(
        self, identity: ActorIdentity, _payload: dict[str, Any]
    ) -> Reply:
        return await self._publish(identity)

    async def _command(self, identity: ActorIdentity, text: str) -> Reply:
        word = text.split()[0].lstrip("/").split("@")[0].lower()
        handlers = {
            "config": self._menu,
            "start": self._menu,
            "brand": self._brand,
            "services": self._services,
            "fields": self._fields,
            "evidence": self._evidence,
            "preview": self._preview,
            "publish": self._publish,
        }
        handler = handlers.get(word)
        if handler is None:
            return await self._menu(identity, note="Неизвестная команда.")
        return await handler(identity)

    async def _consume(
        self, identity: ActorIdentity, session: dict[str, Any], text: str
    ) -> Reply:
        flow = session.get("flow")
        step = session.get("step")
        if flow == "brand":
            return await self._brand_step(identity, step, text)
        if flow == "service_add" and step == "label":
            return await self._service_save(identity, text)
        if flow == "field_add" and step == "label":
            return await self._field_label(identity, session, text)
        return Reply(
            "Выберите вариант кнопкой ниже.", buttons=(_CANCEL_BUTTON,)
        )

    # -- brand -----------------------------------------------------------

    async def _brand(self, identity: ActorIdentity) -> Reply:
        await self._ensure_draft(identity)
        await self._set_session(identity, {"flow": "brand", "step": "name"})
        return Reply("Введите название бренда:", buttons=(_CANCEL_BUTTON,))

    async def _brand_step(
        self, identity: ActorIdentity, step: Any, text: str
    ) -> Reply:
        draft = await self._ensure_draft(identity)
        branding = draft.branding
        value = "" if text == "-" else text
        if step == "name":
            if not value:
                return Reply(
                    "Название не может быть пустым. Введите название бренда:",
                    buttons=(_CANCEL_BUTTON,),
                )
            branding = dataclasses.replace(branding, name=value)
            await self._replace(identity, draft, branding=branding)
            await self._set_session(
                identity, {"flow": "brand", "step": "greeting"}
            )
            return Reply(
                "Введите приветствие для клиента:", buttons=(_CANCEL_BUTTON,)
            )
        if step == "greeting":
            branding = dataclasses.replace(branding, greeting=value)
            await self._replace(identity, draft, branding=branding)
            await self._set_session(
                identity, {"flow": "brand", "step": "support"}
            )
            return Reply(
                "Введите контакт поддержки (или «-», чтобы пропустить):",
                buttons=(_CANCEL_BUTTON,),
            )
        branding = dataclasses.replace(branding, support=value)
        await self._replace(identity, draft, branding=branding)
        await self._sessions.clear(identity.organization_id, identity.actor_id)
        return await self._menu(identity, note="Бренд обновлён.")

    # -- services --------------------------------------------------------

    async def _services(self, identity: ActorIdentity) -> Reply:
        draft = await self._ensure_draft(identity)
        categories = draft.service_catalog.categories
        lines = ["Услуги каталога:"]
        if categories:
            lines.extend(f"• {item.label}" for item in categories)
        else:
            lines.append("(пока пусто)")
        buttons: list[ReplyButton] = [
            ReplyButton("Добавить услугу", CONFIG_SERVICE_ADD, {}, "admin")
        ]
        for index, item in enumerate(categories, start=1):
            buttons.append(
                ReplyButton(
                    f"Удалить: {item.label}",
                    CONFIG_SERVICE_DELETE,
                    {"key": item.key},
                    "admin",
                    row=index,
                )
            )
        buttons.append(
            ReplyButton("В меню", CONFIG_MENU, {}, "admin", row=len(buttons))
        )
        return Reply("\n".join(lines), buttons=tuple(buttons))

    async def _service_add(self, identity: ActorIdentity) -> Reply:
        await self._ensure_draft(identity)
        await self._set_session(
            identity, {"flow": "service_add", "step": "label"}
        )
        return Reply("Введите название услуги:", buttons=(_CANCEL_BUTTON,))

    async def _service_save(self, identity: ActorIdentity, text: str) -> Reply:
        if not text:
            return Reply(
                "Название не может быть пустым. Введите название услуги:",
                buttons=(_CANCEL_BUTTON,),
            )
        draft = await self._ensure_draft(identity)
        catalog = draft.service_catalog
        taken = {item.key for item in catalog.categories}
        key = _slug(text, "service", taken)
        categories = (*catalog.categories, ServiceCategory(key, text))
        new_catalog = dataclasses.replace(catalog, categories=categories)
        await self._replace(identity, draft, service_catalog=new_catalog)
        await self._sessions.clear(identity.organization_id, identity.actor_id)
        return await self._services(identity)

    async def _service_delete(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        key = str(payload.get("key") or "")
        draft = await self._ensure_draft(identity)
        catalog = draft.service_catalog
        categories = tuple(
            item for item in catalog.categories if item.key != key
        )
        new_catalog = dataclasses.replace(catalog, categories=categories)
        await self._replace(identity, draft, service_catalog=new_catalog)
        return await self._services(identity)

    # -- fields ----------------------------------------------------------

    async def _fields(self, identity: ActorIdentity) -> Reply:
        draft = await self._ensure_draft(identity)
        fields = draft.ordered_fields()
        lines = ["Поля заявки:"]
        if fields:
            for item in fields:
                mark = "*" if item.required else ""
                lines.append(f"• {item.label} ({item.field_type.value}){mark}")
        else:
            lines.append("(пока пусто)")
        buttons: list[ReplyButton] = [
            ReplyButton("Добавить поле", CONFIG_FIELD_ADD, {}, "admin")
        ]
        for index, item in enumerate(fields, start=1):
            buttons.append(
                ReplyButton(
                    f"Удалить: {item.label}",
                    CONFIG_FIELD_DELETE,
                    {"key": item.key},
                    "admin",
                    row=index,
                )
            )
        buttons.append(
            ReplyButton("В меню", CONFIG_MENU, {}, "admin", row=len(buttons))
        )
        return Reply("\n".join(lines), buttons=tuple(buttons))

    async def _field_add(self, identity: ActorIdentity) -> Reply:
        await self._ensure_draft(identity)
        await self._set_session(
            identity, {"flow": "field_add", "step": "label", "scratch": {}}
        )
        return Reply("Введите название поля:", buttons=(_CANCEL_BUTTON,))

    async def _field_label(
        self, identity: ActorIdentity, session: dict[str, Any], text: str
    ) -> Reply:
        if not text:
            return Reply(
                "Название не может быть пустым. Введите название поля:",
                buttons=(_CANCEL_BUTTON,),
            )
        session["step"] = "type"
        session["scratch"] = {"label": text}
        await self._set_session(identity, session)
        buttons = [
            ReplyButton(
                label,
                CONFIG_FIELD_TYPE,
                {"type": field_type.value},
                "admin",
                row=index,
            )
            for index, (field_type, label) in enumerate(_FIELD_TYPE_LABELS)
        ]
        buttons.append(
            ReplyButton("Отмена", CONFIG_CANCEL, {}, "admin", row=len(buttons))
        )
        return Reply("Выберите тип поля:", buttons=tuple(buttons))

    async def _field_type(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        session = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if not session or session.get("flow") != "field_add":
            return await self._fields(identity)
        try:
            field_type = FieldType(str(payload.get("type") or ""))
        except ValueError:
            return Reply("Неизвестный тип поля.", buttons=(_CANCEL_BUTTON,))
        scratch = dict(session.get("scratch", {}))
        scratch["type"] = field_type.value
        session["scratch"] = scratch
        session["step"] = "required"
        await self._set_session(identity, session)
        return Reply(
            "Поле обязательно к заполнению?",
            buttons=(
                ReplyButton("Да", CONFIG_FIELD_REQUIRED, {"required": True}, "admin"),
                ReplyButton(
                    "Нет",
                    CONFIG_FIELD_REQUIRED,
                    {"required": False},
                    "admin",
                    row=1,
                ),
            ),
        )

    async def _field_required(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        session = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if not session or session.get("flow") != "field_add":
            return await self._fields(identity)
        scratch = dict(session.get("scratch", {}))
        label = str(scratch.get("label") or "")
        field_type = FieldType(str(scratch.get("type") or FieldType.TEXT.value))
        required = bool(payload.get("required"))
        draft = await self._ensure_draft(identity)
        taken = {item.key for item in draft.fields}
        key = _slug(label, "field", taken)
        order = max((item.order for item in draft.fields), default=0) + 1
        definition = FieldDefinition(
            key=key,
            label=label,
            field_type=field_type,
            required=required,
            prompt=f"Введите: {label}",
            order=order,
        )
        fields = (*draft.fields, definition)
        await self._replace(identity, draft, fields=fields)
        await self._sessions.clear(identity.organization_id, identity.actor_id)
        return await self._fields(identity)

    async def _field_delete(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        key = str(payload.get("key") or "")
        draft = await self._ensure_draft(identity)
        fields = tuple(item for item in draft.fields if item.key != key)
        await self._replace(identity, draft, fields=fields)
        return await self._fields(identity)

    # -- evidence --------------------------------------------------------

    async def _evidence(self, identity: ActorIdentity) -> Reply:
        draft = await self._ensure_draft(identity)
        return self._render_evidence(draft.evidence)

    async def _evidence_toggle(
        self, identity: ActorIdentity, action: str
    ) -> Reply:
        draft = await self._ensure_draft(identity)
        evidence = draft.evidence
        if action == CONFIG_EVIDENCE_PHOTO_INC:
            evidence = dataclasses.replace(
                evidence, minimum_photos=min(evidence.minimum_photos + 1, 10)
            )
        elif action == CONFIG_EVIDENCE_PHOTO_DEC:
            evidence = dataclasses.replace(
                evidence, minimum_photos=max(evidence.minimum_photos - 1, 0)
            )
        elif action == CONFIG_EVIDENCE_COMMENT:
            evidence = dataclasses.replace(
                evidence, comment_required=not evidence.comment_required
            )
        elif action == CONFIG_EVIDENCE_SIGNATURE:
            evidence = dataclasses.replace(
                evidence, signature_required=not evidence.signature_required
            )
        elif action == CONFIG_EVIDENCE_CODE:
            evidence = dataclasses.replace(
                evidence,
                customer_code_required=not evidence.customer_code_required,
            )
        await self._replace(identity, draft, evidence=evidence)
        return self._render_evidence(evidence)

    def _render_evidence(self, evidence: EvidenceRequirements) -> Reply:
        lines = [
            "Требования к закрытию заявки:",
            f"Фото: минимум {evidence.minimum_photos}",
            f"Комментарий: {_yes_no(evidence.comment_required)}",
            f"Подпись: {_yes_no(evidence.signature_required)}",
            f"Код клиента: {_yes_no(evidence.customer_code_required)}",
        ]
        buttons = (
            ReplyButton("Фото +1", CONFIG_EVIDENCE_PHOTO_INC, {}, "admin"),
            ReplyButton(
                "Фото −1", CONFIG_EVIDENCE_PHOTO_DEC, {}, "admin", row=0
            ),
            ReplyButton(
                "Комментарий",
                CONFIG_EVIDENCE_COMMENT,
                {},
                "admin",
                row=1,
            ),
            ReplyButton(
                "Подпись", CONFIG_EVIDENCE_SIGNATURE, {}, "admin", row=1
            ),
            ReplyButton(
                "Код клиента", CONFIG_EVIDENCE_CODE, {}, "admin", row=2
            ),
            ReplyButton("В меню", CONFIG_MENU, {}, "admin", row=3),
        )
        return Reply("\n".join(lines), buttons=buttons)

    # -- preview / publish ----------------------------------------------

    async def _preview(self, identity: ActorIdentity) -> Reply:
        draft = await self._packs.draft(identity.organization_id)
        if draft is None:
            draft = await self._packs.active(identity.organization_id)
        if draft is None:
            return await self._menu(identity, note="Черновик пуст.")
        renderer = CardRenderer(draft)
        lines = ["Предпросмотр (глазами клиента):", "", renderer.greeting()]
        categories = draft.service_catalog.categories
        if categories:
            lines.append("")
            lines.append(
                "Услуги: " + ", ".join(item.label for item in categories)
            )
        sample = {
            field.key: f"<{field.label}>" for field in draft.ordered_fields()
        }
        service_labels = [item.label for item in categories]
        lines.append("")
        lines.append(
            renderer.confirmation_card(
                service_labels=service_labels, field_values=sample
            )
        )
        return Reply(
            "\n".join(lines),
            buttons=(ReplyButton("В меню", CONFIG_MENU, {}, "admin"),),
        )

    async def _publish(self, identity: ActorIdentity) -> Reply:
        try:
            version = await self._packs.publish_draft(identity.organization_id)
        except PackValidationError as exc:
            return Reply(
                f"Не удалось опубликовать: {exc}",
                buttons=(ReplyButton("В меню", CONFIG_MENU, {}, "admin"),),
            )
        except NotFound:
            return await self._menu(identity, note="Нет черновика для публикации.")
        return Reply(
            f"Готово. Версия {version} активна — клиенты уже видят новый флоу."
        )

    # -- helpers ---------------------------------------------------------

    async def _menu(
        self, identity: ActorIdentity, *, note: str | None = None
    ) -> Reply:
        draft = await self._packs.draft(identity.organization_id)
        active = await self._packs.active(identity.organization_id)
        lines: list[str] = []
        if note:
            lines.append(note)
            lines.append("")
        lines.append("Конфигуратор сервиса")
        source = draft or active
        if source is not None:
            name = source.branding.name or "(без названия)"
            services = len(source.service_catalog.categories)
            fields = len(source.fields)
            state = "черновик" if draft is not None else "активный"
            lines.append(
                f"Текущий ({state}): {name} — услуг {services}, полей {fields}"
            )
        else:
            lines.append("Пак ещё не создан — начните с раздела «Бренд».")
        buttons = (
            ReplyButton("Бренд", CONFIG_BRAND, {}, "admin"),
            ReplyButton("Услуги", CONFIG_SERVICES, {}, "admin", row=0),
            ReplyButton("Поля", CONFIG_FIELDS, {}, "admin", row=1),
            ReplyButton("Закрытие", CONFIG_EVIDENCE, {}, "admin", row=1),
            ReplyButton("Предпросмотр", CONFIG_PREVIEW, {}, "admin", row=2),
            ReplyButton("Опубликовать", CONFIG_PUBLISH, {}, "admin", row=2),
        )
        return Reply("\n".join(lines), buttons=buttons)

    async def _ensure_draft(self, identity: ActorIdentity) -> PackDefinition:
        org = identity.organization_id
        draft = await self._packs.draft(org)
        if draft is not None:
            return draft
        active = await self._packs.active(org)
        seed = active if active is not None else blank_definition()
        return await self._packs.ensure_draft(org, seed=seed)

    async def _replace(
        self,
        identity: ActorIdentity,
        draft: PackDefinition,
        **changes: Any,
    ) -> None:
        updated = dataclasses.replace(draft, **changes)
        await self._packs.update_draft(identity.organization_id, updated)

    async def _set_session(
        self, identity: ActorIdentity, state: dict[str, Any]
    ) -> None:
        await self._sessions.put(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            provider=identity.provider,
            state=state,
        )


_EVIDENCE_ACTIONS = frozenset(
    {
        CONFIG_EVIDENCE_PHOTO_INC,
        CONFIG_EVIDENCE_PHOTO_DEC,
        CONFIG_EVIDENCE_COMMENT,
        CONFIG_EVIDENCE_SIGNATURE,
        CONFIG_EVIDENCE_CODE,
    }
)


def _yes_no(value: bool) -> str:
    return "да" if value else "нет"


def _slug(label: str, prefix: str, taken: set[str]) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    if not base:
        base = prefix
    key = base
    index = 2
    while key in taken:
        key = f"{base}_{index}"
        index += 1
    return key
