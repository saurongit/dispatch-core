from __future__ import annotations

import logging
from math import isfinite
from secrets import token_urlsafe
from typing import Any, TypedDict

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.application.identity import ActorIdentity
from dispatch_core.application.tracking_links import intake_address_url
from dispatch_core.infrastructure.messaging import PostgresOutboundStore
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.workflow_store import (
    PostgresIdentityStore,
    PostgresIntakeSessionStore,
)
from dispatch_core.messaging.cards import CardRenderer
from dispatch_core.messaging.replies import Reply, ReplyButton
from dispatch_core.packs.catalog import PackDefinition

logger = logging.getLogger(__name__)

INTAKE_PICK_SERVICE = "intake_pick_service"
INTAKE_SERVICES_DONE = "intake_services_done"
INTAKE_CONFIRM = "intake_confirm"
INTAKE_CANCEL = "intake_cancel"
INTAKE_REQUEST_LOCATION = "intake_request_location"
INTAKE_TYPE_ADDRESS = "intake_type_address"
INTAKE_CONTINUE_AFTER_MAP = "intake_continue_after_map"
INTAKE_ACTIONS = frozenset(
    {
        INTAKE_PICK_SERVICE,
        INTAKE_SERVICES_DONE,
        INTAKE_CONFIRM,
        INTAKE_CANCEL,
        INTAKE_REQUEST_LOCATION,
        INTAKE_TYPE_ADDRESS,
        INTAKE_CONTINUE_AFTER_MAP,
    }
)

_CANCEL_BUTTON = ReplyButton("Отмена", INTAKE_CANCEL, {}, "client")


class _PhoneState(TypedDict, total=False):
    step: str  # "phone"


class _AddressState(TypedDict, total=False):
    step: str  # "address"
    field_values: dict[str, str]
    address_token: str
    address_mode: str
    service_location: dict[str, float | str]


class _ServicesState(TypedDict, total=False):
    step: str  # "services"
    field_values: dict[str, str]
    selected: list[str]


class _FieldsState(TypedDict, total=False):
    step: str  # "fields"
    selected: list[str]
    field_index: int
    field_values: dict[str, str]


class _ConfirmState(TypedDict, total=False):
    step: str  # "confirm"
    selected: list[str]
    field_values: dict[str, str]


IntakeState = (
    _PhoneState | _AddressState | _ServicesState | _FieldsState | _ConfirmState
)


class IntakeCoordinator:
    """Guided, pack-driven client flow that ends in a submitted work order."""

    def __init__(
        self,
        *,
        packs: PostgresPackStore,
        sessions: PostgresIntakeSessionStore,
        service: AsyncDispatchService,
        outbound: PostgresOutboundStore | None = None,
        identities: PostgresIdentityStore | None = None,
        public_base_url: str | None = None,
    ) -> None:
        self._packs = packs
        self._sessions = sessions
        self._service = service
        self._outbound = outbound
        self._identities = identities
        self._public_base_url = public_base_url

    async def start(self, identity: ActorIdentity) -> Reply:
        pack = await self._packs.active(identity.organization_id)
        if pack is None:
            return Reply(_NOT_CONFIGURED)
        state: dict[str, Any] = {"step": "phone", "field_values": {}}
        await self._save(identity, state)
        return Reply("Введите ваш телефон:", buttons=(_CANCEL_BUTTON,))

    async def handle_text(self, identity: ActorIdentity, text: str) -> Reply:
        state = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if state is None:
            return await self.start(identity)
        pack = await self._packs.active(identity.organization_id)
        if pack is None:
            return Reply(_NOT_CONFIGURED)
        step = state.get("step")
        if step == "phone":
            return await self._fill_phone(identity, pack, state, text)
        if step == "address":
            return await self._fill_address(identity, pack, state, text)
        if step == "fields":
            return await self._fill_field(identity, pack, state, text)
        if step == "confirm":
            return Reply(
                "Нажмите «Отправить» или «Отмена».",
                buttons=_confirm_buttons(),
            )
        return Reply(
            "Выберите услугу кнопкой ниже.",
            buttons=_service_buttons(pack, state.get("selected", ())),
        )

    async def handle_callback(
        self,
        identity: ActorIdentity,
        action: str,
        payload: dict[str, Any],
    ) -> Reply:
        if action == INTAKE_CANCEL:
            await self._sessions.clear(
                identity.organization_id, identity.actor_id
            )
            return Reply("Заявка отменена.")
        pack = await self._packs.active(identity.organization_id)
        if pack is None:
            return Reply(_NOT_CONFIGURED)
        state = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if state is None:
            return await self.start(identity)
        step = state.get("step")
        if action == INTAKE_REQUEST_LOCATION:
            if step != "address":
                return Reply("Геопозиция сейчас не запрашивается.")
            state["address_mode"] = "native"
            await self._save(identity, state)
            return Reply(
                "Нажмите кнопку ниже и отправьте геопозицию устройства. "
                "Если точка неверная или вы находитесь не на объекте — "
                "вернитесь и выберите место на карте.",
                buttons=(
                    ReplyButton(
                        "📍 Отправить геопозицию",
                        "",
                        request_location=True,
                    ),
                ),
            )
        if action == INTAKE_TYPE_ADDRESS:
            if step != "address":
                return Reply("Адрес уже указан.")
            state["address_mode"] = "text"
            await self._save(identity, state)
            return Reply(
                "Введите город, улицу и номер дома:",
                buttons=(_CANCEL_BUTTON,),
            )
        if action == INTAKE_CONTINUE_AFTER_MAP:
            if step == "services":
                return _services_reply(pack)
            if step != "address":
                return Reply("Точка на карте сейчас не запрашивается.")
            return Reply(
                "Сначала откройте карту и сохраните точку. Затем вернитесь "
                "сюда и снова нажмите «Продолжить после карты».",
                buttons=self._address_buttons(state),
            )
        if action == INTAKE_PICK_SERVICE:
            if step not in {"services", "fields", "confirm"}:
                return Reply(
                    "Сначала начните новую заявку.",
                    buttons=_service_buttons(pack, state.get("selected", ())),
                )
            return await self._pick_service(identity, pack, state, payload)
        if action == INTAKE_SERVICES_DONE:
            if step != "services":
                return Reply(
                    "Сначала выберите услуги.",
                    buttons=_service_buttons(pack, state.get("selected", ())),
                )
            return await self._finish_services(identity, pack, state)
        if action == INTAKE_CONFIRM:
            if step != "confirm":
                return Reply("Сначала заполните все поля.")
            return await self._confirm(identity, pack, state)
        return Reply("Неизвестное действие.")

    # -- hardcoded phone / address ----------------------------------------

    async def _fill_phone(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
        text: str,
    ) -> Reply:
        value = text.strip()[:500]
        if not value or len(value.replace(" ", "").replace("-", "")) < 7:
            return Reply(
                "Введите корректный номер телефона (минимум 7 цифр):",
                buttons=(_CANCEL_BUTTON,),
            )
        state.setdefault("field_values", {})["phone"] = value
        state["step"] = "address"
        state["address_token"] = token_urlsafe(32)
        state.pop("address_mode", None)
        state.pop("service_location", None)
        await self._save(identity, state)
        return self._address_prompt(state)

    async def _fill_address(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
        text: str,
    ) -> Reply:
        value = text.strip()[:500]
        if not value or len(value) < 5:
            return Reply(
                "Введите адрес подробнее (минимум 5 символов):",
                buttons=(_CANCEL_BUTTON,),
            )
        state.setdefault("field_values", {})["address"] = value
        state.pop("service_location", None)
        state.pop("address_token", None)
        state.pop("address_mode", None)
        state["step"] = "services"
        state.setdefault("selected", [])
        await self._save(identity, state)
        return _services_reply(pack)

    async def handle_location(
        self,
        identity: ActorIdentity,
        *,
        latitude: float,
        longitude: float,
        method: str = "native",
        address: str | None = None,
    ) -> Reply:
        if (
            not isfinite(latitude)
            or not isfinite(longitude)
            or not -90 <= latitude <= 90
            or not -180 <= longitude <= 180
        ):
            return Reply("Не удалось распознать геопозицию. Выберите адрес заново.")
        state = await self._sessions.get(
            identity.organization_id, identity.actor_id
        )
        if state is None:
            return await self.start(identity)
        pack = await self._packs.active(identity.organization_id)
        if pack is None:
            return Reply(_NOT_CONFIGURED)
        if state.get("step") != "address":
            return Reply("Геопозиция сейчас не запрашивается.")
        label = (address or "").strip()[:500]
        if not label:
            label = f"Точка на карте: {latitude:.5f}, {longitude:.5f}"
        state.setdefault("field_values", {})["address"] = label
        state["service_location"] = {
            "latitude": latitude,
            "longitude": longitude,
            "method": method,
        }
        state["step"] = "services"
        state.setdefault("selected", [])
        state.pop("address_token", None)
        state.pop("address_mode", None)
        await self._save(identity, state)
        return _services_reply(pack)

    def _address_prompt(self, state: dict[str, Any]) -> Reply:
        return Reply(
            "📍 Укажите место выполнения работы.\n\n"
            "VPN обычно меняет IP, а не GPS, но геопозиция иногда "
            "определяется неверно. Перед отправкой проверьте точку. Если вы "
            "используете VPN, находитесь не на объекте или точка показана "
            "неправильно — выберите место на карте либо введите адрес вручную.",
            buttons=self._address_buttons(state),
        )

    def _address_buttons(
        self, state: dict[str, Any]
    ) -> tuple[ReplyButton, ...]:
        buttons = [
            ReplyButton(
                "📍 Отправить геопозицию",
                INTAKE_REQUEST_LOCATION,
                {},
                "client",
            )
        ]
        token = str(state.get("address_token") or "")
        if self._public_base_url and len(token) >= 43:
            buttons.extend(
                (
                    ReplyButton(
                        "🗺 Выбрать точку на карте",
                        "",
                        row=1,
                        url=intake_address_url(self._public_base_url, token),
                    ),
                    ReplyButton(
                        "✅ Продолжить после карты",
                        INTAKE_CONTINUE_AFTER_MAP,
                        {},
                        "client",
                        row=2,
                    ),
                )
            )
        buttons.append(
            ReplyButton(
                "⌨️ Ввести адрес",
                INTAKE_TYPE_ADDRESS,
                {},
                "client",
                row=3,
            )
        )
        buttons.append(
            ReplyButton("Отмена", INTAKE_CANCEL, {}, "client", row=4)
        )
        return tuple(buttons)

    # -- services / fields / confirm --------------------------------------

    async def _pick_service(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
        payload: dict[str, Any],
    ) -> Reply:
        key = str(payload.get("service") or "")
        catalog = pack.service_catalog
        if key not in {item.key for item in catalog.categories}:
            return Reply("Услуга недоступна.")
        selected: list[str] = list(state.get("selected", []))
        if not catalog.multi_select:
            state["selected"] = [key]
            return await self._finish_services(identity, pack, state)
        if key in selected:
            selected.remove(key)
        else:
            selected.append(key)
        state["selected"] = selected
        await self._save(identity, state)
        return Reply(
            "Отметьте услуги и нажмите «Готово».",
            buttons=_service_buttons(pack, selected),
        )

    async def _finish_services(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
    ) -> Reply:
        selected = list(state.get("selected", []))
        if not selected:
            return Reply(
                "Выберите хотя бы одну услугу.",
                buttons=_service_buttons(pack, selected),
            )
        state["step"] = "fields"
        state.setdefault("field_values", {})
        fields = pack.ordered_fields()
        start = 0
        for i, f in enumerate(fields):
            if f.key not in state["field_values"]:
                start = i
                break
        else:
            return await self._to_confirmation(identity, pack, state)
        state["field_index"] = start
        if not fields:
            return await self._to_confirmation(identity, pack, state)
        await self._save(identity, state)
        return _ask_field(fields[start])

    async def _fill_field(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
        text: str,
    ) -> Reply:
        fields = pack.ordered_fields()
        try:
            index = int(state.get("field_index", 0))
        except (ValueError, TypeError):
            return await self._to_confirmation(identity, pack, state)
        if index < 0 or index >= len(fields):
            return await self._to_confirmation(identity, pack, state)
        definition = fields[index]
        value = text.strip()[:500]
        if not value or value == "-":
            if definition.required:
                return Reply(
                    f"Это поле обязательно. {definition.ask()}",
                    buttons=(_CANCEL_BUTTON,),
                )
        else:
            state.setdefault("field_values", {})[definition.key] = value
        index += 1
        state["field_index"] = index
        fields = pack.ordered_fields()
        fv = state.get("field_values", {})
        while index < len(fields) and fields[index].key in fv:
            index += 1
            state["field_index"] = index
        if index >= len(fields):
            return await self._to_confirmation(identity, pack, state)
        await self._save(identity, state)
        return _ask_field(fields[index])

    async def _to_confirmation(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
    ) -> Reply:
        state["step"] = "confirm"
        await self._save(identity, state)
        return self._render_confirmation(pack, state)

    def _render_confirmation(
        self, pack: PackDefinition, state: dict[str, Any]
    ) -> Reply:
        labels = [
            pack.service_catalog.label_for(key)
            for key in state.get("selected", [])
        ]
        fv = state.get("field_values", {})
        lines = ["Заявка:", ""]
        if fv.get("phone"):
            lines.append(f"Телефон: {fv['phone']}")
        if fv.get("address"):
            lines.append(f"Адрес: {fv['address']}")
        if labels:
            lines.append(f"Услуги: {', '.join(labels)}")
        for key, val in fv.items():
            if key not in ("phone", "address"):
                lines.append(f"{key}: {val}")
        lines.append("")
        lines.append("Подтвердить отправку?")
        return Reply("\n".join(lines), buttons=_confirm_buttons())

    async def _confirm(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
    ) -> Reply:
        selected = list(state.get("selected", []))
        if not selected:
            return await self.start(identity)
        labels = [pack.service_catalog.label_for(key) for key in selected]
        field_values = dict(state.get("field_values", {}))
        details: dict[str, Any] = {
            "services": labels,
            "service_keys": selected,
            **field_values,
        }
        service_location = state.get("service_location")
        if isinstance(service_location, dict):
            details["service_location"] = dict(service_location)
        order = None
        try:
            order = await self._service.create_order(
                organization_id=identity.organization_id,
                work_type=",".join(selected),
                source=f"{identity.provider.value}:{identity.external_user_id}",
                details=details,
                requester_id=identity.actor_id,
                evidence_requirements=pack.evidence,
            )
        finally:
            await self._sessions.clear(identity.organization_id, identity.actor_id)

        if order is not None and self._outbound is not None:
            await self._notify_operators(identity, order, field_values, labels)

        return Reply(
            "Заявка отправлена. Оператор свяжется с вами в ближайшее время."
        )

    async def _notify_operators(
        self,
        identity: ActorIdentity,
        order: Any,
        field_values: dict[str, str],
        service_labels: list[str],
    ) -> None:
        phone = field_values.get("phone", "не указан")
        address = field_values.get("address", "не указан")
        lines = [
            f"Новая заявка #{order.id[:8]}",
            "",
            f"Клиент: {identity.display_name}",
            f"Телефон: {phone}",
            f"Адрес: {address}",
            f"Услуги: {', '.join(service_labels)}",
            "",
            "Статус: Новая",
        ]
        extra = [
            f"{k}: {v}"
            for k, v in field_values.items()
            if k not in ("phone", "address")
        ]
        if extra:
            lines.extend(extra)
        text = "\n".join(lines)

        if self._identities is not None and self._outbound is not None:
            external_ids = await self._identities.external_ids_for_role(
                organization_id=identity.organization_id,
                provider=identity.provider,
                role="operator",
                consumer_key="operator",
            )
            for external_id in external_ids:
                if not external_id:
                    continue
                await self._outbound.enqueue(
                    deduplication_key=f"order:{order.id}:op:{external_id}",
                    organization_id=identity.organization_id,
                    provider=identity.provider,
                    recipient_id=external_id,
                    text=text,
                    consumer_key="operator",
                )

    async def _save(
        self, identity: ActorIdentity, state: dict[str, Any]
    ) -> None:
        await self._sessions.put(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            provider=identity.provider,
            state=state,
        )


_NOT_CONFIGURED = "Приём заявок ещё не настроен. Обратитесь к администратору."


def _services_reply(pack: PackDefinition) -> Reply:
    renderer = CardRenderer(pack)
    return Reply(
        renderer.greeting(),
        buttons=_service_buttons(pack, ()),
    )


def _ask_field(definition: Any) -> Reply:
    hint = "" if definition.required else " (или «-», чтобы пропустить)"
    return Reply(f"{definition.ask()}{hint}", buttons=(_CANCEL_BUTTON,))


def _service_buttons(
    pack: PackDefinition, selected: object
) -> tuple[ReplyButton, ...]:
    chosen = set(selected or ())
    buttons: list[ReplyButton] = []
    for row, category in enumerate(pack.service_catalog.categories):
        mark = "✅ " if category.key in chosen else ""
        buttons.append(
            ReplyButton(
                f"{mark}{category.label}",
                INTAKE_PICK_SERVICE,
                {"service": category.key},
                "client",
                row=row,
            )
        )
    if pack.service_catalog.multi_select:
        buttons.append(
            ReplyButton(
                "Готово",
                INTAKE_SERVICES_DONE,
                {},
                "client",
                row=len(buttons),
            )
        )
    return tuple(buttons)


def _confirm_buttons() -> tuple[ReplyButton, ...]:
    return (
        ReplyButton("Отправить", INTAKE_CONFIRM, {}, "client"),
        ReplyButton("Отмена", INTAKE_CANCEL, {}, "client", row=1),
    )
