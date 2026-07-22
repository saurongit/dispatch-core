from __future__ import annotations

from typing import Any, TypedDict

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.application.identity import ActorIdentity
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.workflow_store import PostgresIntakeSessionStore
from dispatch_core.messaging.cards import CardRenderer
from dispatch_core.messaging.replies import Reply, ReplyButton
from dispatch_core.packs.catalog import PackDefinition

INTAKE_PICK_SERVICE = "intake_pick_service"
INTAKE_SERVICES_DONE = "intake_services_done"
INTAKE_CONFIRM = "intake_confirm"
INTAKE_CANCEL = "intake_cancel"
INTAKE_ACTIONS = frozenset(
    {INTAKE_PICK_SERVICE, INTAKE_SERVICES_DONE, INTAKE_CONFIRM, INTAKE_CANCEL}
)

_CANCEL_BUTTON = ReplyButton("Отмена", INTAKE_CANCEL, {}, "requester")


class _ServicesState(TypedDict, total=False):
    step: str  # literal "services"
    selected: list[str]


class _FieldsState(TypedDict, total=False):
    step: str  # literal "fields"
    selected: list[str]
    field_index: int
    field_values: dict[str, str]


class _ConfirmState(TypedDict, total=False):
    step: str  # literal "confirm"
    selected: list[str]
    field_values: dict[str, str]


IntakeState = _ServicesState | _FieldsState | _ConfirmState


class IntakeCoordinator:
    """Guided, pack-driven client flow that ends in a submitted work order."""

    def __init__(
        self,
        *,
        packs: PostgresPackStore,
        sessions: PostgresIntakeSessionStore,
        service: AsyncDispatchService,
    ) -> None:
        self._packs = packs
        self._sessions = sessions
        self._service = service

    async def start(self, identity: ActorIdentity) -> Reply:
        pack = await self._packs.active(identity.organization_id)
        if pack is None:
            return Reply(_NOT_CONFIGURED)
        state = {"step": "services", "selected": [], "field_values": {}}
        await self._save(identity, state)
        renderer = CardRenderer(pack)
        return Reply(
            renderer.greeting(),
            buttons=_service_buttons(pack, ()),
        )

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
        if step == "fields":
            return await self._fill_field(identity, pack, state, text)
        if step == "confirm":
            return Reply(
                "Нажмите «Подтвердить» или «Отмена».",
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
        state["field_index"] = 0
        state.setdefault("field_values", {})
        fields = pack.ordered_fields()
        if not fields:
            return await self._to_confirmation(identity, pack, state)
        await self._save(identity, state)
        return _ask_field(fields[0])

    async def _fill_field(
        self,
        identity: ActorIdentity,
        pack: PackDefinition,
        state: dict[str, Any],
        text: str,
    ) -> Reply:
        fields = pack.ordered_fields()
        index = int(state.get("field_index", 0))
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
        renderer = CardRenderer(pack)
        card = renderer.confirmation_card(
            service_labels=labels,
            field_values=state.get("field_values", {}),
        )
        return Reply(card, buttons=_confirm_buttons())

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
        try:
            await self._service.create_order(
                organization_id=identity.organization_id,
                work_type=",".join(selected),
                source=f"{identity.provider.value}:{identity.external_user_id}",
                details=details,
                requester_id=identity.actor_id,
                evidence_requirements=pack.evidence,
            )
        finally:
            await self._sessions.clear(identity.organization_id, identity.actor_id)
        return Reply("Заявка создана. Оператор скоро свяжется с вами.")

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
                "requester",
                row=row,
            )
        )
    if pack.service_catalog.multi_select:
        buttons.append(
            ReplyButton(
                "Готово",
                INTAKE_SERVICES_DONE,
                {},
                "requester",
                row=len(buttons),
            )
        )
    return tuple(buttons)


def _confirm_buttons() -> tuple[ReplyButton, ...]:
    return (
        ReplyButton("Подтвердить", INTAKE_CONFIRM, {}, "requester"),
        ReplyButton("Отмена", INTAKE_CANCEL, {}, "requester", row=1),
    )
