from __future__ import annotations

from secrets import token_urlsafe
from typing import Any

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.staff_workflows import (
    PostgresStaffViewStore,
    PostgresStaffWorkflowSessionStore,
)
from dispatch_core.infrastructure.workflow_store import PostgresIdentityStore
from dispatch_core.messaging.cards import (
    master_order_card,
    normalize_phone,
    operator_order_card,
    order_number,
    phone_display,
    status_emoji,
)
from dispatch_core.messaging.replies import Reply, ReplyButton

OPERATOR_MENU = "operator_menu"
OPERATOR_LIST_ORDERS = "operator_list_orders"
OPERATOR_OPEN_ORDER = "operator_open_order"
OPERATOR_LIST_MASTERS = "operator_list_masters"
OPERATOR_ADD_MASTER = "operator_add_master"
OPERATOR_MASTER_INFO = "operator_master_info"
OPERATOR_CALL_MASTER = "operator_call_master"
OPERATOR_CALL_CLIENT = "operator_call_client"
OPERATOR_DELETE_MASTER = "operator_delete_master"
OPERATOR_DELETE_MASTER_CONFIRM = "operator_delete_master_confirm"
OPERATOR_STATS = "operator_stats"
OPERATOR_CANCEL = "operator_cancel"

MASTER_MENU = "master_menu"
MASTER_LIST_ORDERS = "master_list_orders"
MASTER_OPEN_ORDER = "master_open_order"

OPERATOR_ACTIONS = frozenset(
    {
        OPERATOR_MENU,
        OPERATOR_LIST_ORDERS,
        OPERATOR_OPEN_ORDER,
        OPERATOR_LIST_MASTERS,
        OPERATOR_ADD_MASTER,
        OPERATOR_MASTER_INFO,
        OPERATOR_CALL_MASTER,
        OPERATOR_CALL_CLIENT,
        OPERATOR_DELETE_MASTER,
        OPERATOR_DELETE_MASTER_CONFIRM,
        OPERATOR_STATS,
        OPERATOR_CANCEL,
    }
)
MASTER_ACTIONS = frozenset({MASTER_MENU, MASTER_LIST_ORDERS, MASTER_OPEN_ORDER})

OPERATOR_MENU_COMMANDS: tuple[tuple[str, str], ...] = (
    ("active", "Активные заявки"),
    ("masters", "Мастера"),
    ("stats", "Статистика"),
)
MASTER_MENU_COMMANDS: tuple[tuple[str, str], ...] = (("active", "Мои активные заявки"),)

_ACTIVE_LABELS = {
    "submitted": "🆕 новая",
    "pool_open": "📢 в пуле",
    "assigned": "🟡 назначена",
    "accepted": "✅ принята",
    "en_route": "🚗 мастер выехал",
    "in_progress": "🔧 выполняется",
}


class OperatorCoordinator:
    """Messenger-only operator workspace and controlled master management."""

    def __init__(
        self,
        *,
        identities: PostgresIdentityStore,
        sessions: PostgresStaffWorkflowSessionStore,
        views: PostgresStaffViewStore,
        packs: PostgresPackStore | None = None,
    ) -> None:
        self._identities = identities
        self._sessions = sessions
        self._views = views
        self._packs = packs

    async def start(self, identity: ActorIdentity) -> Reply:
        await self._clear_session(identity)
        return self.menu(identity)

    def menu(self, identity: ActorIdentity, *, note: str | None = None) -> Reply:
        lines: list[str] = []
        if note:
            lines.extend((note, ""))
        lines.extend(
            (
                "👋 Добро пожаловать в диспетчерскую!",
                "",
                "Ты зарегистрирован как оператор.",
                "",
                (
                    "Команды доступны в меню слева от поля ввода 👈"
                    if identity.provider.value == "telegram"
                    else "Команды доступны по кнопкам ниже 👇"
                ),
            )
        )
        if identity.provider.value == "telegram":
            return Reply("\n".join(lines))
        return Reply(
            "\n".join(lines),
            buttons=(
                ReplyButton(
                    "📋 Активные заявки",
                    OPERATOR_LIST_ORDERS,
                    allowed_role="operator",
                ),
                ReplyButton(
                    "👨‍🔧 Мастера",
                    OPERATOR_LIST_MASTERS,
                    allowed_role="operator",
                    row=1,
                ),
                ReplyButton(
                    "📊 Статистика",
                    OPERATOR_STATS,
                    allowed_role="operator",
                    row=2,
                ),
            ),
        )

    async def handle_text(self, identity: ActorIdentity, text: str) -> Reply:
        value = text.strip()
        command = value.split(maxsplit=1)[0].split("@")[0].lower()
        if command in {"/start", "/menu"}:
            return await self.start(identity)
        if command == "/cancel":
            await self._clear_session(identity)
            return self.menu(identity, note="Добавление мастера отменено.")
        if command in {"/active", "/orders"}:
            await self._clear_session(identity)
            return await self.list_orders(identity)
        if command in {"/mastera", "/masters"}:
            await self._clear_session(identity)
            return await self.list_masters(identity)
        if command == "/stats":
            await self._clear_session(identity)
            return await self.statistics(identity)

        session = await self._session(identity)
        if session is None:
            return self.menu(identity, note="Выберите действие кнопкой ниже.")
        if session.get("flow") != "add_master":
            await self._clear_session(identity)
            return self.menu(identity, note="Незавершённое действие сброшено.")
        step = session.get("step")
        if step == "name":
            return await self._master_name(identity, session, value)
        if step == "phone":
            return await self._master_phone(identity, session, value)
        await self._clear_session(identity)
        return self.menu(identity, note="Незавершённое действие сброшено.")

    async def handle_callback(
        self,
        identity: ActorIdentity,
        action: str,
        payload: dict[str, Any],
    ) -> Reply:
        if action == OPERATOR_MENU:
            await self._clear_session(identity)
            return self.menu(identity)
        if action == OPERATOR_CANCEL:
            await self._clear_session(identity)
            return self.menu(identity, note="Добавление мастера отменено.")
        if action == OPERATOR_LIST_ORDERS:
            await self._clear_session(identity)
            return await self.list_orders(identity)
        if action == OPERATOR_OPEN_ORDER:
            await self._clear_session(identity)
            return await self.open_order(identity, str(payload.get("order_id") or ""))
        if action == OPERATOR_LIST_MASTERS:
            await self._clear_session(identity)
            return await self.list_masters(identity)
        if action == OPERATOR_ADD_MASTER:
            return await self.add_master(identity)
        if action == OPERATOR_MASTER_INFO:
            return await self.master_info(identity, str(payload.get("actor_id") or ""))
        if action == OPERATOR_CALL_MASTER:
            return await self.call_master(
                identity,
                str(payload.get("actor_id") or ""),
                str(payload.get("order_id") or ""),
            )
        if action == OPERATOR_CALL_CLIENT:
            return await self.call_client(
                identity,
                str(payload.get("order_id") or ""),
            )
        if action == OPERATOR_DELETE_MASTER:
            return await self.delete_master(
                identity, str(payload.get("actor_id") or "")
            )
        if action == OPERATOR_DELETE_MASTER_CONFIRM:
            return await self.delete_master_confirm(
                identity, str(payload.get("actor_id") or "")
            )
        if action == OPERATOR_STATS:
            await self._clear_session(identity)
            return await self.statistics(identity)
        return self.menu(identity, note="Неизвестное действие.")

    async def add_master(self, identity: ActorIdentity) -> Reply:
        await self._sessions.put(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            role="operator",
            provider=identity.provider,
            state={
                "flow": "add_master",
                "step": "name",
                "request_key": token_urlsafe(18),
            },
        )
        return Reply(
            "➕ Добавление мастера\n\nВведите имя мастера:",
            buttons=(
                ReplyButton(
                    "Отмена",
                    OPERATOR_CANCEL,
                    allowed_role="operator",
                ),
            ),
        )

    async def list_masters(
        self, identity: ActorIdentity, *, note: str | None = None
    ) -> Reply:
        actors = await self._identities.list_actors(
            organization_id=identity.organization_id,
            role="master",
        )
        lines: list[str] = []
        if note:
            lines.extend((note, ""))
        lines.append("👨‍🔧 Мастера:")
        buttons: list[ReplyButton] = []
        if not actors:
            lines.append("Мастеров пока нет.")
        for index, actor in enumerate(actors):
            channels = tuple(str(item) for item in actor.get("channels") or ())
            if actor.get("has_bind_code"):
                state = "⚫ ожидает привязки"
            elif channels:
                state = "✅ " + ", ".join(channels)
            else:
                state = "⚫ не привязан"
            lines.append(f"• {actor['display_name']} — {state}")
            buttons.append(
                ReplyButton(
                    f"ℹ️ {actor['display_name']}",
                    OPERATOR_MASTER_INFO,
                    {"actor_id": actor["id"]},
                    "operator",
                    row=index,
                )
            )
        buttons.extend(
            (
                ReplyButton(
                    "➕ Добавить мастера",
                    OPERATOR_ADD_MASTER,
                    allowed_role="operator",
                    row=len(buttons),
                ),
                ReplyButton(
                    "⬅ Главное меню",
                    OPERATOR_MENU,
                    allowed_role="operator",
                    row=len(buttons) + 1,
                ),
            )
        )
        return Reply("\n".join(lines), buttons=tuple(buttons))

    async def master_info(self, identity: ActorIdentity, actor_id: str) -> Reply:
        actor = await self._identities.get_actor(
            organization_id=identity.organization_id,
            actor_id=actor_id,
        )
        if actor is None or "master" not in set(actor.get("roles") or ()):
            return await self.list_masters(identity, note="Мастер не найден.")
        return Reply(
            "\n".join(
                (
                    "🧑‍🔧 Карточка мастера",
                    "",
                    f"Имя: {actor['display_name']}",
                    "📞 Телефон:",
                    phone_display(actor.get("phone")),
                    f"ID: {actor['id']}",
                )
            ),
            buttons=(
                ReplyButton(
                    "📞 Позвонить мастеру",
                    OPERATOR_CALL_MASTER,
                    {"actor_id": actor_id},
                    "operator",
                ),
                ReplyButton(
                    "🗑 Убрать роль мастера",
                    OPERATOR_DELETE_MASTER,
                    {"actor_id": actor_id},
                    "operator",
                    row=1,
                ),
                ReplyButton(
                    "⬅ К мастерам",
                    OPERATOR_LIST_MASTERS,
                    allowed_role="operator",
                    row=2,
                ),
            ),
        )

    async def call_master(
        self,
        identity: ActorIdentity,
        actor_id: str,
        order_id: str = "",
    ) -> Reply:
        actor = await self._identities.get_actor(
            organization_id=identity.organization_id,
            actor_id=actor_id,
        )
        if actor is None or "master" not in set(actor.get("roles") or ()):
            return await self.list_masters(identity, note="Мастер не найден.")
        heading = (
            f"📞 Мастер по заявке {order_id}"
            if order_id
            else f"📞 Мастер {actor['display_name']}"
        )
        return Reply(
            f"{heading}\n\n{phone_display(actor.get('phone'))}",
            buttons=(
                ReplyButton(
                    "⬅ К мастеру",
                    OPERATOR_MASTER_INFO,
                    {"actor_id": actor_id},
                    "operator",
                ),
            ),
        )

    async def call_client(
        self,
        identity: ActorIdentity,
        order_id: str,
    ) -> Reply:
        order = await self._views.get_active_order(
            organization_id=identity.organization_id,
            role="operator",
            actor_id=identity.actor_id,
            order_id=order_id,
        )
        if order is None:
            return self.menu(identity, note="Заявка не найдена или уже закрыта.")
        details = dict(order.get("details") or {})
        return Reply(
            (
                f"📞 Клиент по заявке {order_number(order)}\n\n"
                f"{phone_display(details.get('phone') or details.get('client_phone'))}"
            ),
            buttons=(
                ReplyButton(
                    "⬅ К заявке",
                    OPERATOR_OPEN_ORDER,
                    {"order_id": order_id},
                    "operator",
                ),
            ),
        )

    async def delete_master(self, identity: ActorIdentity, actor_id: str) -> Reply:
        actor = await self._identities.get_actor(
            organization_id=identity.organization_id,
            actor_id=actor_id,
        )
        if actor is None or "master" not in set(actor.get("roles") or ()):
            return await self.list_masters(identity, note="Мастер не найден.")
        return Reply(
            f"Убрать роль мастера у «{actor['display_name']}»?",
            buttons=(
                ReplyButton(
                    "Да, убрать роль",
                    OPERATOR_DELETE_MASTER_CONFIRM,
                    {"actor_id": actor_id},
                    "operator",
                ),
                ReplyButton(
                    "Отмена",
                    OPERATOR_LIST_MASTERS,
                    allowed_role="operator",
                    row=1,
                ),
            ),
        )

    async def delete_master_confirm(
        self, identity: ActorIdentity, actor_id: str
    ) -> Reply:
        if await self._views.master_has_active_orders(
            organization_id=identity.organization_id,
            master_id=actor_id,
        ):
            return await self.list_masters(
                identity,
                note=(
                    "Роль не снята: у мастера есть активная заявка. "
                    "Сначала завершите или переназначьте её."
                ),
            )
        removed = await self._identities.revoke_role(
            organization_id=identity.organization_id,
            actor_id=actor_id,
            role="master",
        )
        note = "Роль мастера снята." if removed else "Мастер уже недоступен."
        return await self.list_masters(identity, note=note)

    async def list_orders(self, identity: ActorIdentity) -> Reply:
        orders = await self._views.list_active_orders(
            organization_id=identity.organization_id,
            role="operator",
            actor_id=identity.actor_id,
        )
        return _orders_reply(
            orders,
            title="📋 Активные заявки:",
            open_action=OPERATOR_OPEN_ORDER,
            allowed_role="operator",
            empty_text="Активных заявок нет.",
            back_action=OPERATOR_MENU,
        )

    async def open_order(self, identity: ActorIdentity, order_id: str) -> Reply:
        order = await self._views.get_active_order(
            organization_id=identity.organization_id,
            role="operator",
            actor_id=identity.actor_id,
            order_id=order_id,
        )
        if order is None:
            return self.menu(identity, note="Заявка не найдена или уже закрыта.")
        buttons: list[ReplyButton] = []
        details = dict(order.get("details") or {})
        if details.get("phone") or details.get("client_phone"):
            buttons.append(
                ReplyButton(
                    "📞 Позвонить клиенту",
                    OPERATOR_CALL_CLIENT,
                    {"order_id": order_id},
                    "operator",
                )
            )
        if order.get("assignee_id"):
            buttons.append(
                ReplyButton(
                    "📞 Позвонить мастеру",
                    OPERATOR_CALL_MASTER,
                    {
                        "actor_id": str(order["assignee_id"]),
                        "order_id": order_number(order),
                    },
                    "operator",
                    row=len(buttons),
                )
            )
        if order["status"] == "submitted":
            buttons.append(
                ReplyButton(
                    "📢 Сбросить в пул",
                    "pool_publish",
                    {"order_id": order_id},
                    "operator",
                    row=len(buttons),
                )
            )
        buttons.append(
            ReplyButton(
                "⬅ К активным заявкам",
                OPERATOR_LIST_ORDERS,
                allowed_role="operator",
                row=len(buttons),
            )
        )
        return Reply(await self._order_card(identity, order), buttons=tuple(buttons))

    async def statistics(self, identity: ActorIdentity) -> Reply:
        values = await self._views.statistics(organization_id=identity.organization_id)
        return Reply(
            "\n".join(
                (
                    "📊 Статистика",
                    "",
                    f"Всего заявок: {values['total']}",
                    f"Активных: {values['active']}",
                    f"Новых: {values['submitted']}",
                    f"Закрыто сегодня: {values['completed_today']}",
                    (
                        "Привязанных мастеров: "
                        f"{values['masters_bound']}/{values['masters_total']}"
                    ),
                )
            ),
            buttons=(
                ReplyButton(
                    "⬅ Главное меню",
                    OPERATOR_MENU,
                    allowed_role="operator",
                ),
            ),
        )

    async def _master_name(
        self,
        identity: ActorIdentity,
        session: dict[str, Any],
        value: str,
    ) -> Reply:
        name = " ".join(value.split())[:200]
        if not name:
            return Reply("Имя не может быть пустым. Введите имя мастера:")
        await self._sessions.put(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            role="operator",
            provider=identity.provider,
            state={
                "flow": "add_master",
                "step": "phone",
                "name": name,
                "request_key": session["request_key"],
            },
        )
        return Reply(
            f"Имя: {name}\n\nВведите телефон мастера полностью, "
            "например 89991112233 или +358401234567:"
        )

    async def _master_phone(
        self,
        identity: ActorIdentity,
        session: dict[str, Any],
        value: str,
    ) -> Reply:
        phone = normalize_phone(value)
        if phone is None:
            return Reply(
                "Введите номер полностью: от 10 до 15 цифр, например "
                "89991112233 или +358401234567."
            )
        name = str(session.get("name") or "").strip()
        if not name:
            await self._clear_session(identity)
            return self.menu(identity, note="Имя мастера потеряно, начните заново.")
        created = await self._identities.create_staff_actor(
            organization_id=identity.organization_id,
            role="master",
            name=name,
            phone=phone,
            request_key=str(session.get("request_key") or ""),
        )
        await self._clear_session(identity)
        return Reply(
            "\n".join(
                (
                    "✅ Мастер создан!",
                    "",
                    f"Имя: {created['name']}",
                    f"Телефон: {created['phone']}",
                    f"Одноразовый код привязки: {created['bind_code']}",
                    "",
                    "Передайте код мастеру. Он нажимает /start в мастерском "
                    "Telegram-боте или общем staff-боте MAX и вводит код.",
                )
            ),
            buttons=(
                ReplyButton(
                    "👨‍🔧 К мастерам",
                    OPERATOR_LIST_MASTERS,
                    allowed_role="operator",
                ),
                ReplyButton(
                    "⬅ Главное меню",
                    OPERATOR_MENU,
                    allowed_role="operator",
                    row=1,
                ),
            ),
        )

    async def _order_card(self, identity: ActorIdentity, order: dict[str, Any]) -> str:
        pack = None
        if self._packs is not None:
            pack = await self._packs.active(identity.organization_id)
        return operator_order_card(order, pack=pack)

    async def _session(self, identity: ActorIdentity) -> dict[str, Any] | None:
        return await self._sessions.get(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            role="operator",
            provider=identity.provider,
        )

    async def _clear_session(self, identity: ActorIdentity) -> None:
        await self._sessions.clear(
            organization_id=identity.organization_id,
            actor_id=identity.actor_id,
            role="operator",
            provider=identity.provider,
        )


class MasterCoordinator:
    """Messenger-only master landing menu and active-order navigation."""

    def __init__(
        self,
        *,
        views: PostgresStaffViewStore,
        packs: PostgresPackStore | None = None,
    ) -> None:
        self._views = views
        self._packs = packs

    async def start(self, identity: ActorIdentity) -> Reply:
        return Reply(
            "\n".join(
                (
                    "Рабочее место мастера",
                    f"Здравствуйте, {identity.display_name}!",
                    "Здесь находятся назначенные вам заявки.",
                )
            ),
            buttons=(
                ReplyButton(
                    "📋 Мои заявки",
                    MASTER_LIST_ORDERS,
                    allowed_role="master",
                ),
            ),
        )

    async def handle_text(self, identity: ActorIdentity, text: str) -> Reply:
        command = text.strip().split(maxsplit=1)[0].split("@")[0].lower()
        if command in {"/active", "/orders"}:
            return await self.list_orders(identity)
        return await self.start(identity)

    async def handle_callback(
        self,
        identity: ActorIdentity,
        action: str,
        payload: dict[str, Any],
    ) -> Reply:
        if action == MASTER_LIST_ORDERS:
            return await self.list_orders(identity)
        if action == MASTER_OPEN_ORDER:
            return await self.open_order(identity, str(payload.get("order_id") or ""))
        return await self.start(identity)

    async def list_orders(self, identity: ActorIdentity) -> Reply:
        orders = await self._views.list_active_orders(
            organization_id=identity.organization_id,
            role="master",
            actor_id=identity.actor_id,
        )
        return _orders_reply(
            orders,
            title="📋 Мои активные заявки:",
            open_action=MASTER_OPEN_ORDER,
            allowed_role="master",
            empty_text="Активных заявок нет. Ожидайте назначения оператора.",
            back_action=MASTER_MENU,
        )

    async def open_order(self, identity: ActorIdentity, order_id: str) -> Reply:
        order = await self._views.get_active_order(
            organization_id=identity.organization_id,
            role="master",
            actor_id=identity.actor_id,
            order_id=order_id,
        )
        if order is None:
            return Reply(
                "Заявка не найдена, закрыта или назначена другому мастеру.",
                buttons=(
                    ReplyButton(
                        "📋 Мои заявки",
                        MASTER_LIST_ORDERS,
                        allowed_role="master",
                    ),
                ),
            )
        status = str(order["status"])
        buttons: list[ReplyButton] = []
        if status == "assigned":
            buttons.extend(
                (
                    ReplyButton(
                        "✅ Принять",
                        "accept",
                        {"order_id": order_id},
                        "master",
                    ),
                    ReplyButton(
                        "❌ Отказаться",
                        "reject",
                        {"order_id": order_id},
                        "master",
                        row=1,
                    ),
                )
            )
        elif status == "accepted":
            buttons.extend(
                (
                    ReplyButton(
                        "🚗 Выехал",
                        "start_travel",
                        {"order_id": order_id},
                        "master",
                    ),
                    ReplyButton(
                        "🔧 Начать на месте",
                        "start_work",
                        {"order_id": order_id},
                        "master",
                        row=1,
                    ),
                )
            )
        elif status == "en_route":
            buttons.append(
                ReplyButton(
                    "🔧 Начать работу",
                    "start_work",
                    {"order_id": order_id},
                    "master",
                )
            )
        elif status == "in_progress":
            buttons.append(
                ReplyButton(
                    "✅ Передать отчёт",
                    "submit_report",
                    {"order_id": order_id},
                    "master",
                )
            )
        buttons.append(
            ReplyButton(
                "⬅ К моим заявкам",
                MASTER_LIST_ORDERS,
                allowed_role="master",
                row=len(buttons),
            )
        )
        return Reply(await self._order_card(identity, order), buttons=tuple(buttons))

    async def _order_card(self, identity: ActorIdentity, order: dict[str, Any]) -> str:
        pack = None
        if self._packs is not None:
            pack = await self._packs.active(identity.organization_id)
        return master_order_card(order, pack=pack)


def _orders_reply(
    orders: list[dict[str, Any]],
    *,
    title: str,
    open_action: str,
    allowed_role: str,
    empty_text: str,
    back_action: str,
) -> Reply:
    lines = [title]
    buttons: list[ReplyButton] = []
    if not orders:
        lines.append(empty_text)
    for index, order in enumerate(orders):
        order_id = str(order["id"])
        number = order_number(order)
        buttons.append(
            ReplyButton(
                f"{status_emoji(order['status'])} {number}",
                open_action,
                {"order_id": order_id},
                allowed_role,
                row=index,
            )
        )
    buttons.append(
        ReplyButton(
            "⬅ Главное меню",
            back_action,
            allowed_role=allowed_role,
            row=len(buttons),
        )
    )
    return Reply("\n".join(lines), buttons=tuple(buttons))
