from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.messaging.models import Provider
from dispatch_core.messaging.workspaces import (
    MASTER_LIST_ORDERS,
    MASTER_MENU,
    MASTER_OPEN_ORDER,
    OPERATOR_ADD_MASTER,
    OPERATOR_CALL_MASTER,
    OPERATOR_CANCEL,
    OPERATOR_DELETE_MASTER,
    OPERATOR_DELETE_MASTER_CONFIRM,
    OPERATOR_LIST_MASTERS,
    OPERATOR_LIST_ORDERS,
    OPERATOR_MASTER_INFO,
    OPERATOR_MENU,
    OPERATOR_OPEN_ORDER,
    OPERATOR_STATS,
    MasterCoordinator,
    OperatorCoordinator,
)


def identity(role: str, provider: Provider = Provider.TELEGRAM) -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="owner-1" if role == "operator" else "master-1",
        role=role,
        display_name="Тестовый сотрудник",
        provider=provider,
        external_user_id="7001",
        roles=frozenset({role}),
    )


@dataclass
class FakeSessions:
    values: dict[tuple[str, str, str, Provider], dict[str, Any]] = field(
        default_factory=dict
    )

    async def get(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
    ) -> dict[str, Any] | None:
        return self.values.get((organization_id, actor_id, role, provider))

    async def put(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
        state: dict[str, Any],
    ) -> None:
        self.values[(organization_id, actor_id, role, provider)] = dict(state)

    async def clear(
        self,
        *,
        organization_id: str,
        actor_id: str,
        role: str,
        provider: Provider,
    ) -> None:
        self.values.pop((organization_id, actor_id, role, provider), None)


@dataclass
class FakeIdentities:
    actors: list[dict[str, Any]] = field(default_factory=list)
    created: list[dict[str, Any]] = field(default_factory=list)
    revoked: list[tuple[str, str]] = field(default_factory=list)
    revoke_result: bool = True

    async def create_staff_actor(self, **values: Any) -> dict[str, Any]:
        self.created.append(values)
        return {
            "actor_id": "master-new",
            "name": values["name"],
            "phone": values["phone"],
            "bind_code": "4321",
        }

    async def list_actors(self, **values: Any) -> list[dict[str, Any]]:
        return list(self.actors)

    async def get_actor(self, **values: Any) -> dict[str, Any] | None:
        actor_id = values["actor_id"]
        return next((item for item in self.actors if item["id"] == actor_id), None)

    async def revoke_role(self, **values: Any) -> bool:
        self.revoked.append((values["actor_id"], values["role"]))
        return self.revoke_result


@dataclass
class FakeViews:
    orders: list[dict[str, Any]] = field(default_factory=list)
    busy_master_ids: set[str] = field(default_factory=set)

    async def list_active_orders(self, **values: Any) -> list[dict[str, Any]]:
        role = values["role"]
        actor_id = values["actor_id"]
        if role == "master":
            return [item for item in self.orders if item["assignee_id"] == actor_id]
        return list(self.orders)

    async def get_active_order(self, **values: Any) -> dict[str, Any] | None:
        role = values["role"]
        actor_id = values["actor_id"]
        order_id = values["order_id"]
        for item in self.orders:
            if item["id"] != order_id:
                continue
            if role == "master" and item["assignee_id"] != actor_id:
                return None
            return item
        return None

    async def statistics(self, **values: Any) -> dict[str, int]:
        return {
            "total": 8,
            "active": 3,
            "submitted": 1,
            "completed_today": 2,
            "masters_total": 4,
            "masters_bound": 3,
        }

    async def master_has_active_orders(self, **values: Any) -> bool:
        return values["master_id"] in self.busy_master_ids


def order(status: str = "submitted") -> dict[str, Any]:
    return {
        "id": "order-1",
        "public_number": "S777",
        "work_type": "lift_repair",
        "source": "phone",
        "details": {"summary": "Лифт остановился", "address": "Дом 7"},
        "status": status,
        "assignee_id": "master-1" if status != "submitted" else None,
        "coordinator_id": "owner-1",
    }


@pytest.mark.parametrize("provider", [Provider.TELEGRAM, Provider.MAX])
@pytest.mark.asyncio
async def test_operator_creates_master_through_same_flow_for_both_providers(
    provider: Provider,
) -> None:
    sessions = FakeSessions()
    identities = FakeIdentities()
    target = OperatorCoordinator(
        identities=identities,  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
        views=FakeViews(),  # type: ignore[arg-type]
    )
    operator = identity("operator", provider)

    menu = await target.start(operator)
    if provider is Provider.TELEGRAM:
        assert menu.buttons == ()
        assert "слева от поля ввода" in menu.text
    else:
        assert {button.action for button in menu.buttons} >= {
            OPERATOR_LIST_ORDERS,
            OPERATOR_LIST_MASTERS,
        }
    prompt = await target.handle_callback(operator, OPERATOR_ADD_MASTER, {})
    assert "имя мастера" in prompt.text.lower()
    phone_prompt = await target.handle_text(operator, "Антон")
    assert "телефон" in phone_prompt.text.lower()
    invalid = await target.handle_text(operator, "123")
    assert "полностью" in invalid.text.lower()
    created = await target.handle_text(operator, "8 (999) 111-22-33")

    assert len(identities.created) == 1
    creation = identities.created[0]
    assert creation.pop("request_key")
    assert creation == {
        "organization_id": "org-1",
        "role": "master",
        "name": "Антон",
        "phone": "+7 (999) 111-22-33",
    }
    assert "4321" in created.text
    assert "одноразовый" in created.text.lower()
    assert sessions.values == {}


@pytest.mark.asyncio
async def test_operator_navigation_cancels_pending_master_creation() -> None:
    sessions = FakeSessions()
    target = OperatorCoordinator(
        identities=FakeIdentities(),  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
        views=FakeViews(),  # type: ignore[arg-type]
    )
    operator = identity("operator")

    await target.handle_callback(operator, OPERATOR_ADD_MASTER, {})
    await target.handle_text(operator, "Антон")
    assert sessions.values

    reply = await target.handle_text(operator, "/masters")

    assert "Мастера" in reply.text
    assert sessions.values == {}


@pytest.mark.asyncio
async def test_operator_cannot_revoke_busy_master_but_can_revoke_free_master() -> None:
    master = {
        "id": "master-1",
        "display_name": "Антон",
        "phone": "+7 (999) 111-22-33",
        "active": True,
        "roles": ["master"],
        "channels": ["telegram", "max"],
        "has_bind_code": False,
    }
    identities = FakeIdentities(actors=[master])
    views = FakeViews(busy_master_ids={"master-1"})
    target = OperatorCoordinator(
        identities=identities,  # type: ignore[arg-type]
        sessions=FakeSessions(),  # type: ignore[arg-type]
        views=views,  # type: ignore[arg-type]
    )
    operator = identity("operator")

    listing = await target.handle_callback(operator, OPERATOR_LIST_MASTERS, {})
    assert "Антон" in listing.text
    info = await target.handle_callback(
        operator, OPERATOR_MASTER_INFO, {"actor_id": "master-1"}
    )
    assert "+79991112233" in info.text
    assert OPERATOR_CALL_MASTER in {button.action for button in info.buttons}
    confirm = await target.handle_callback(
        operator, OPERATOR_DELETE_MASTER, {"actor_id": "master-1"}
    )
    assert any(
        button.action == OPERATOR_DELETE_MASTER_CONFIRM for button in confirm.buttons
    )
    blocked = await target.handle_callback(
        operator,
        OPERATOR_DELETE_MASTER_CONFIRM,
        {"actor_id": "master-1"},
    )
    assert "активн" in blocked.text.lower()
    assert identities.revoked == []

    views.busy_master_ids.clear()
    removed = await target.handle_callback(
        operator,
        OPERATOR_DELETE_MASTER_CONFIRM,
        {"actor_id": "master-1"},
    )
    assert "роль мастера снята" in removed.text.lower()
    assert identities.revoked == [("master-1", "master")]


@pytest.mark.asyncio
async def test_operator_and_master_order_menus_expose_only_valid_actions() -> None:
    views = FakeViews(orders=[order("submitted")])
    operator_target = OperatorCoordinator(
        identities=FakeIdentities(),  # type: ignore[arg-type]
        sessions=FakeSessions(),  # type: ignore[arg-type]
        views=views,  # type: ignore[arg-type]
    )
    operator = identity("operator")
    listing = await operator_target.handle_callback(operator, OPERATOR_LIST_ORDERS, {})
    assert any("S777" in button.text for button in listing.buttons)
    opened = await operator_target.handle_callback(
        operator, OPERATOR_OPEN_ORDER, {"order_id": "order-1"}
    )
    assert any(button.action == "pool_publish" for button in opened.buttons)

    views.orders = [order("assigned")]
    master_target = MasterCoordinator(views=views)  # type: ignore[arg-type]
    master = identity("master")
    master_menu = await master_target.start(master)
    assert any(button.action == MASTER_LIST_ORDERS for button in master_menu.buttons)
    master_list = await master_target.handle_callback(master, MASTER_LIST_ORDERS, {})
    open_button = next(
        button for button in master_list.buttons if "S777" in button.text
    )
    master_opened = await master_target.handle_callback(
        master, open_button.action, dict(open_button.payload)
    )
    assert {button.action for button in master_opened.buttons} >= {"accept", "reject"}


@pytest.mark.parametrize(
    ("status", "expected", "unexpected"),
    [
        ("accepted", {"start_travel", "start_work"}, {"accept"}),
        ("en_route", {"start_work"}, {"start_travel"}),
        ("in_progress", {"submit_report"}, {"accept", "start_work"}),
    ],
)
@pytest.mark.asyncio
async def test_master_order_card_follows_current_lifecycle_state(
    status: str,
    expected: set[str],
    unexpected: set[str],
) -> None:
    views = FakeViews(orders=[order(status)])
    target = MasterCoordinator(views=views)  # type: ignore[arg-type]
    reply = await target.open_order(identity("master"), "order-1")
    actions = {button.action for button in reply.buttons}
    assert actions >= expected
    assert actions.isdisjoint(unexpected)


@pytest.mark.asyncio
async def test_operator_commands_callbacks_and_stale_sessions_are_safe() -> None:
    sessions = FakeSessions()
    target = OperatorCoordinator(
        identities=FakeIdentities(),  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
        views=FakeViews(),  # type: ignore[arg-type]
    )
    operator = identity("operator")

    assert "диспетчерскую" in (await target.handle_text(operator, "/start@staff")).text
    assert "отменено" in (await target.handle_text(operator, "/cancel")).text
    assert "Активных заявок нет" in (await target.handle_text(operator, "/orders")).text
    assert "Статистика" in (await target.handle_text(operator, "/stats")).text
    assert (
        "Выберите действие" in (await target.handle_text(operator, "что делать")).text
    )

    session_key = ("org-1", "owner-1", "operator", Provider.TELEGRAM)
    sessions.values[session_key] = {"flow": "obsolete", "step": "name"}
    assert "сброшено" in (await target.handle_text(operator, "Антон")).text
    sessions.values[session_key] = {"flow": "add_master", "step": "obsolete"}
    assert "сброшено" in (await target.handle_text(operator, "Антон")).text

    for action in (OPERATOR_MENU, OPERATOR_CANCEL, OPERATOR_STATS):
        reply = await target.handle_callback(operator, action, {})
        assert reply.text
    assert (
        "Неизвестное"
        in (await target.handle_callback(operator, "operator_obsolete", {})).text
    )


@pytest.mark.asyncio
async def test_operator_handles_unbound_missing_and_already_removed_masters() -> None:
    actors = [
        {
            "id": "master-waiting",
            "display_name": "Ждёт код",
            "phone": None,
            "roles": ["master"],
            "channels": [],
            "has_bind_code": True,
        },
        {
            "id": "master-unbound",
            "display_name": "Без канала",
            "phone": None,
            "roles": ["master"],
            "channels": [],
            "has_bind_code": False,
        },
    ]
    identities = FakeIdentities(actors=actors, revoke_result=False)
    target = OperatorCoordinator(
        identities=identities,  # type: ignore[arg-type]
        sessions=FakeSessions(),  # type: ignore[arg-type]
        views=FakeViews(),  # type: ignore[arg-type]
    )
    operator = identity("operator")

    listing = await target.list_masters(operator, note="Обновлено")
    assert "ожидает привязки" in listing.text
    assert "не привязан" in listing.text
    assert "Мастер не найден" in (await target.master_info(operator, "missing")).text
    assert "Мастер не найден" in (await target.delete_master(operator, "missing")).text
    assert (
        "уже недоступен"
        in (await target.delete_master_confirm(operator, "master-unbound")).text
    )
    assert "не найдена" in (await target.open_order(operator, "missing")).text


@pytest.mark.asyncio
async def test_operator_recovers_from_lost_name_and_accepts_international_phone() -> (
    None
):
    sessions = FakeSessions()
    identities = FakeIdentities()
    target = OperatorCoordinator(
        identities=identities,  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
        views=FakeViews(),  # type: ignore[arg-type]
    )
    operator = identity("operator")
    session_key = ("org-1", "owner-1", "operator", Provider.TELEGRAM)

    sessions.values[session_key] = {
        "flow": "add_master",
        "step": "phone",
        "request_key": "request-1",
    }
    assert "потеряно" in (await target.handle_text(operator, "+358401234567")).text

    await target.handle_callback(operator, OPERATOR_ADD_MASTER, {})
    await target.handle_text(operator, "Тимо")
    assert "полностью" in (await target.handle_text(operator, "+123")).text
    created = await target.handle_text(operator, "+358 40 123 4567")
    assert "Мастер создан" in created.text
    assert identities.created[-1]["phone"] == "+358401234567"


@pytest.mark.asyncio
async def test_master_empty_missing_and_unknown_navigation_returns_safe_menus() -> None:
    views = FakeViews()
    target = MasterCoordinator(views=views)  # type: ignore[arg-type]
    master = identity("master")

    assert (
        "Активных заявок нет"
        in (await target.handle_text(master, "/active@staff")).text
    )
    assert "Рабочее место" in (await target.handle_text(master, "привет")).text
    assert (
        "не найдена"
        in (
            await target.handle_callback(
                master,
                MASTER_OPEN_ORDER,
                {"order_id": "missing"},
            )
        ).text
    )
    assert (
        "Рабочее место" in (await target.handle_callback(master, MASTER_MENU, {})).text
    )


@pytest.mark.asyncio
async def test_fallback_order_cards_support_destination_fault_and_unknown_status() -> (
    None
):
    fallback = order("custom_status")
    fallback["details"] = {"destination": "Склад 3", "fault": "Не заводится"}
    fallback["assignee_id"] = "master-1"
    views = FakeViews(orders=[fallback])

    operator_reply = await OperatorCoordinator(
        identities=FakeIdentities(),  # type: ignore[arg-type]
        sessions=FakeSessions(),  # type: ignore[arg-type]
        views=views,  # type: ignore[arg-type]
    ).open_order(identity("operator"), "order-1")
    assert "Склад 3" in operator_reply.text
    assert "Не заводится" in operator_reply.text
    assert "custom_status" in operator_reply.text

    master_reply = await MasterCoordinator(views=views).open_order(  # type: ignore[arg-type]
        identity("master"), "order-1"
    )
    assert {button.action for button in master_reply.buttons} == {MASTER_LIST_ORDERS}
