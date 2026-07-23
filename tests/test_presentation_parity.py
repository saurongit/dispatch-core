from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.messaging.cards import master_order_card, operator_order_card
from dispatch_core.messaging.models import Provider
from dispatch_core.messaging.workspaces import (
    OPERATOR_LIST_MASTERS,
    OPERATOR_LIST_ORDERS,
    OperatorCoordinator,
)


def _order() -> dict[str, Any]:
    return {
        "id": "internal-uuid",
        "public_number": "S777",
        "work_type": "repair",
        "source": "telegram:7001",
        "details": {
            "client_name": "Иван",
            "phone": "+7 (999) 123-45-67",
            "address": "ул. Ленина, 10",
            "services": ["Ремонт лифта"],
            "description": "Не закрываются двери",
            "schedule_note": "Сегодня, 14:00",
        },
        "status": "assigned",
        "assignee_id": "master-1",
        "coordinator_id": "operator-1",
        "master_name": "Антон",
        "created_at": datetime(2026, 7, 23, 10, 5, tzinfo=UTC),
    }


def test_operator_card_matches_the_canonical_snapshot() -> None:
    assert operator_order_card(_order()) == (
        "👤 Заявка S777\n\n"
        "👤 Клиент: Иван\n"
        "📞 Телефон:\n"
        "+79991234567\n"
        "⬇️ Нажмите на номер выше ⬇️\n"
        "📍 Адрес объекта: ул. Ленина, 10\n"
        "🧰 Услуги: Ремонт лифта\n"
        "📝 Описание: Не закрываются двери\n"
        "🗓 Когда нужен мастер: Сегодня, 14:00\n"
        "📌 Статус: 👤 Назначена\n"
        "🕐 Создана: 23.07.2026 10:05\n"
        "🌐 Источник: Telegram\n"
        "👨‍🔧 Мастер: Антон"
    )


def test_master_card_matches_the_canonical_snapshot() -> None:
    assert master_order_card(_order()) == (
        "🟡 Заявка S777\n\n"
        "👤 Клиент: Иван\n"
        "📞 Телефон:\n"
        "+79991234567\n"
        "📍 Адрес объекта: ул. Ленина, 10\n"
        "📝 Описание: Не закрываются двери\n"
        "🗓 Когда нужен мастер: Сегодня, 14:00\n"
        "📊 Статус: 👤 Назначена\n"
        "👨‍🔧 Мастер: Антон"
    )


@dataclass
class _Sessions:
    async def get(self, **values: Any) -> None:
        return None

    async def put(self, **values: Any) -> None:
        return None

    async def clear(self, **values: Any) -> None:
        return None


@dataclass
class _Identities:
    async def list_actors(self, **values: Any) -> list[dict[str, Any]]:
        return []

    async def get_actor(self, **values: Any) -> None:
        return None


@dataclass
class _Views:
    orders: list[dict[str, Any]] = field(default_factory=lambda: [_order()])

    async def list_active_orders(self, **values: Any) -> list[dict[str, Any]]:
        return list(self.orders)

    async def get_active_order(self, **values: Any) -> dict[str, Any] | None:
        return next(
            (item for item in self.orders if item["id"] == values["order_id"]),
            None,
        )


def _operator(provider: Provider) -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="operator-1",
        role="operator",
        display_name="Михаил",
        provider=provider,
        external_user_id="7001",
    )


@pytest.mark.asyncio
async def test_order_view_is_semantically_identical_for_telegram_and_max() -> None:
    target = OperatorCoordinator(
        identities=_Identities(),  # type: ignore[arg-type]
        sessions=_Sessions(),  # type: ignore[arg-type]
        views=_Views(),  # type: ignore[arg-type]
    )

    telegram = await target.open_order(_operator(Provider.TELEGRAM), "internal-uuid")
    maximum = await target.open_order(_operator(Provider.MAX), "internal-uuid")

    assert telegram == maximum
    assert "S777" in telegram.text
    assert "internal-uuid" not in telegram.text


@pytest.mark.asyncio
async def test_top_level_navigation_differs_only_by_platform_capability() -> None:
    target = OperatorCoordinator(
        identities=_Identities(),  # type: ignore[arg-type]
        sessions=_Sessions(),  # type: ignore[arg-type]
        views=_Views(),  # type: ignore[arg-type]
    )

    telegram = await target.start(_operator(Provider.TELEGRAM))
    maximum = await target.start(_operator(Provider.MAX))

    assert telegram.buttons == ()
    assert {button.action for button in maximum.buttons} >= {
        OPERATOR_LIST_ORDERS,
        OPERATOR_LIST_MASTERS,
    }
    assert "слева от поля ввода" in telegram.text
    assert "по кнопкам ниже" in maximum.text
