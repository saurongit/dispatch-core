from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from dispatch_core.packs.catalog import PackDefinition

STATUS_LABELS = {
    "submitted": "🆕 Новая",
    "pool_open": "📢 В пуле",
    "assigned": "👤 Назначена",
    "accepted": "✅ Принята",
    "en_route": "🚗 Выехал",
    "in_progress": "📍 На месте",
    "completed": "✅ Завершена",
    "cancelled": "❌ Отменена",
}

_MASTER_STATUS_EMOJI = {
    "submitted": "🆕",
    "pool_open": "📢",
    "assigned": "🟡",
    "accepted": "✅",
    "en_route": "🚗",
    "in_progress": "🔧",
    "completed": "✅",
    "cancelled": "❌",
}

_CORE_DETAIL_KEYS = frozenset(
    {
        "address",
        "client_name",
        "comment",
        "description",
        "destination",
        "fault",
        "name",
        "phone",
        "problem",
        "schedule_note",
        "scheduled_at",
        "service_keys",
        "service_location",
        "services",
        "summary",
    }
)


class CardRenderer:
    """Deterministic, template-free rendering shared by intake and projector."""

    def __init__(self, pack: PackDefinition) -> None:
        self._pack = pack

    def greeting(self) -> str:
        branding = self._pack.branding
        if branding.greeting:
            return branding.greeting
        return f"Здравствуйте! Это {branding.name}. Выберите услугу."

    def order_card(self, *, work_type: str, details: Mapping[str, Any]) -> str:
        return self._render(
            header=self._pack.branding.name,
            details=details,
            fallback=work_type,
        )

    def confirmation_card(
        self,
        *,
        service_labels: Sequence[str],
        field_values: Mapping[str, Any],
    ) -> str:
        details: dict[str, Any] = dict(field_values)
        if service_labels:
            details["services"] = list(service_labels)
        body = self._render(
            header="Проверьте заявку",
            details=details,
            fallback=self._pack.branding.name,
        )
        return f"{body}\n\nПодтвердить отправку?"

    def _render(
        self,
        *,
        header: str,
        details: Mapping[str, Any],
        fallback: str,
    ) -> str:
        lines: list[str] = [header] if header else []
        services = details.get("services")
        if services:
            lines.append("Услуги: " + ", ".join(str(item) for item in services))
        rendered_field = False
        for definition in self._pack.ordered_fields():
            value = details.get(definition.key)
            if value in (None, ""):
                continue
            lines.append(f"{definition.label}: {value}")
            rendered_field = True
        if not services and not rendered_field:
            lines.append(fallback)
        return "\n".join(lines)


def normalize_phone(value: str) -> str | None:
    stripped = value.strip()
    international = stripped.startswith("+")
    digits = "".join(character for character in stripped if character.isdigit())
    if international:
        if not 10 <= len(digits) <= 15:
            return None
        return f"+{digits}"
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if not 11 <= len(digits) <= 15:
        return None
    if len(digits) == 11 and digits.startswith("7"):
        return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    return f"+{digits}"


def phone_display(value: Any) -> str:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return f"+{digits}" if digits else "—"


def order_number(order: Mapping[str, Any]) -> str:
    return str(order.get("public_number") or order.get("id") or "—")


def status_label(status: Any) -> str:
    value = str(status or "")
    return STATUS_LABELS.get(value, value or "❓ Неизвестен")


def status_emoji(status: Any) -> str:
    return status_label(status).split(maxsplit=1)[0]


def operator_order_card(
    order: Mapping[str, Any],
    *,
    pack: PackDefinition | None = None,
) -> str:
    details = _details(order)
    status = str(order.get("status") or "")
    client_label = _role_label(pack, "client", "Клиент")
    master_label = _role_label(pack, "master", "Мастер")
    lines = [
        f"{status_emoji(status)} Заявка {order_number(order)}",
        "",
        f"👤 {client_label}: {_first(details, 'client_name', 'name') or 'не указан'}",
        "📞 Телефон:",
        phone_display(_first(details, "phone", "client_phone")),
        "⬇️ Нажмите на номер выше ⬇️",
        f"📍 Адрес объекта: {_first(details, 'address', 'destination') or '—'}",
        f"🧰 Услуги: {_services(details, str(order.get('work_type') or '—'))}",
    ]
    description = _first(
        details,
        "description",
        "summary",
        "fault",
        "problem",
        "comment",
    )
    if description:
        lines.append(f"📝 Описание: {description}")
    schedule = _first(details, "schedule_note", "scheduled_at")
    if schedule:
        lines.append(f"🗓 Когда нужен мастер: {_datetime_text(schedule)}")
    lines.extend(
        (
            f"📌 Статус: {status_label(status)}",
            f"🕐 Создана: {_datetime_text(order.get('created_at'))}",
            f"🌐 Источник: {_source_label(order.get('source'))}",
        )
    )
    if order.get("master_name"):
        lines.append(f"👨‍🔧 {master_label}: {order['master_name']}")
    lines.extend(_extra_fields(details, pack))
    return "\n".join(lines)


def master_order_card(
    order: Mapping[str, Any],
    *,
    pack: PackDefinition | None = None,
) -> str:
    details = _details(order)
    status = str(order.get("status") or "")
    client_label = _role_label(pack, "client", "Клиент")
    master_label = _role_label(pack, "master", "Мастер")
    lines = [
        f"{_MASTER_STATUS_EMOJI.get(status, '📄')} Заявка {order_number(order)}",
        "",
        f"👤 {client_label}: {_first(details, 'client_name', 'name') or '—'}",
        "📞 Телефон:",
        phone_display(_first(details, "phone", "client_phone")),
        f"📍 Адрес объекта: {_first(details, 'address', 'destination') or '—'}",
        (
            "📝 Описание: "
            + (
                _first(
                    details,
                    "description",
                    "summary",
                    "fault",
                    "problem",
                    "comment",
                )
                or _services(details, str(order.get("work_type") or "—"))
            )
        ),
    ]
    schedule = _first(details, "schedule_note", "scheduled_at")
    if schedule:
        lines.append(f"🗓 Когда нужен мастер: {_datetime_text(schedule)}")
    lines.append(f"📊 Статус: {status_label(status)}")
    if order.get("master_name"):
        lines.append(f"👨‍🔧 {master_label}: {order['master_name']}")
    lines.extend(_extra_fields(details, pack))
    return "\n".join(lines)


def _details(order: Mapping[str, Any]) -> dict[str, Any]:
    value = order.get("details")
    return dict(value) if isinstance(value, Mapping) else {}


def _first(details: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = details.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _services(details: Mapping[str, Any], fallback: str) -> str:
    value = details.get("services")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        labels = ", ".join(str(item) for item in value if str(item))
        if labels:
            return labels
    if value not in (None, ""):
        return str(value)
    return fallback


def _extra_fields(
    details: Mapping[str, Any],
    pack: PackDefinition | None,
) -> list[str]:
    if pack is None:
        return []
    lines: list[str] = []
    for definition in pack.ordered_fields():
        if definition.key in _CORE_DETAIL_KEYS:
            continue
        value = details.get(definition.key)
        if value not in (None, ""):
            lines.append(f"🔹 {definition.label}: {value}")
    return lines


def _role_label(
    pack: PackDefinition | None,
    role: str,
    fallback: str,
) -> str:
    if pack is None:
        return fallback
    return str(pack.role_labels.get(role) or fallback)


def _datetime_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    return str(value or "—")


def _source_label(value: Any) -> str:
    source = str(value or "")
    if source.startswith("telegram"):
        return "Telegram"
    if source.startswith("max"):
        return "MAX"
    if source.startswith("site") or source.startswith("web"):
        return "Сайт"
    if source in {"dispatcher_phone_call", "phone"}:
        return "Телефон"
    return source or "—"
