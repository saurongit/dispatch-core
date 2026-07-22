from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dispatch_core.packs.catalog import PackDefinition


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
        return self._render(header=self._pack.branding.name, details=details,
                            fallback=work_type)

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
