from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from dispatch_core.domain.work_order import EvidenceRequirements, PoolMode

DEFAULT_ROLE_LABELS: dict[str, str] = {
    "master": "Мастер",
    "operator": "Оператор",
    "client": "Клиент",
}


class FieldType(StrEnum):
    TEXT = "text"
    INTEGER = "integer"
    ADDRESS = "address"
    ASSET_REFERENCE = "asset_reference"
    ROUTE = "route"
    ENUM = "enum"


@dataclass(frozen=True, slots=True)
class FieldDefinition:
    key: str
    label: str
    field_type: FieldType
    required: bool = False
    choices: tuple[str, ...] = ()
    prompt: str = ""
    order: int = 0

    def __post_init__(self) -> None:
        if not self.key or not self.label:
            raise ValueError("field key and label are required")
        if self.field_type is FieldType.ENUM and not self.choices:
            raise ValueError("enum field requires choices")
        if self.field_type is not FieldType.ENUM and self.choices:
            raise ValueError("choices are valid only for enum fields")

    def ask(self) -> str:
        return self.prompt or f"Введите: {self.label}"

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "type": self.field_type.value,
            "required": self.required,
            "choices": list(self.choices),
            "prompt": self.prompt,
            "order": self.order,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> FieldDefinition:
        return cls(
            key=data["key"],
            label=data["label"],
            field_type=FieldType(data["type"]),
            required=bool(data.get("required", False)),
            choices=tuple(data.get("choices", ()) or ()),
            prompt=data.get("prompt", ""),
            order=int(data.get("order", 0)),
        )


@dataclass(frozen=True, slots=True)
class Branding:
    name: str
    greeting: str = ""
    support: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "greeting": self.greeting,
            "support": self.support,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Branding:
        return cls(
            name=data.get("name", ""),
            greeting=data.get("greeting", ""),
            support=data.get("support", ""),
        )


@dataclass(frozen=True, slots=True)
class ServiceCategory:
    key: str
    label: str

    def __post_init__(self) -> None:
        if not self.key or not self.label:
            raise ValueError("service category key and label are required")

    def to_json(self) -> dict[str, Any]:
        return {"key": self.key, "label": self.label}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ServiceCategory:
        return cls(key=data["key"], label=data["label"])


@dataclass(frozen=True, slots=True)
class ServiceCatalog:
    categories: tuple[ServiceCategory, ...] = ()
    multi_select: bool = True

    def __post_init__(self) -> None:
        keys = [category.key for category in self.categories]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate service category key")

    def label_for(self, key: str) -> str:
        for category in self.categories:
            if category.key == key:
                return category.label
        return key

    def to_json(self) -> dict[str, Any]:
        return {
            "multi_select": self.multi_select,
            "categories": [category.to_json() for category in self.categories],
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> ServiceCatalog:
        return cls(
            categories=tuple(
                ServiceCategory.from_json(item)
                for item in data.get("categories", ())
            ),
            multi_select=bool(data.get("multi_select", True)),
        )


def _evidence_to_json(evidence: EvidenceRequirements) -> dict[str, Any]:
    return {
        "minimum_photos": evidence.minimum_photos,
        "comment_required": evidence.comment_required,
        "signature_required": evidence.signature_required,
        "customer_code_required": evidence.customer_code_required,
    }


def _evidence_from_json(data: dict[str, Any]) -> EvidenceRequirements:
    return EvidenceRequirements(
        minimum_photos=int(data.get("minimum_photos", 0)),
        comment_required=bool(data.get("comment_required", False)),
        signature_required=bool(data.get("signature_required", False)),
        customer_code_required=bool(data.get("customer_code_required", False)),
    )


@dataclass(frozen=True, slots=True)
class PackDefinition:
    branding: Branding
    service_catalog: ServiceCatalog
    fields: tuple[FieldDefinition, ...]
    evidence: EvidenceRequirements
    role_labels: dict[str, str] = field(
        default_factory=lambda: dict(DEFAULT_ROLE_LABELS)
    )
    default_pool_mode: PoolMode = PoolMode.CURATED

    def ordered_fields(self) -> tuple[FieldDefinition, ...]:
        return tuple(sorted(self.fields, key=lambda item: (item.order, item.key)))

    def to_json(self) -> dict[str, Any]:
        return {
            "branding": self.branding.to_json(),
            "service_catalog": self.service_catalog.to_json(),
            "fields": [item.to_json() for item in self.fields],
            "evidence": _evidence_to_json(self.evidence),
            "role_labels": dict(self.role_labels),
            "lifecycle": {"default_pool_mode": self.default_pool_mode.value},
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> PackDefinition:
        lifecycle = data.get("lifecycle", {})
        role_labels = dict(DEFAULT_ROLE_LABELS)
        role_labels.update(data.get("role_labels", {}) or {})
        return cls(
            branding=Branding.from_json(data.get("branding", {})),
            service_catalog=ServiceCatalog.from_json(
                data.get("service_catalog", {})
            ),
            fields=tuple(
                FieldDefinition.from_json(item)
                for item in data.get("fields", ())
            ),
            evidence=_evidence_from_json(data.get("evidence", {})),
            role_labels=role_labels,
            default_pool_mode=PoolMode(
                lifecycle.get("default_pool_mode", PoolMode.CURATED.value)
            ),
        )


@dataclass(frozen=True, slots=True)
class IndustryPack:
    pack_id: str
    title: str
    work_types: tuple[str, ...]
    branding: Branding
    service_catalog: ServiceCatalog
    fields: tuple[FieldDefinition, ...]
    completion_evidence: EvidenceRequirements
    status: str = "template"

    def __post_init__(self) -> None:
        if not self.pack_id or not self.title or not self.work_types:
            raise ValueError("pack id, title and work types are required")
        field_keys = [item.key for item in self.fields]
        if len(field_keys) != len(set(field_keys)):
            raise ValueError(f"duplicate field key in pack {self.pack_id}")

    def to_definition(self) -> PackDefinition:
        return PackDefinition(
            branding=self.branding,
            service_catalog=self.service_catalog,
            fields=self.fields,
            evidence=self.completion_evidence,
        )


INDUSTRY_PACKS: tuple[IndustryPack, ...] = (
    IndustryPack(
        pack_id="field_service",
        title="Выездной сервис",
        work_types=("repair", "maintenance", "installation", "inspection"),
        branding=Branding(
            name="Выездной сервис",
            greeting="Здравствуйте! Выберите услугу, и мы примем вашу заявку.",
            support="",
        ),
        service_catalog=ServiceCatalog(
            categories=(
                ServiceCategory("repair", "Ремонт"),
                ServiceCategory("maintenance", "Обслуживание"),
                ServiceCategory("installation", "Установка"),
                ServiceCategory("inspection", "Диагностика"),
            ),
        ),
        fields=(
            FieldDefinition(
                "address",
                "Адрес",
                FieldType.ADDRESS,
                required=True,
                prompt="Введите адрес объекта",
                order=1,
            ),
            FieldDefinition(
                "asset",
                "Оборудование",
                FieldType.ASSET_REFERENCE,
                prompt="Укажите оборудование (или пропустите)",
                order=2,
            ),
            FieldDefinition(
                "fault",
                "Неисправность или задача",
                FieldType.TEXT,
                required=True,
                prompt="Опишите неисправность или задачу",
                order=3,
            ),
        ),
        completion_evidence=EvidenceRequirements(
            minimum_photos=1, comment_required=True
        ),
    ),
    IndustryPack(
        pack_id="local_delivery",
        title="Локальная доставка",
        work_types=("pickup", "delivery", "pickup_and_delivery", "return"),
        branding=Branding(
            name="Локальная доставка",
            greeting="Здравствуйте! Выберите тип доставки.",
        ),
        service_catalog=ServiceCatalog(
            categories=(
                ServiceCategory("pickup", "Приёмка"),
                ServiceCategory("delivery", "Доставка"),
                ServiceCategory("pickup_and_delivery", "Приёмка и доставка"),
                ServiceCategory("return", "Возврат"),
            ),
        ),
        fields=(
            FieldDefinition(
                "origin",
                "Откуда",
                FieldType.ADDRESS,
                required=True,
                prompt="Введите адрес отправления",
                order=1,
            ),
            FieldDefinition(
                "destination",
                "Куда",
                FieldType.ADDRESS,
                required=True,
                prompt="Введите адрес назначения",
                order=2,
            ),
            FieldDefinition(
                "cargo",
                "Груз",
                FieldType.TEXT,
                required=True,
                prompt="Опишите груз",
                order=3,
            ),
            FieldDefinition(
                "recipient",
                "Получатель",
                FieldType.TEXT,
                prompt="Укажите получателя (или пропустите)",
                order=4,
            ),
        ),
        completion_evidence=EvidenceRequirements(customer_code_required=True),
    ),
    IndustryPack(
        pack_id="guided_route",
        title="Туристический или групповой маршрут",
        work_types=("excursion", "transfer", "guided_route", "equipment_handoff"),
        branding=Branding(
            name="Групповой маршрут",
            greeting="Здравствуйте! Выберите формат маршрута.",
        ),
        service_catalog=ServiceCatalog(
            categories=(
                ServiceCategory("excursion", "Экскурсия"),
                ServiceCategory("transfer", "Трансфер"),
                ServiceCategory("guided_route", "Сопровождение"),
                ServiceCategory("equipment_handoff", "Выдача снаряжения"),
            ),
        ),
        fields=(
            FieldDefinition(
                "departure",
                "Точка отправления",
                FieldType.ADDRESS,
                required=True,
                prompt="Введите точку отправления",
                order=1,
            ),
            FieldDefinition(
                "route",
                "Маршрут",
                FieldType.ROUTE,
                required=True,
                prompt="Опишите маршрут",
                order=2,
            ),
            FieldDefinition(
                "group_size",
                "Размер группы",
                FieldType.INTEGER,
                required=True,
                prompt="Укажите размер группы",
                order=3,
            ),
            FieldDefinition(
                "equipment",
                "Снаряжение",
                FieldType.TEXT,
                prompt="Укажите снаряжение (или пропустите)",
                order=4,
            ),
        ),
        completion_evidence=EvidenceRequirements(comment_required=True),
    ),
    IndustryPack(
        pack_id="municipal_work",
        title="Муниципальное или коммунальное поручение",
        work_types=("incident", "repair", "inspection", "scheduled_round"),
        branding=Branding(
            name="Коммунальная служба",
            greeting="Здравствуйте! Выберите тип обращения.",
        ),
        service_catalog=ServiceCatalog(
            categories=(
                ServiceCategory("incident", "Инцидент"),
                ServiceCategory("repair", "Ремонт"),
                ServiceCategory("inspection", "Осмотр"),
                ServiceCategory("scheduled_round", "Плановый обход"),
            ),
        ),
        fields=(
            FieldDefinition(
                "zone",
                "Район или зона",
                FieldType.TEXT,
                required=True,
                prompt="Укажите район или зону",
                order=1,
            ),
            FieldDefinition(
                "asset",
                "Объект инфраструктуры",
                FieldType.ASSET_REFERENCE,
                prompt="Укажите объект (или пропустите)",
                order=2,
            ),
            FieldDefinition(
                "priority",
                "Приоритет",
                FieldType.ENUM,
                required=True,
                choices=("routine", "urgent", "emergency"),
                prompt="Выберите приоритет: routine / urgent / emergency",
                order=3,
            ),
            FieldDefinition(
                "request",
                "Сообщение или дефект",
                FieldType.TEXT,
                prompt="Опишите сообщение или дефект (или пропустите)",
                order=4,
            ),
        ),
        completion_evidence=EvidenceRequirements(
            minimum_photos=1, comment_required=True
        ),
    ),
)


INDUSTRY_PACK_INDEX: dict[str, IndustryPack] = {
    pack.pack_id: pack for pack in INDUSTRY_PACKS
}


def seed_definition(pack_id: str) -> PackDefinition:
    pack = INDUSTRY_PACK_INDEX.get(pack_id)
    if pack is None:
        raise ValueError(
            f"unknown pack_id {pack_id!r}; "
            f"available: {', '.join(INDUSTRY_PACK_INDEX)}"
        )
    return pack.to_definition()


def blank_definition() -> PackDefinition:
    return PackDefinition(
        branding=Branding(name=""),
        service_catalog=ServiceCatalog(categories=()),
        fields=(),
        evidence=EvidenceRequirements(),
    )
