from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from dispatch_core.domain.work_order import EvidenceRequirements


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

    def __post_init__(self) -> None:
        if not self.key or not self.label:
            raise ValueError("field key and label are required")
        if self.field_type is FieldType.ENUM and not self.choices:
            raise ValueError("enum field requires choices")
        if self.field_type is not FieldType.ENUM and self.choices:
            raise ValueError("choices are valid only for enum fields")


@dataclass(frozen=True, slots=True)
class IndustryPack:
    pack_id: str
    title: str
    work_types: tuple[str, ...]
    fields: tuple[FieldDefinition, ...]
    completion_evidence: EvidenceRequirements
    status: str = "example"

    def __post_init__(self) -> None:
        if not self.pack_id or not self.title or not self.work_types:
            raise ValueError("pack id, title and work types are required")
        field_keys = [field.key for field in self.fields]
        if len(field_keys) != len(set(field_keys)):
            raise ValueError(f"duplicate field key in pack {self.pack_id}")


INDUSTRY_PACKS: tuple[IndustryPack, ...] = (
    IndustryPack(
        pack_id="field_service",
        title="Выездной сервис",
        work_types=("repair", "maintenance", "installation", "inspection"),
        fields=(
            FieldDefinition(
                "address", "Адрес", FieldType.ADDRESS, required=True
            ),
            FieldDefinition(
                "asset", "Оборудование", FieldType.ASSET_REFERENCE
            ),
            FieldDefinition(
                "fault",
                "Неисправность или задача",
                FieldType.TEXT,
                required=True,
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
        fields=(
            FieldDefinition(
                "origin", "Откуда", FieldType.ADDRESS, required=True
            ),
            FieldDefinition(
                "destination", "Куда", FieldType.ADDRESS, required=True
            ),
            FieldDefinition("cargo", "Груз", FieldType.TEXT, required=True),
            FieldDefinition("recipient", "Получатель", FieldType.TEXT),
        ),
        completion_evidence=EvidenceRequirements(customer_code_required=True),
    ),
    IndustryPack(
        pack_id="guided_route",
        title="Туристический или групповой маршрут",
        work_types=("excursion", "transfer", "guided_route", "equipment_handoff"),
        fields=(
            FieldDefinition(
                "departure", "Точка отправления", FieldType.ADDRESS, required=True
            ),
            FieldDefinition("route", "Маршрут", FieldType.ROUTE, required=True),
            FieldDefinition(
                "group_size", "Размер группы", FieldType.INTEGER, required=True
            ),
            FieldDefinition("equipment", "Снаряжение", FieldType.TEXT),
        ),
        completion_evidence=EvidenceRequirements(comment_required=True),
    ),
    IndustryPack(
        pack_id="municipal_work",
        title="Муниципальное или коммунальное поручение",
        work_types=("incident", "repair", "inspection", "scheduled_round"),
        fields=(
            FieldDefinition("zone", "Район или зона", FieldType.TEXT, required=True),
            FieldDefinition(
                "asset", "Объект инфраструктуры", FieldType.ASSET_REFERENCE
            ),
            FieldDefinition(
                "priority",
                "Приоритет",
                FieldType.ENUM,
                required=True,
                choices=("routine", "urgent", "emergency"),
            ),
            FieldDefinition("request", "Сообщение или дефект", FieldType.TEXT),
        ),
        completion_evidence=EvidenceRequirements(
            minimum_photos=1, comment_required=True
        ),
    ),
)
