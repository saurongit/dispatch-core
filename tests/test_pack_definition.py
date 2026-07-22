from __future__ import annotations

import pytest

from dispatch_core.infrastructure.pack_store import (
    PackValidationError,
    validate_definition,
)
from dispatch_core.packs.catalog import (
    Branding,
    FieldDefinition,
    FieldType,
    PackDefinition,
    ServiceCatalog,
    ServiceCategory,
    seed_definition,
)


def test_seed_definition_round_trips_through_json() -> None:
    original = seed_definition("field_service")
    restored = PackDefinition.from_json(original.to_json())
    assert restored.to_json() == original.to_json()
    assert [f.key for f in restored.ordered_fields()] == [
        f.key for f in original.ordered_fields()
    ]


def test_ordered_fields_sorts_by_order_then_key() -> None:
    definition = PackDefinition(
        branding=Branding(name="Сервис"),
        service_catalog=ServiceCatalog(
            categories=(ServiceCategory("a", "Услуга"),)
        ),
        fields=(
            FieldDefinition("address", "Адрес", FieldType.ADDRESS, order=2),
            FieldDefinition("note", "Заметка", FieldType.TEXT, order=1),
        ),
        evidence=None,  # type: ignore[arg-type]
    )
    assert [f.key for f in definition.ordered_fields()] == ["note", "address"]


def _base_fields() -> tuple[FieldDefinition, ...]:
    return (FieldDefinition("address", "Адрес", FieldType.ADDRESS),)


def test_validate_accepts_a_complete_definition() -> None:
    validate_definition(seed_definition("field_service"))


def test_validate_requires_a_brand_name() -> None:
    definition = PackDefinition(
        branding=Branding(name="  "),
        service_catalog=ServiceCatalog(
            categories=(ServiceCategory("a", "Услуга"),)
        ),
        fields=_base_fields(),
        evidence=None,  # type: ignore[arg-type]
    )
    with pytest.raises(PackValidationError, match="бренд"):
        validate_definition(definition)


def test_validate_requires_a_service() -> None:
    definition = PackDefinition(
        branding=Branding(name="Сервис"),
        service_catalog=ServiceCatalog(categories=()),
        fields=_base_fields(),
        evidence=None,  # type: ignore[arg-type]
    )
    with pytest.raises(PackValidationError, match="услуг"):
        validate_definition(definition)


def test_validate_requires_an_address_field() -> None:
    definition = PackDefinition(
        branding=Branding(name="Сервис"),
        service_catalog=ServiceCatalog(
            categories=(ServiceCategory("a", "Услуга"),)
        ),
        fields=(FieldDefinition("note", "Заметка", FieldType.TEXT),),
        evidence=None,  # type: ignore[arg-type]
    )
    with pytest.raises(PackValidationError, match="адрес"):
        validate_definition(definition)
