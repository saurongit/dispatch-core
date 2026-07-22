from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ModuleKind(StrEnum):
    CORE = "core"
    INTAKE = "intake"
    WORKER_TRANSPORT = "worker_transport"
    USER_INTERFACE = "user_interface"
    STORAGE = "storage"
    CONNECTIVITY = "connectivity"


class ModuleState(StrEnum):
    AVAILABLE = "available"
    STUB = "stub"


@dataclass(frozen=True, slots=True)
class ModuleDescriptor:
    module_id: str
    kind: ModuleKind
    state: ModuleState
    capabilities: tuple[str, ...]
    note: str


MODULES: tuple[ModuleDescriptor, ...] = (
    ModuleDescriptor(
        "core.work_orders",
        ModuleKind.CORE,
        ModuleState.AVAILABLE,
        ("lifecycle", "assignment", "evidence", "domain_events"),
        "Transport-neutral domain kernel.",
    ),
    ModuleDescriptor(
        "core.tracking",
        ModuleKind.CORE,
        ModuleState.AVAILABLE,
        ("sessions", "validated_points", "freshness_metadata"),
        "PostgreSQL stores location history append-only.",
    ),
    ModuleDescriptor(
        "core.industry_packs",
        ModuleKind.CORE,
        ModuleState.AVAILABLE,
        ("field_definitions", "work_types", "evidence_defaults", "versioned_db"),
        "Per-organization packs live in PostgreSQL; code packs seed templates.",
    ),
    ModuleDescriptor(
        "intake.manual",
        ModuleKind.INTAKE,
        ModuleState.AVAILABLE,
        ("create_work_order",),
        "Exercised through the application API and demo CLI.",
    ),
    ModuleDescriptor(
        "intake.telegram_guided",
        ModuleKind.INTAKE,
        ModuleState.AVAILABLE,
        ("service_selection", "field_prompts", "confirmation", "order_creation"),
        "Pack-driven client flow in the messenger; no dispatcher panel needed.",
    ),
    ModuleDescriptor(
        "config.messenger_admin",
        ModuleKind.USER_INTERFACE,
        ModuleState.AVAILABLE,
        ("brand", "services", "fields", "evidence", "preview", "publish"),
        "Admin assembles the whole service from bot commands; drafts then publish.",
    ),
    ModuleDescriptor(
        "worker.telegram",
        ModuleKind.WORKER_TRANSPORT,
        ModuleState.AVAILABLE,
        ("polling", "callbacks", "location", "photo_report"),
        "Durable polling or webhook ingestion; tokens remain deployment secrets.",
    ),
    ModuleDescriptor(
        "worker.max",
        ModuleKind.WORKER_TRANSPORT,
        ModuleState.AVAILABLE,
        ("polling", "callbacks", "location", "photo_report"),
        "Uses platform-api2.max.ru and Authorization header authentication.",
    ),
    ModuleDescriptor(
        "ui.dispatcher_web",
        ModuleKind.USER_INTERFACE,
        ModuleState.STUB,
        ("board", "map", "configuration"),
        "Planned for the single-machine milestone.",
    ),
    ModuleDescriptor(
        "storage.memory",
        ModuleKind.STORAGE,
        ModuleState.AVAILABLE,
        ("unit_of_work", "optimistic_lock", "outbox"),
        "For tests and demonstrations only.",
    ),
    ModuleDescriptor(
        "storage.sqlite",
        ModuleKind.STORAGE,
        ModuleState.STUB,
        ("migrations", "transactional_outbox", "backup"),
        "First durable local deployment target.",
    ),
    ModuleDescriptor(
        "storage.postgresql",
        ModuleKind.STORAGE,
        ModuleState.AVAILABLE,
        (
            "migrations",
            "transactional_outbox",
            "durable_inbox",
            "skip_locked_workers",
            "multi_process",
        ),
        "Default durable deployment target.",
    ),
    ModuleDescriptor(
        "connectivity.egress",
        ModuleKind.CONNECTIVITY,
        ModuleState.AVAILABLE,
        ("route_policy", "health_aware_failover", "signed_metadata_model"),
        "Network probes and route process control remain adapters.",
    ),
    ModuleDescriptor(
        "connectivity.edge_gateway",
        ModuleKind.CONNECTIVITY,
        ModuleState.STUB,
        ("webhooks", "durable_inbound_queue", "hybrid_delivery"),
        "Webhooks and durable ingress exist; a separate hybrid edge does not.",
    ),
)


def module_catalog() -> tuple[ModuleDescriptor, ...]:
    return MODULES
