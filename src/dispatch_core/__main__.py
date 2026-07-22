from __future__ import annotations

import json

from dispatch_core.application.service import DispatchService
from dispatch_core.domain.tracking import LocationSource
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    PoolMode,
)
from dispatch_core.infrastructure.memory import InMemoryUnitOfWorkFactory
from dispatch_core.modules.catalog import module_catalog
from dispatch_core.packs import INDUSTRY_PACKS


def main() -> None:
    factory = InMemoryUnitOfWorkFactory()
    service = DispatchService(factory)

    order = service.create_order(
        order_id="demo-order",
        organization_id="demo-org",
        work_type="field_service",
        source="dispatcher_phone_call",
        details={
            "summary": "Inspect and repair equipment",
            "address": "Demo site",
        },
        evidence_requirements=EvidenceRequirements(
            minimum_photos=1,
            comment_required=True,
        ),
    )
    service.claim_coordination("demo-org", order.id, "dispatcher-1")
    service.publish_pool(
        "demo-org",
        order.id,
        PoolMode.CURATED,
        actor_id="dispatcher-1",
    )
    service.express_interest("demo-org", order.id, "executor-7")
    service.assign_order(
        "demo-org",
        order.id,
        "executor-7",
        actor_id="dispatcher-1",
    )
    service.accept_order("demo-org", order.id, "executor-7")
    order, session = service.start_travel(
        "demo-org", order.id, "executor-7", session_id="demo-track"
    )
    service.record_location(
        organization_id="demo-org",
        executor_id="executor-7",
        session_id=session.id,
        latitude=55.751244,
        longitude=37.618423,
        source=LocationSource.TELEGRAM,
        accuracy_m=12.0,
    )
    service.start_work("demo-org", order.id, "executor-7")
    order = service.complete_order(
        "demo-org",
        order.id,
        "executor-7",
        CompletionReport(
            photo_refs=("object://demo/report-1.jpg",),
            comment="Inspection complete; unit is operational.",
        ),
    )

    print(
        json.dumps(
            {
                "order_id": order.id,
                "status": order.status.value,
                "executor_id": order.assignee_id,
                "outbox_events": len(factory.store.outbox_events),
                "modules": {
                    item.module_id: item.state.value for item in module_catalog()
                },
                "industry_packs": [item.pack_id for item in INDUSTRY_PACKS],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
