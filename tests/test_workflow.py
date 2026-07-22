from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import UTC, datetime, timedelta

from dispatch_core.application.service import DispatchService
from dispatch_core.connectivity.egress import (
    EgressMode,
    EgressPolicy,
    EgressRoute,
    RouteHealth,
    SignedConfigEnvelope,
)
from dispatch_core.domain.errors import (
    ConcurrencyConflict,
    EvidenceMissing,
    InvalidTransition,
)
from dispatch_core.domain.tracking import LocationSource, TrackingStatus
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    PoolMode,
    PoolResponseStatus,
    WorkOrderStatus,
)
from dispatch_core.infrastructure.memory import InMemoryUnitOfWorkFactory
from dispatch_core.modules.catalog import ModuleState, module_catalog
from dispatch_core.packs import INDUSTRY_PACKS


class DispatchWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = InMemoryUnitOfWorkFactory()
        self.service = DispatchService(self.factory)

    def create_order(self):
        return self.service.create_order(
            order_id="order-1",
            organization_id="org-1",
            work_type="repair",
            source="phone",
            details={"asset": "lift-42"},
            evidence_requirements=EvidenceRequirements(
                minimum_photos=1, comment_required=True
            ),
        )

    def test_complete_vertical_slice_closes_tracking_and_writes_outbox(self) -> None:
        order = self.create_order()
        self.service.claim_coordination("org-1", order.id, "dispatcher-1")
        self.service.publish_pool(
            "org-1", order.id, PoolMode.CURATED, actor_id="dispatcher-1"
        )
        self.service.express_interest("org-1", order.id, "worker-1")
        self.service.assign_order(
            "org-1", order.id, "worker-1", actor_id="dispatcher-1"
        )
        self.service.accept_order("org-1", order.id, "worker-1")
        _, session = self.service.start_travel(
            "org-1", order.id, "worker-1", session_id="track-1"
        )
        self.service.record_location(
            organization_id="org-1",
            executor_id="worker-1",
            session_id=session.id,
            latitude=53.7557,
            longitude=87.1099,
            source=LocationSource.TELEGRAM,
            accuracy_m=9.5,
        )
        self.service.start_work("org-1", order.id, "worker-1")
        completed = self.service.complete_order(
            "org-1",
            order.id,
            "worker-1",
            CompletionReport(photo_refs=("photo://1",), comment="Repaired"),
        )

        self.assertEqual(WorkOrderStatus.COMPLETED, completed.status)
        stored_session = self.factory.store.tracking_sessions[("org-1", session.id)]
        self.assertEqual(TrackingStatus.COMPLETED, stored_session.status)
        self.assertEqual(1, len(stored_session.points))
        event_names = [item.name for item in self.factory.store.outbox_events]
        self.assertEqual(
            [
                "work_order.submitted",
                "work_order.coordination_claimed",
                "work_order.pool_published",
                "work_order.pool_interest_recorded",
                "work_order.assigned",
                "work_order.accepted",
                "work_order.travel_started",
                "tracking.started",
                "tracking.point_recorded",
                "work_order.started",
                "work_order.completed",
                "tracking.completed",
            ],
            event_names,
        )
        event_ids = {item.event_id for item in self.factory.store.outbox_events}
        self.assertEqual(len(event_names), len(event_ids))

    def test_duplicate_location_source_event_is_idempotent(self) -> None:
        order = self.create_order()
        self.service.assign_order("org-1", order.id, "worker-1")
        self.service.accept_order("org-1", order.id, "worker-1")
        _, session = self.service.start_travel(
            "org-1", order.id, "worker-1", session_id="track-idempotent"
        )
        first = self.service.record_location(
            organization_id="org-1",
            executor_id="worker-1",
            session_id=session.id,
            latitude=53.75,
            longitude=87.1,
            source=LocationSource.TELEGRAM,
            source_event_id="telegram:update-77",
        )
        duplicate = self.service.record_location(
            organization_id="org-1",
            executor_id="worker-1",
            session_id=session.id,
            latitude=1.0,
            longitude=2.0,
            source=LocationSource.TELEGRAM,
            source_event_id="telegram:update-77",
        )
        self.assertEqual(1, len(first.points))
        self.assertEqual(1, len(duplicate.points))
        self.assertEqual(53.75, duplicate.points[0].latitude)
        self.assertEqual(first.version, duplicate.version)

    def test_completion_rejects_missing_evidence_without_persisting(self) -> None:
        order = self.create_order()
        self.service.assign_order("org-1", order.id, "worker-1")
        self.service.accept_order("org-1", order.id, "worker-1")
        self.service.start_work("org-1", order.id, "worker-1")

        with self.assertRaises(EvidenceMissing):
            self.service.complete_order(
                "org-1", order.id, "worker-1", CompletionReport()
            )

        stored = self.factory.store.orders[order.id]
        self.assertEqual(WorkOrderStatus.IN_PROGRESS, stored.status)

    def test_assigned_order_rejects_another_executor(self) -> None:
        order = self.create_order()
        self.service.assign_order("org-1", order.id, "worker-1")
        with self.assertRaises(InvalidTransition):
            self.service.accept_order("org-1", order.id, "worker-2")

    def test_optimistic_lock_allows_only_first_claim(self) -> None:
        order = self.create_order()
        self.service.publish_pool("org-1", order.id, PoolMode.FIRST_CLAIM)
        first_copy = deepcopy(self.factory.store.orders[order.id])
        second_copy = deepcopy(self.factory.store.orders[order.id])
        expected = first_copy.version
        first_copy.claim_first("worker-1")
        second_copy.claim_first("worker-2")

        with self.factory() as first_uow:
            first_uow.orders.save(first_copy, expected_version=expected)
            first_uow.outbox.add(first_copy.pull_events())
            first_uow.commit()

        with self.assertRaises(ConcurrencyConflict):
            with self.factory() as second_uow:
                second_uow.orders.save(second_copy, expected_version=expected)
                second_uow.outbox.add(second_copy.pull_events())
                second_uow.commit()

        self.assertEqual("worker-1", self.factory.store.orders[order.id].assignee_id)

    def test_curated_interest_does_not_assign_executor(self) -> None:
        order = self.create_order()
        self.service.publish_pool("org-1", order.id, PoolMode.CURATED)
        interested = self.service.express_interest("org-1", order.id, "worker-1")

        self.assertIsNone(interested.assignee_id)
        self.assertEqual(WorkOrderStatus.POOL_OPEN, interested.status)
        self.assertEqual(
            PoolResponseStatus.INTERESTED,
            interested.pool_responses["worker-1"].status,
        )

    def test_curated_operator_selects_among_interested_executors(self) -> None:
        order = self.create_order()
        self.service.claim_coordination("org-1", order.id, "dispatcher-1")
        self.service.publish_pool(
            "org-1", order.id, PoolMode.CURATED, actor_id="dispatcher-1"
        )
        self.service.express_interest("org-1", order.id, "worker-1")
        self.service.express_interest("org-1", order.id, "worker-2")

        assigned = self.service.assign_order(
            "org-1", order.id, "worker-2", actor_id="dispatcher-1"
        )

        self.assertEqual("worker-2", assigned.assignee_id)
        self.assertEqual(
            PoolResponseStatus.REJECTED,
            assigned.pool_responses["worker-1"].status,
        )
        self.assertEqual(
            PoolResponseStatus.SELECTED,
            assigned.pool_responses["worker-2"].status,
        )


class ConnectivityTests(unittest.TestCase):
    def test_policy_uses_healthy_route_with_best_priority(self) -> None:
        now = datetime.now(UTC)
        routes = (
            EgressRoute("direct", EgressMode.DIRECT, priority=10),
            EgressRoute(
                "awg-local",
                EgressMode.WIREPROXY,
                priority=20,
                endpoint="socks5://127.0.0.1:1080",
                secret_ref="secret://install/awg",
            ),
        )
        selected = EgressPolicy().choose(
            routes,
            (
                RouteHealth("direct", False, now),
                RouteHealth("awg-local", True, now),
            ),
        )
        self.assertIsNotNone(selected)
        self.assertEqual("awg-local", selected.route_id)

    def test_signed_metadata_rejects_rollback_expiry_and_digest_mismatch(self) -> None:
        now = datetime.now(UTC)
        payload = b'{"route":"direct"}'
        from hashlib import sha256

        envelope = SignedConfigEnvelope(
            installation_id="box-1",
            version=4,
            issued_at=now - timedelta(minutes=1),
            expires_at=now + timedelta(minutes=5),
            payload_sha256=sha256(payload).hexdigest(),
            key_id="release-key-1",
            signature="not-verified-by-metadata-check",
        )
        envelope.validate_metadata(
            payload,
            expected_installation_id="box-1",
            minimum_version=4,
            now=now,
        )
        with self.assertRaisesRegex(ValueError, "rollback"):
            envelope.validate_metadata(
                payload,
                expected_installation_id="box-1",
                minimum_version=5,
                now=now,
            )
        with self.assertRaisesRegex(ValueError, "digest"):
            envelope.validate_metadata(
                b"tampered",
                expected_installation_id="box-1",
                minimum_version=4,
                now=now,
            )


class ModuleCatalogTests(unittest.TestCase):
    def test_module_ids_are_unique_and_stubs_are_explicit(self) -> None:
        modules = module_catalog()
        self.assertEqual(len(modules), len({item.module_id for item in modules}))
        self.assertTrue(any(item.state is ModuleState.AVAILABLE for item in modules))
        self.assertTrue(any(item.state is ModuleState.STUB for item in modules))
        self.assertTrue(all(item.note for item in modules))

    def test_industry_packs_share_the_core_without_duplicate_schema_keys(self) -> None:
        self.assertEqual(
            len(INDUSTRY_PACKS), len({item.pack_id for item in INDUSTRY_PACKS})
        )
        self.assertEqual(
            {"field_service", "local_delivery", "guided_route", "municipal_work"},
            {item.pack_id for item in INDUSTRY_PACKS},
        )
        for pack in INDUSTRY_PACKS:
            keys = [field.key for field in pack.fields]
            self.assertEqual(len(keys), len(set(keys)))


if __name__ == "__main__":
    unittest.main()
