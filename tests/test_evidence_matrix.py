from __future__ import annotations

from itertools import product

import pytest

from dispatch_core.domain.errors import EvidenceMissing
from dispatch_core.domain.work_order import (
    CompletionReport,
    EvidenceRequirements,
    WorkOrder,
    WorkOrderStatus,
)

EVIDENCE_BITS = {
    "photo": 1,
    "comment": 2,
    "signature": 4,
    "customer_code": 8,
}


def _requirements(mask: int) -> EvidenceRequirements:
    return EvidenceRequirements(
        minimum_photos=1 if mask & EVIDENCE_BITS["photo"] else 0,
        comment_required=bool(mask & EVIDENCE_BITS["comment"]),
        signature_required=bool(mask & EVIDENCE_BITS["signature"]),
        customer_code_required=bool(mask & EVIDENCE_BITS["customer_code"]),
    )


def _report(mask: int) -> CompletionReport:
    return CompletionReport(
        photo_refs=("object://photo-1",) if mask & EVIDENCE_BITS["photo"] else (),
        comment="work completed" if mask & EVIDENCE_BITS["comment"] else None,
        signature_ref="object://signature-1"
        if mask & EVIDENCE_BITS["signature"]
        else None,
        customer_code="4381" if mask & EVIDENCE_BITS["customer_code"] else None,
    )


def _mask_id(prefix: str, mask: int) -> str:
    names = [name for name, bit in EVIDENCE_BITS.items() if mask & bit]
    return f"{prefix}={'none' if not names else '+'.join(names)}"


EVIDENCE_CASES = [
    pytest.param(
        required_mask,
        report_mask,
        id=f"{_mask_id('requires', required_mask)}-{_mask_id('has', report_mask)}",
    )
    for required_mask, report_mask in product(range(16), repeat=2)
]


@pytest.mark.parametrize(
    ("required_mask", "report_mask"),
    EVIDENCE_CASES,
)
def test_completion_evidence_truth_table(
    required_mask: int,
    report_mask: int,
) -> None:
    """Exhaust every required/present evidence combination (16 x 16)."""
    order = WorkOrder.create(
        order_id="order-1",
        public_number="A000",
        organization_id="org-1",
        work_type="repair",
        source="phone",
        details={},
        evidence_requirements=_requirements(required_mask),
    )
    order.pull_events()
    order.assign("executor-1")
    order.accept("executor-1")
    order.start_work("executor-1")
    order.pull_events()

    missing_mask = required_mask & ~report_mask
    if missing_mask:
        with pytest.raises(EvidenceMissing):
            order.complete("executor-1", _report(report_mask))
        assert order.status is WorkOrderStatus.IN_PROGRESS
        assert order.report is None
        assert order.pull_events() == ()
    else:
        report = _report(report_mask)
        order.complete("executor-1", report)
        assert order.status is WorkOrderStatus.COMPLETED
        assert order.report == report
        events = order.pull_events()
        assert [event.name for event in events] == ["work_order.completed"]
        assert events[0].payload["photo_count"] == len(report.photo_refs)
