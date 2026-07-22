from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from dispatch_core import __main__ as demo_cli
from dispatch_core.api.settings import Settings
from dispatch_core.infrastructure.messaging import PendingDomainEvent, retry_delay
from dispatch_core.messaging.models import (
    InboundEnvelope,
    OutboundButton,
    OutboundEnvelope,
    Provider,
)
from dispatch_core.messaging.projector import OutboxProjectorWorker
from dispatch_core.messaging.replies import ReplyButton
from dispatch_core.messaging.sender import OutboundSender
from dispatch_core.runtime import api as runtime_api
from dispatch_core.runtime.factory import build_transports
from dispatch_core.runtime.worker import _cleanup_sessions, _repeat, _wait_or_stop
from dispatch_core.transports.contracts import SendResult
from dispatch_core.transports.polling import DurablePollingReceiver

KEY = "a-secure-test-key-with-at-least-32-characters"


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": "postgresql://unused",
        "admin_api_key": KEY,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize("api_key", ["", "short", "x" * 31])
def test_settings_reject_short_admin_key(api_key: str) -> None:
    with pytest.raises(ValidationError, match="at least 32"):
        settings(admin_api_key=api_key)


@pytest.mark.parametrize(
    ("mode_field", "secret_field"),
    [
        ("telegram_receive_mode", "telegram_webhook_secret"),
        ("max_receive_mode", "max_webhook_secret"),
    ],
)
def test_webhook_mode_requires_non_empty_secret(
    mode_field: str,
    secret_field: str,
) -> None:
    with pytest.raises(ValidationError, match="requires a secret"):
        settings(**{mode_field: "webhook", secret_field: ""})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("organization_id", ""),
        ("organization_name", "   "),
        ("api_host", "\t"),
    ],
)
def test_settings_reject_blank_identity_and_host(field: str, value: str) -> None:
    with pytest.raises(ValidationError, match="cannot be blank"):
        settings(**{field: value})


def test_settings_normalizes_public_base_url() -> None:
    configured = settings(public_base_url="https://dispatch.example/base/")
    assert configured.public_base_url == "https://dispatch.example/base"


@pytest.mark.parametrize(
    "value",
    [
        "dispatch.example",
        "ftp://dispatch.example",
        "https://dispatch.example/?token=bad",
        "https://dispatch.example/#secret",
    ],
)
def test_settings_rejects_invalid_public_base_url(value: str) -> None:
    with pytest.raises(ValidationError, match="public_base_url"):
        settings(public_base_url=value)


def test_transport_factory_returns_empty_when_channels_disabled() -> None:
    assert build_transports(settings()) == {}


@pytest.mark.parametrize(
    "values",
    [
        {"telegram_receive_mode": "polling"},
        {"max_receive_mode": "polling"},
    ],
)
def test_transport_factory_fails_when_enabled_token_is_missing(
    values: dict[str, str],
) -> None:
    with pytest.raises(RuntimeError, match="token is missing"):
        build_transports(settings(**values))


def test_transport_factory_reads_token_file(tmp_path: Path) -> None:
    token_file = tmp_path / "telegram-token"
    token_file.write_text(" token-from-file\n", encoding="utf-8")
    transports = build_transports(
        settings(
            telegram_receive_mode="polling",
            telegram_bot_token_file=token_file,
        )
    )
    assert set(transports) == {Provider.TELEGRAM}
    asyncio.run(transports[Provider.TELEGRAM].close())


def test_transport_factory_skips_mode_not_used_by_process() -> None:
    result = build_transports(
        settings(telegram_receive_mode="polling"),
        allowed_modes={"webhook"},
    )
    assert result == {}


@pytest.mark.parametrize(
    ("attempts", "lower", "upper"),
    [
        (1, 0.8, 1.2),
        (2, 1.6, 2.4),
        (3, 3.2, 4.8),
        (4, 6.4, 9.6),
        (10, 240.0, 300.0),
    ],
)
def test_retry_delay_is_bounded_exponential(
    attempts: int,
    lower: float,
    upper: float,
) -> None:
    assert lower <= retry_delay(attempts, "stable-key") <= upper


def test_retry_delay_is_deterministic_per_item_and_attempt() -> None:
    assert retry_delay(4, "event-1") == retry_delay(4, "event-1")
    assert retry_delay(4, "event-1") != retry_delay(4, "event-2")


@pytest.mark.parametrize(
    "values",
    [
        {"attempts": 0, "stable_key": "x"},
        {"attempts": 1, "stable_key": "x", "base_seconds": 0},
        {"attempts": 1, "stable_key": "x", "maximum_seconds": -1},
        {"attempts": 1, "stable_key": "x", "jitter_ratio": -0.1},
        {"attempts": 1, "stable_key": "x", "jitter_ratio": 1.1},
    ],
)
def test_retry_delay_rejects_invalid_configuration(values: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        retry_delay(**values)


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"callback_token": "one", "url": "https://example.test"},
        {"callback_token": "one", "request_location": True},
    ],
)
def test_outbound_button_requires_exactly_one_action(values: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        OutboundButton("button", **values)


def test_outbound_button_rejects_blank_text_and_negative_row() -> None:
    with pytest.raises(ValueError, match="text"):
        OutboundButton("", callback_token="token")
    with pytest.raises(ValueError, match="row"):
        OutboundButton("button", callback_token="token", row=-1)


def test_reply_button_validates_direct_action_shape() -> None:
    with pytest.raises(ValueError, match="text"):
        ReplyButton("", "action")
    with pytest.raises(ValueError, match="row"):
        ReplyButton("button", "action", row=-1)
    with pytest.raises(ValueError, match="combine"):
        ReplyButton(
            "button",
            "",
            url="https://example.test",
            request_location=True,
        )
    with pytest.raises(ValueError, match="action"):
        ReplyButton("button", "")


@pytest.mark.parametrize(
    ("external_event_id", "organization_id"),
    [("", "org"), ("event", ""), ("", "")],
)
def test_inbound_envelope_requires_identifiers(
    external_event_id: str,
    organization_id: str,
) -> None:
    with pytest.raises(ValueError, match="identifiers"):
        InboundEnvelope(
            provider=Provider.TELEGRAM,
            external_event_id=external_event_id,
            organization_id=organization_id,
            payload={},
        )


def message(provider: Provider, message_id: int = 1) -> OutboundEnvelope:
    return OutboundEnvelope(
        message_id=message_id,
        deduplication_key=f"message-{message_id}",
        organization_id="org-1",
        provider=provider,
        recipient_id="user-1",
        text="hello",
        attempts=1,
    )


@dataclass
class FakeOutboundStore:
    messages: tuple[OutboundEnvelope, ...]
    delivered: list[tuple[int, str | None]] = field(default_factory=list)
    failed: list[tuple[int, str]] = field(default_factory=list)
    claimed_consumer_keys: tuple[str, ...] = ()

    async def claim(
        self,
        provider: Provider,
        *,
        limit: int,
        consumer_key: str = "",
        consumer_keys: tuple[str, ...] | None = None,
    ) -> tuple[OutboundEnvelope, ...]:
        self.claimed_consumer_keys = consumer_keys or (consumer_key,)
        matches = tuple(item for item in self.messages if item.provider is provider)
        return matches[:limit]

    async def mark_delivered(
        self, item: OutboundEnvelope, external_message_id: str | None
    ) -> None:
        self.delivered.append((item.message_id, external_message_id))

    async def mark_failed(self, item: OutboundEnvelope, error: str) -> None:
        self.failed.append((item.message_id, error))


@dataclass
class FakeTransport:
    fail_ids: set[int] = field(default_factory=set)

    async def send(self, item: OutboundEnvelope) -> SendResult:
        if item.message_id in self.fail_ids:
            raise RuntimeError("provider down")
        return SendResult(external_message_id=f"external-{item.message_id}")


@pytest.mark.asyncio
async def test_sender_records_success_and_failure_without_losing_batch() -> None:
    store = FakeOutboundStore(
        messages=(message(Provider.TELEGRAM, 1), message(Provider.TELEGRAM, 2))
    )
    sender = OutboundSender(  # type: ignore[arg-type]
        store,
        {Provider.TELEGRAM: FakeTransport(fail_ids={2})},  # type: ignore[dict-item]
    )
    assert await sender.run_once(Provider.TELEGRAM) == 1
    assert store.delivered == [(1, "external-1")]
    assert store.failed == [(2, "RuntimeError: provider down")]


@pytest.mark.asyncio
async def test_sender_does_not_claim_queue_without_transport() -> None:
    store = FakeOutboundStore(messages=(message(Provider.MAX),))
    sender = OutboundSender(store, {})  # type: ignore[arg-type]
    assert await sender.run_once(Provider.MAX) == 0
    assert store.delivered == []


@pytest.mark.asyncio
async def test_shared_max_staff_sender_claims_all_staff_role_queues() -> None:
    store = FakeOutboundStore(messages=(message(Provider.MAX),))
    sender = OutboundSender(  # type: ignore[arg-type]
        store,
        {Provider.MAX: FakeTransport()},  # type: ignore[dict-item]
        consumer_key="staff",
    )

    assert await sender.run_once(Provider.MAX) == 1
    assert store.claimed_consumer_keys == (
        "staff",
        "admin",
        "operator",
        "master",
    )


@dataclass
class FakePollingInbox:
    cursors: dict[tuple[Provider, str], str] = field(default_factory=dict)
    batches: list[dict[str, Any]] = field(default_factory=list)

    async def get_cursor(self, provider: Provider, key: str) -> str | None:
        return self.cursors.get((provider, key))

    async def accept_poll_batch(self, **values: Any) -> int:
        self.batches.append(values)
        self.cursors[(values["provider"], values["cursor_key"])] = values["next_cursor"]
        return len(values["events"])


@dataclass
class FakeTelegramPolling:
    updates: tuple[dict[str, Any], ...]
    next_offset: int | None
    received_offset: int | None = None

    async def get_updates(
        self, *, offset: int | None, timeout_seconds: int
    ) -> tuple[tuple[dict[str, Any], ...], int | None]:
        self.received_offset = offset
        return self.updates, self.next_offset

    def external_event_id(self, update: dict[str, Any]) -> str:
        return f"telegram:{update['update_id']}"


@pytest.mark.asyncio
async def test_telegram_polling_persists_batch_before_cursor() -> None:
    inbox = FakePollingInbox(
        cursors={(Provider.TELEGRAM, "org-1:telegram:consumer"): "40"}
    )
    transport = FakeTelegramPolling(
        updates=({"update_id": 40}, {"update_id": 41}),
        next_offset=42,
    )
    receiver = DurablePollingReceiver(inbox)  # type: ignore[arg-type]
    inserted = await receiver.telegram_once(
        transport,  # type: ignore[arg-type]
        organization_id="org-1",
        consumer_key="consumer",
        timeout_seconds=1,
    )
    assert inserted == 2
    assert transport.received_offset == 40
    assert inbox.batches[0]["next_cursor"] == "42"


@dataclass
class FakeMaxPolling:
    updates: tuple[dict[str, Any], ...]
    next_marker: int | None
    received_marker: int | None = None

    async def get_updates(
        self, *, marker: int | None, timeout_seconds: int
    ) -> tuple[tuple[dict[str, Any], ...], int | None]:
        self.received_marker = marker
        return self.updates, self.next_marker

    def external_event_id(self, update: dict[str, Any]) -> str:
        return f"max:{update['id']}"


@pytest.mark.asyncio
async def test_max_polling_persists_batch_and_marker() -> None:
    inbox = FakePollingInbox(cursors={(Provider.MAX, "org-1:max:consumer"): "8"})
    transport = FakeMaxPolling(updates=({"id": "u-1"},), next_marker=9)
    receiver = DurablePollingReceiver(inbox)  # type: ignore[arg-type]
    inserted = await receiver.max_once(
        transport,  # type: ignore[arg-type]
        organization_id="org-1",
        consumer_key="consumer",
        timeout_seconds=1,
    )
    assert inserted == 1
    assert transport.received_marker == 8
    assert inbox.batches[0]["next_cursor"] == "9"


@pytest.mark.asyncio
async def test_polling_does_not_advance_when_provider_has_no_cursor() -> None:
    inbox = FakePollingInbox()
    transport = FakeTelegramPolling(updates=(), next_offset=None)
    receiver = DurablePollingReceiver(inbox)  # type: ignore[arg-type]
    assert (
        await receiver.telegram_once(  # type: ignore[arg-type]
            transport,
            organization_id="org-1",
            consumer_key="consumer",
        )
        == 0
    )
    assert inbox.batches == []


@dataclass
class FakeOutboxWorkerStore:
    events: tuple[PendingDomainEvent, ...]
    failures: list[str] = field(default_factory=list)

    async def claim_events(self, *, limit: int) -> tuple[PendingDomainEvent, ...]:
        return self.events[:limit]

    async def mark_failed(self, event: PendingDomainEvent, error: str) -> None:
        self.failures.append(error)


@dataclass
class FakeProjector:
    fail_event_id: str | None = None
    projected: list[str] = field(default_factory=list)

    async def project(self, event: PendingDomainEvent) -> int:
        if event.event_id == self.fail_event_id:
            raise RuntimeError("projection failed")
        self.projected.append(event.event_id)
        return 1


def pending_event(event_id: str) -> PendingDomainEvent:
    from datetime import UTC, datetime

    return PendingDomainEvent(
        event_id=event_id,
        organization_id="org-1",
        aggregate_type="work_order",
        aggregate_id="order-1",
        aggregate_version=1,
        name="work_order.submitted",
        payload={},
        occurred_at=datetime.now(UTC),
        attempts=1,
    )


@pytest.mark.asyncio
async def test_projector_worker_isolates_failed_event() -> None:
    store = FakeOutboxWorkerStore(
        events=(pending_event("event-1"), pending_event("event-2"))
    )
    projector = FakeProjector(fail_event_id="event-2")
    worker = OutboxProjectorWorker(  # type: ignore[arg-type]
        store,
        projector,  # type: ignore[arg-type]
    )
    assert await worker.run_once() == 1
    assert projector.projected == ["event-1"]
    assert store.failures == ["RuntimeError: projection failed"]


@pytest.mark.asyncio
async def test_repeat_stops_after_signal_and_skips_idle_delay_when_busy() -> None:
    stop = asyncio.Event()
    calls = 0

    async def operation() -> int:
        nonlocal calls
        calls += 1
        if calls == 3:
            stop.set()
        return 1

    await _repeat(operation, stop, idle_seconds=10)
    assert calls == 3


@pytest.mark.asyncio
async def test_repeat_waits_when_idle_then_runs_again() -> None:
    stop = asyncio.Event()
    calls = 0

    async def operation() -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()
        return 0

    await _repeat(operation, stop, idle_seconds=0.001)
    assert calls == 2


@pytest.mark.asyncio
async def test_repeat_recovers_after_unexpected_operation_failure(caplog) -> None:
    stop = asyncio.Event()
    calls = 0

    async def operation() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary database failure")
        stop.set()
        return 0

    await _repeat(operation, stop, idle_seconds=0)
    assert calls == 2
    assert "will be retried" in caplog.text


@pytest.mark.asyncio
async def test_wait_or_stop_handles_pre_set_and_zero_timeout() -> None:
    stopped = asyncio.Event()
    stopped.set()
    await _wait_or_stop(stopped, 10)
    await _wait_or_stop(asyncio.Event(), 0)


@dataclass
class FakeStaleSessionStore:
    deleted: int
    max_ages: list[int] = field(default_factory=list)

    async def cleanup_stale(self, *, max_age_hours: int) -> int:
        self.max_ages.append(max_age_hours)
        return self.deleted


@dataclass
class FakeBindingSessionStore:
    deleted: int
    calls: int = 0

    async def cleanup_expired(self) -> int:
        self.calls += 1
        return self.deleted


@pytest.mark.asyncio
async def test_session_cleanup_covers_every_durable_workflow_store(caplog) -> None:
    caplog.set_level("INFO", logger="dispatch_core.runtime.worker")
    intake = FakeStaleSessionStore(1)
    config = FakeStaleSessionStore(2)
    binding = FakeBindingSessionStore(3)
    staff = FakeStaleSessionStore(4)

    await _cleanup_sessions(  # type: ignore[arg-type]
        intake,
        config,
        binding,
        staff,
    )

    assert intake.max_ages == [24]
    assert config.max_ages == [24]
    assert staff.max_ages == [24]
    assert binding.calls == 1
    assert "cleaned up 10 stale sessions" in caplog.text


@pytest.mark.asyncio
async def test_session_cleanup_does_not_log_when_nothing_expired(caplog) -> None:
    intake = FakeStaleSessionStore(0)
    config = FakeStaleSessionStore(0)
    binding = FakeBindingSessionStore(0)
    staff = FakeStaleSessionStore(0)

    await _cleanup_sessions(  # type: ignore[arg-type]
        intake,
        config,
        binding,
        staff,
    )

    assert "cleaned up" not in caplog.text


def test_demo_cli_executes_complete_curated_flow(capsys) -> None:
    demo_cli.main()
    result = __import__("json").loads(capsys.readouterr().out)
    assert result["status"] == "completed"
    assert result["executor_id"] == "executor-7"
    assert result["outbox_events"] >= 10
    assert result["modules"]["core.work_orders"] == "available"


def test_runtime_api_factory_builds_webhook_only_app(monkeypatch) -> None:
    configured = settings()
    sentinel = object()
    calls: dict[str, Any] = {}
    monkeypatch.setattr(runtime_api, "Settings", lambda: configured)

    def fake_build(value, *, allowed_modes):
        calls["settings"] = value
        calls["allowed_modes"] = allowed_modes
        return {}

    def fake_create_app(value, *, transports):
        calls["transports"] = transports
        return sentinel

    monkeypatch.setattr(runtime_api, "build_transports", fake_build)
    monkeypatch.setattr(runtime_api, "create_app", fake_create_app)
    assert runtime_api.factory() is sentinel
    assert calls == {
        "settings": configured,
        "allowed_modes": {"webhook"},
        "transports": {},
    }


def test_runtime_api_main_uses_hardened_uvicorn_options(monkeypatch) -> None:
    configured = settings(api_host="127.0.0.1", api_port=9080)
    calls: dict[str, Any] = {}
    monkeypatch.setattr(runtime_api, "Settings", lambda: configured)
    monkeypatch.setattr(
        runtime_api.uvicorn,
        "run",
        lambda app, **options: calls.update(app=app, **options),
    )
    runtime_api.main()
    assert calls == {
        "app": "dispatch_core.runtime.api:factory",
        "factory": True,
        "host": "127.0.0.1",
        "port": 9080,
        "proxy_headers": False,
        "server_header": False,
    }
