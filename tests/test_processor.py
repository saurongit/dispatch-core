from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import InvalidTransition
from dispatch_core.domain.tracking import LocationSource
from dispatch_core.domain.work_order import CompletionReport, PoolMode, WorkOrder
from dispatch_core.infrastructure.workflow_store import ActiveExecution
from dispatch_core.messaging.models import (
    CallbackAction,
    InboundEnvelope,
    Provider,
)
from dispatch_core.messaging.processor import InboundProcessor
from dispatch_core.messaging.replies import Reply, ReplyButton
from dispatch_core.messaging.workspaces import MASTER_MENU, OPERATOR_MENU
from dispatch_core.transports.contracts import EventKind, InboundEvent


def identity(role: str = "master") -> ActorIdentity:
    return ActorIdentity(
        organization_id="org-1",
        actor_id="actor-1",
        role=role,
        display_name="Actor One",
        provider=Provider.TELEGRAM,
        external_user_id="7001",
    )


def event(kind: EventKind, **values: Any) -> InboundEvent:
    base: dict[str, Any] = {
        "provider": Provider.TELEGRAM,
        "external_event_id": "telegram:1",
        "external_user_id": "7001",
        "chat_id": "7001",
        "kind": kind,
    }
    base.update(values)
    return InboundEvent(**base)


@dataclass
class FakeInbox:
    items: tuple[InboundEnvelope, ...] = ()
    processed: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    async def claim(self, **values: Any) -> tuple[InboundEnvelope, ...]:
        limit = int(values["limit"])
        return self.items[:limit]

    async def mark_processed(self, item: InboundEnvelope) -> None:
        self.processed.append(item.external_event_id)

    async def mark_failed(self, item: InboundEnvelope, error: str) -> None:
        self.failed.append((item.external_event_id, error))


@dataclass
class FakeIdentities:
    value: ActorIdentity | None = None
    bound_value: ActorIdentity | None = None
    error: Exception | None = None
    bind_calls: list[dict[str, Any]] = field(default_factory=list)

    async def resolve(self, **values: Any) -> ActorIdentity | None:
        if self.error:
            raise self.error
        return self.value

    async def bind_actor_by_code(self, **values: Any) -> ActorIdentity | None:
        self.bind_calls.append(values)
        return self.bound_value


@dataclass
class FakeBindingSessions:
    active: bool = True
    attempts: int = 0
    begun: list[dict[str, Any]] = field(default_factory=list)
    cleared: list[dict[str, Any]] = field(default_factory=list)

    MAX_ATTEMPTS = 5

    async def begin(self, **values: Any) -> None:
        self.begun.append(values)
        self.active = True
        self.attempts = 0

    async def is_active(self, **values: Any) -> bool:
        return self.active and self.attempts < self.MAX_ATTEMPTS

    async def take_attempt(self, **values: Any) -> int | None:
        if not self.active or self.attempts >= self.MAX_ATTEMPTS:
            return None
        self.attempts += 1
        return self.attempts

    async def clear(self, **values: Any) -> None:
        self.cleared.append(values)
        self.active = False


@dataclass
class FakeRoleCoordinator:
    label: str
    started: list[str] = field(default_factory=list)
    texts: list[str] = field(default_factory=list)
    callbacks: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def start(self, actor: ActorIdentity) -> Reply:
        self.started.append(actor.actor_id)
        return Reply(f"{self.label} start")

    async def handle_text(self, actor: ActorIdentity, text: str) -> Reply:
        self.texts.append(text)
        return Reply(f"{self.label} text")

    async def handle_callback(
        self,
        actor: ActorIdentity,
        action: str,
        payload: dict[str, Any],
    ) -> Reply:
        self.callbacks.append((action, payload))
        return Reply(f"{self.label} callback")


@dataclass
class FakeCallbacks:
    action: str = "accept"
    payload: dict[str, Any] = field(default_factory=lambda: {"order_id": "order-1"})
    value: CallbackAction | None = None

    async def resolve(self, **values: Any) -> CallbackAction | None:
        if self.value is not None:
            return self.value
        return CallbackAction(
            token=str(values["token"]),
            organization_id=str(values["organization_id"]),
            action=self.action,
            payload=MappingProxyType(dict(self.payload)),
            allowed_role=None,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )


@dataclass
class FakeExecutions:
    value: ActiveExecution | None = None

    async def active_for_executor(
        self, organization_id: str, executor_id: str
    ) -> ActiveExecution | None:
        return self.value


@dataclass
class FakeDrafts:
    report: CompletionReport = field(default_factory=CompletionReport)
    photos: list[str] = field(default_factory=list)
    comments: list[str] = field(default_factory=list)
    cleared: int = 0

    async def append_photo(self, **values: Any) -> int:
        self.photos.append(str(values["photo_ref"]))
        return len(self.photos)

    async def set_comment(self, **values: Any) -> None:
        self.comments.append(str(values["comment"]))

    async def get(self, **values: Any) -> CompletionReport:
        return self.report

    async def clear(self, **values: Any) -> None:
        self.cleared += 1


@dataclass
class FakeOutbound:
    values: list[dict[str, Any]] = field(default_factory=list)

    async def enqueue(self, **values: Any) -> bool:
        self.values.append(values)
        return True


@dataclass
class FakeService:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(
        default_factory=list
    )
    fail_methods: set[str] = field(default_factory=set)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))
        if name in self.fail_methods:
            raise InvalidTransition(f"forced {name} failure")

    async def express_interest(self, *args: Any, **kwargs: Any) -> None:
        await self._call("express_interest", *args, **kwargs)

    async def claim_first(self, *args: Any, **kwargs: Any) -> None:
        await self._call("claim_first", *args, **kwargs)

    async def publish_pool(self, *args: Any, **kwargs: Any) -> None:
        await self._call("publish_pool", *args, **kwargs)

    async def assign_order(self, *args: Any, **kwargs: Any) -> None:
        await self._call("assign_order", *args, **kwargs)

    async def accept_order(self, *args: Any, **kwargs: Any) -> None:
        await self._call("accept_order", *args, **kwargs)

    async def reject_assignment(self, *args: Any, **kwargs: Any) -> None:
        await self._call("reject_assignment", *args, **kwargs)

    async def start_travel(self, *args: Any, **kwargs: Any) -> None:
        await self._call("start_travel", *args, **kwargs)

    async def start_work(self, *args: Any, **kwargs: Any) -> None:
        await self._call("start_work", *args, **kwargs)

    async def complete_order(self, *args: Any, **kwargs: Any) -> None:
        await self._call("complete_order", *args, **kwargs)

    async def record_location(self, *args: Any, **kwargs: Any) -> None:
        await self._call("record_location", *args, **kwargs)


@dataclass
class FakeReader:
    order: WorkOrder

    async def get(self, organization_id: str, order_id: str) -> WorkOrder:
        return self.order


@dataclass
class FakeTransport:
    events: tuple[InboundEvent, ...]
    callback_answers: list[tuple[str, str | None]] = field(default_factory=list)
    callback_error: bool = False

    def parse(self, payload: dict[str, Any]) -> tuple[InboundEvent, ...]:
        return self.events

    async def answer_callback(self, callback_id: str, text: str | None = None) -> None:
        if self.callback_error:
            raise RuntimeError("callback expired")
        self.callback_answers.append((callback_id, text))


def order() -> WorkOrder:
    return WorkOrder.create(
        order_id="order-1",
        organization_id="org-1",
        work_type="repair",
        source="phone",
        details={},
    )


def processor(
    *,
    inbox: FakeInbox | None = None,
    identities: FakeIdentities | None = None,
    callbacks: FakeCallbacks | None = None,
    executions: FakeExecutions | None = None,
    drafts: FakeDrafts | None = None,
    outbound: FakeOutbound | None = None,
    service: FakeService | None = None,
    reader: FakeReader | None = None,
    transport: FakeTransport | None = None,
    binding_sessions: FakeBindingSessions | None = None,
    operator: FakeRoleCoordinator | None = None,
    master: FakeRoleCoordinator | None = None,
    consumer_key: str = "",
    include_transport: bool = True,
) -> tuple[InboundProcessor, dict[str, Any]]:
    values = {
        "inbox": inbox or FakeInbox(),
        "identities": identities or FakeIdentities(identity()),
        "callbacks": callbacks or FakeCallbacks(),
        "executions": executions or FakeExecutions(),
        "drafts": drafts or FakeDrafts(),
        "outbound": outbound or FakeOutbound(),
        "service": service or FakeService(),
        "reader": reader or FakeReader(order()),
        "transports": (
            {Provider.TELEGRAM: transport or FakeTransport(())}
            if include_transport
            else {}
        ),
        "binding_sessions": binding_sessions,
        "operator": operator,
        "master": master,
        "consumer_key": consumer_key,
    }
    return InboundProcessor(**values), values  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_reply_buttons_can_mint_url_and_native_location_actions() -> None:
    target, _ = processor()

    buttons = await target._mint_buttons(
        "org-1",
        (
            ReplyButton(
                "Карта",
                "",
                url="https://dispatch.example/address#token",
            ),
            ReplyButton("Геопозиция", "", request_location=True, row=1),
        ),
    )

    assert buttons[0].url == "https://dispatch.example/address#token"
    assert buttons[0].callback_token is None
    assert buttons[1].request_location
    assert buttons[1].row == 1


@pytest.mark.parametrize(
    ("kind", "execution", "expected"),
    [
        (EventKind.START, None, "зарегистрированы"),
        (EventKind.MESSAGE, None, "Используйте кнопки"),
        (EventKind.PHOTO, None, "только во время работы"),
        (EventKind.LOCATION, None, "Нет активного маршрута"),
    ],
)
@pytest.mark.asyncio
async def test_dispatch_informational_branches(
    kind: EventKind,
    execution: ActiveExecution | None,
    expected: str,
) -> None:
    values: dict[str, Any] = {}
    if kind is EventKind.MESSAGE:
        values["text"] = "hello"
    elif kind is EventKind.PHOTO:
        values["media_id"] = "photo-1"
    elif kind is EventKind.LOCATION:
        values.update(latitude=1.0, longitude=2.0)
    target, _ = processor(executions=FakeExecutions(execution))
    assert expected in (await target._dispatch(identity(), event(kind, **values))).text


@pytest.mark.asyncio
async def test_dispatch_saves_report_photo_and_comment() -> None:
    execution = ActiveExecution("order-1", "in_progress", None)
    drafts = FakeDrafts()
    target, _ = processor(
        executions=FakeExecutions(execution),
        drafts=drafts,
    )
    photo_result = await target._dispatch(
        identity(), event(EventKind.PHOTO, media_id="photo-1")
    )
    comment_result = await target._dispatch(
        identity(), event(EventKind.MESSAGE, text="work done")
    )
    assert photo_result.text == "Фото добавлено в отчёт: 1."
    assert comment_result.text == "Комментарий отчёта сохранён."
    assert drafts.photos == ["telegram:photo-1"]
    assert drafts.comments == ["work done"]


@pytest.mark.asyncio
async def test_dispatch_saves_location_to_active_tracking_session() -> None:
    service = FakeService()
    execution = ActiveExecution("order-1", "en_route", "track-1")
    target, _ = processor(
        executions=FakeExecutions(execution),
        service=service,
    )
    result = await target._dispatch(
        identity(),
        event(EventKind.LOCATION, latitude=53.75, longitude=87.1),
    )
    assert result.text == "Геопозиция сохранена."
    name, _, kwargs = service.calls[0]
    assert name == "record_location"
    assert kwargs["session_id"] == "track-1"
    assert kwargs["source"] is LocationSource.TELEGRAM
    assert kwargs["source_event_id"] == "telegram:1"


@pytest.mark.asyncio
async def test_dispatch_accepts_contact_as_neutral_event() -> None:
    target, _ = processor()
    assert (
        await target._dispatch(identity(), event(EventKind.CONTACT))
    ).text == "Событие принято."


@pytest.mark.parametrize(
    ("action", "role", "expected_method"),
    [
        ("pool_interest", "master", "express_interest"),
        ("pool_claim", "master", "claim_first"),
        ("accept", "master", "accept_order"),
        ("reject", "master", "reject_assignment"),
        ("start_travel", "master", "start_travel"),
        ("start_work", "master", "start_work"),
        ("assign", "operator", "assign_order"),
        ("assign", "admin", "assign_order"),
        ("submit_report", "master", "complete_order"),
    ],
)
@pytest.mark.asyncio
async def test_callback_routes_every_supported_action(
    action: str,
    role: str,
    expected_method: str,
) -> None:
    service = FakeService()
    drafts = FakeDrafts(report=CompletionReport(comment="done"))
    payload = {"order_id": "order-1"}
    if action == "assign":
        payload["executor_id"] = "executor-2"
    target, _ = processor(
        callbacks=FakeCallbacks(action=action, payload=payload),
        service=service,
        drafts=drafts,
    )
    result = await target._callback(
        identity(role),
        event(
            EventKind.CALLBACK,
            callback_token="token-1",
            callback_id="cb-1",
        ),
    )
    assert result
    assert service.calls[0][0] == expected_method
    if action == "submit_report":
        assert drafts.cleared == 1


@pytest.mark.asyncio
async def test_callback_rejects_unknown_expired_and_wrong_role_actions() -> None:
    expired, _ = processor(callbacks=FakeCallbacks(value=None))
    expired._callbacks = FakeCallbacks(value=None)  # type: ignore[assignment]

    class NoCallback:
        async def resolve(self, **values: Any) -> None:
            return None

    expired._callbacks = NoCallback()  # type: ignore[assignment]
    with pytest.raises(Exception, match="устарела"):
        await expired._callback(
            identity(),
            event(EventKind.CALLBACK, callback_token="token"),
        )


@pytest.mark.asyncio
async def test_operator_can_publish_order_to_pool() -> None:
    service = FakeService()
    target, _ = processor(service=service)

    reply = await target._publish_pool(identity("operator"), {"order_id": "order-1"})

    assert reply.text == "Заявка опубликована в пул."
    assert service.calls[0][0] == "publish_pool"

    wrong_role, _ = processor(callbacks=FakeCallbacks(action="pool_claim"))
    with pytest.raises(Exception, match="роли"):
        await wrong_role._callback(
            identity("operator"),
            event(EventKind.CALLBACK, callback_token="token"),
        )

    unknown, _ = processor(callbacks=FakeCallbacks(action="unknown"))
    with pytest.raises(Exception, match="unknown callback"):
        await unknown._callback(
            identity(),
            event(EventKind.CALLBACK, callback_token="token"),
        )

    missing_order, _ = processor(callbacks=FakeCallbacks(action="accept", payload={}))
    with pytest.raises(InvalidTransition, match="does not reference"):
        await missing_order._callback(
            identity(),
            event(EventKind.CALLBACK, callback_token="token"),
        )


def completed_order() -> WorkOrder:
    value = order()
    value.assign("actor-1")
    value.accept("actor-1")
    value.start_work("actor-1")
    value.complete("actor-1", CompletionReport())
    return value


@pytest.mark.parametrize(
    ("action", "method", "persisted", "payload"),
    [
        (
            "pool_interest",
            "express_interest",
            lambda: _interested_order(),
            {"order_id": "order-1"},
        ),
        (
            "pool_claim",
            "claim_first",
            lambda: _claimed_order(),
            {"order_id": "order-1"},
        ),
        (
            "assign",
            "assign_order",
            lambda: _assigned_order("executor-2"),
            {"order_id": "order-1", "executor_id": "executor-2"},
        ),
        (
            "accept",
            "accept_order",
            lambda: _accepted_order(),
            {"order_id": "order-1"},
        ),
        (
            "reject",
            "reject_assignment",
            lambda: _rejected_order(),
            {"order_id": "order-1"},
        ),
        (
            "start_work",
            "start_work",
            lambda: _started_order(),
            {"order_id": "order-1"},
        ),
        (
            "submit_report",
            "complete_order",
            completed_order,
            {"order_id": "order-1"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_callback_retries_are_idempotent_after_command_was_committed(
    action: str,
    method: str,
    persisted,
    payload: dict[str, str],
) -> None:
    service = FakeService(fail_methods={method})
    target, values = processor(
        callbacks=FakeCallbacks(action=action, payload=payload),
        service=service,
        reader=FakeReader(persisted()),
    )
    role = "operator" if action == "assign" else "master"
    result = await target._callback(
        identity(role),
        event(EventKind.CALLBACK, callback_token="token"),
    )
    assert result
    if action == "submit_report":
        assert values["drafts"].cleared == 1


def _interested_order() -> WorkOrder:
    value = order()
    value.publish_pool(PoolMode.CURATED)
    value.express_interest("actor-1")
    return value


def _claimed_order() -> WorkOrder:
    value = order()
    value.publish_pool(PoolMode.FIRST_CLAIM)
    value.claim_first("actor-1")
    return value


def _assigned_order(executor_id: str) -> WorkOrder:
    value = order()
    value.assign(executor_id)
    return value


def _accepted_order() -> WorkOrder:
    value = _assigned_order("actor-1")
    value.accept("actor-1")
    return value


def _rejected_order() -> WorkOrder:
    value = _assigned_order("actor-1")
    value.reject_assignment("actor-1")
    return value


def _started_order() -> WorkOrder:
    value = _accepted_order()
    value.start_work("actor-1")
    return value


@pytest.mark.asyncio
async def test_start_travel_retry_detects_existing_execution() -> None:
    service = FakeService()
    target, _ = processor(
        callbacks=FakeCallbacks(action="start_travel"),
        executions=FakeExecutions(ActiveExecution("order-1", "en_route", "track-1")),
        service=service,
    )
    assert (
        await target._callback(
            identity(), event(EventKind.CALLBACK, callback_token="token")
        )
    ).text == "Выезд уже начат."
    assert service.calls == []


@pytest.mark.asyncio
async def test_run_once_replies_to_unregistered_callback() -> None:
    inbound = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:1",
        organization_id="org-1",
        payload={"update_id": 1},
    )
    callback = event(
        EventKind.CALLBACK,
        callback_token="token",
        callback_id="cb-1",
    )
    inbox = FakeInbox(items=(inbound,))
    outbound = FakeOutbound()
    transport = FakeTransport(events=(callback,))
    target, _ = processor(
        inbox=inbox,
        identities=FakeIdentities(None),
        outbound=outbound,
        transport=transport,
    )
    assert await target.run_once() == 1
    assert inbox.processed == ["telegram:1"]
    assert "не зарегистрирован" in outbound.values[0]["text"]
    assert transport.callback_answers[0][0] == "cb-1"


@pytest.mark.asyncio
async def test_unregistered_staff_start_opens_binding_session() -> None:
    inbound = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:bind-start",
        organization_id="org-1",
        payload={},
    )
    sessions = FakeBindingSessions(active=False)
    outbound = FakeOutbound()
    target, _ = processor(
        inbox=FakeInbox(items=(inbound,)),
        identities=FakeIdentities(None),
        outbound=outbound,
        transport=FakeTransport(events=(event(EventKind.START),)),
        binding_sessions=sessions,
        consumer_key="operator",
    )

    assert await target.run_once() == 1
    assert len(sessions.begun) == 1
    assert "4-значный код" in outbound.values[0]["text"]
    assert outbound.values[0]["consumer_key"] == "operator"


@pytest.mark.asyncio
async def test_reply_deduplication_is_scoped_to_the_frontend() -> None:
    def inbound(consumer_key: str) -> InboundEnvelope:
        return InboundEnvelope(
            provider=Provider.TELEGRAM,
            external_event_id="telegram:same-update",
            organization_id="org-1",
            payload={},
            consumer_key=consumer_key,
        )

    keys: list[str] = []
    for consumer_key in ("operator", "master"):
        outbound = FakeOutbound()
        target, _ = processor(
            inbox=FakeInbox(items=(inbound(consumer_key),)),
            identities=FakeIdentities(identity(consumer_key)),
            outbound=outbound,
            transport=FakeTransport(events=(event(EventKind.START),)),
            consumer_key=consumer_key,
        )
        assert await target.run_once() == 1
        keys.append(outbound.values[0]["deduplication_key"])

    assert len(set(keys)) == 2
    assert ":operator:" in keys[0]
    assert ":master:" in keys[1]


@pytest.mark.asyncio
async def test_staff_code_binds_identity_without_counting_a_failure() -> None:
    inbound = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:bind-code",
        organization_id="org-1",
        payload={},
    )
    bound = identity("operator")
    identities = FakeIdentities(None, bound_value=bound)
    sessions = FakeBindingSessions()
    outbound = FakeOutbound()
    target, _ = processor(
        inbox=FakeInbox(items=(inbound,)),
        identities=identities,
        outbound=outbound,
        transport=FakeTransport(events=(event(EventKind.MESSAGE, text=" 0123 "),)),
        binding_sessions=sessions,
        consumer_key="operator",
    )

    assert await target.run_once() == 1
    assert identities.bind_calls[0]["bind_code"] == "0123"
    assert sessions.attempts == 0
    assert len(sessions.cleared) == 1
    assert "Привязка выполнена" in outbound.values[0]["text"]


@pytest.mark.asyncio
async def test_successful_master_binding_opens_master_workspace() -> None:
    bound = identity("master")
    master = FakeRoleCoordinator("master")
    target, _ = processor(
        identities=FakeIdentities(None, bound_value=bound),
        binding_sessions=FakeBindingSessions(),
        consumer_key="master",
        master=master,
    )
    item = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:bind-master",
        organization_id="org-1",
        payload={},
        consumer_key="master",
    )

    identity_value, reply = await target._maybe_bind_staff(
        item,
        event(EventKind.MESSAGE, text="0123"),
    )

    assert identity_value == bound
    assert reply is not None
    assert "Привязка выполнена" in reply.text
    assert "master start" in reply.text
    assert master.started == ["actor-1"]


@pytest.mark.asyncio
async def test_role_workspaces_receive_start_text_and_callback() -> None:
    operator = FakeRoleCoordinator("operator")
    master = FakeRoleCoordinator("master")
    target, _ = processor(operator=operator, master=master)

    assert (
        await target._dispatch(identity("operator"), event(EventKind.START))
    ).text == "operator start"
    assert (
        await target._dispatch(
            identity("operator"), event(EventKind.MESSAGE, text="/masters")
        )
    ).text == "operator text"

    target._callbacks = FakeCallbacks(action=OPERATOR_MENU)
    assert (
        await target._callback(
            identity("operator"),
            event(EventKind.CALLBACK, callback_token="operator-token"),
        )
    ).text == "operator callback"
    target._callbacks = FakeCallbacks(action=MASTER_MENU)
    assert (
        await target._callback(
            identity("master"),
            event(EventKind.CALLBACK, callback_token="master-token"),
        )
    ).text == "master callback"


@pytest.mark.asyncio
async def test_staff_binding_locks_after_five_failed_codes() -> None:
    target, _ = processor(
        identities=FakeIdentities(None),
        binding_sessions=FakeBindingSessions(attempts=4),
        consumer_key="master",
    )
    item = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:bind-fail",
        organization_id="org-1",
        payload={},
    )

    bound, reply = await target._maybe_bind_staff(
        item,
        event(EventKind.MESSAGE, text="xxxx"),
    )

    assert bound is None
    assert reply is not None
    assert "Попытки закончились" in reply.text


@pytest.mark.asyncio
async def test_run_once_retries_unexpected_failure_and_missing_transport() -> None:
    inbound = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:1",
        organization_id="org-1",
        payload={},
    )
    inbox = FakeInbox(items=(inbound,))
    target, _ = processor(inbox=inbox, include_transport=False)
    assert await target.run_once() == 0
    assert "not configured" in inbox.failed[0][1]

    inbox = FakeInbox(items=(inbound,))
    target, _ = processor(
        inbox=inbox,
        identities=FakeIdentities(error=RuntimeError("database down")),
        transport=FakeTransport(events=(event(EventKind.START),)),
    )
    assert await target.run_once() == 0
    assert "database down" in inbox.failed[0][1]


@pytest.mark.asyncio
async def test_callback_ack_failure_does_not_retry_committed_command() -> None:
    inbound = InboundEnvelope(
        provider=Provider.TELEGRAM,
        external_event_id="telegram:1",
        organization_id="org-1",
        payload={},
    )
    transport = FakeTransport(
        events=(
            event(
                EventKind.CALLBACK,
                callback_token="token",
                callback_id="cb-1",
            ),
        ),
        callback_error=True,
    )
    inbox = FakeInbox(items=(inbound,))
    target, _ = processor(inbox=inbox, transport=transport)
    assert await target.run_once() == 1
    assert inbox.failed == []
