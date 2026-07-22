from __future__ import annotations

import logging
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import DomainError, InvalidTransition
from dispatch_core.domain.work_order import (
    PoolMode,
    PoolResponseStatus,
    WorkOrderStatus,
)
from dispatch_core.infrastructure.messaging import (
    PostgresCallbackStore,
    PostgresInboxStore,
    PostgresOutboundStore,
)
from dispatch_core.infrastructure.pack_store import PostgresPackStore
from dispatch_core.infrastructure.read_models import OrderReader
from dispatch_core.infrastructure.workflow_store import (
    PostgresExecutionStore,
    PostgresIdentityStore,
    PostgresReportDraftStore,
    PostgresStaffBindingSessionStore,
)
from dispatch_core.messaging.config import CONFIG_ACTIONS, ConfigCoordinator
from dispatch_core.messaging.intake import INTAKE_ACTIONS, IntakeCoordinator
from dispatch_core.messaging.models import InboundEnvelope, OutboundButton, Provider
from dispatch_core.messaging.replies import Reply
from dispatch_core.messaging.staff import (
    STAFF_SELECT_ROLE,
    StaffRoleCoordinator,
)
from dispatch_core.messaging.workspaces import (
    MASTER_ACTIONS,
    OPERATOR_ACTIONS,
    MasterCoordinator,
    OperatorCoordinator,
)
from dispatch_core.transports.contracts import EventKind, InboundEvent, Transport
from dispatch_core.transports.max import MaxRateLimitError
from dispatch_core.transports.telegram import TelegramRateLimitError

logger = logging.getLogger(__name__)


class InboundProcessor:
    """Turns durable provider updates into transport-neutral application commands."""

    def __init__(
        self,
        *,
        inbox: PostgresInboxStore,
        identities: PostgresIdentityStore,
        callbacks: PostgresCallbackStore,
        executions: PostgresExecutionStore,
        drafts: PostgresReportDraftStore,
        outbound: PostgresOutboundStore,
        service: AsyncDispatchService,
        reader: OrderReader,
        transports: dict[Provider, Transport],
        packs: PostgresPackStore | None = None,
        intake: IntakeCoordinator | None = None,
        config: ConfigCoordinator | None = None,
        binding_sessions: PostgresStaffBindingSessionStore | None = None,
        staff_roles: StaffRoleCoordinator | None = None,
        operator: OperatorCoordinator | None = None,
        master: MasterCoordinator | None = None,
        organization_id: str = "",
        consumer_key: str = "",
    ) -> None:
        self._inbox = inbox
        self._identities = identities
        self._callbacks = callbacks
        self._executions = executions
        self._drafts = drafts
        self._outbound = outbound
        self._service = service
        self._reader = reader
        self._transports = transports
        self._packs = packs
        self._intake = intake
        self._config = config
        self._binding_sessions = binding_sessions
        self._staff_roles = staff_roles
        self._operator = operator
        self._master = master
        self._organization_id = organization_id
        self._consumer_key = consumer_key

    async def run_once(self, *, limit: int = 50) -> int:
        items = await self._inbox.claim(
            organization_id=self._organization_id or None,
            consumer_key=self._consumer_key,
            limit=limit,
        )
        processed = 0
        for item in items:
            try:
                await self._process(item)
            except Exception as exc:
                logger.exception(
                    "processing failed for %s:%s",
                    item.provider.value,
                    item.external_event_id,
                )
                await self._inbox.mark_failed(
                    item,
                    f"{type(exc).__name__}: {exc}",
                )
                continue
            await self._inbox.mark_processed(item)
            processed += 1
        return processed

    async def _process(self, item: InboundEnvelope) -> None:
        transport = self._transports.get(item.provider)
        if transport is None:
            raise RuntimeError(f"transport {item.provider.value} is not configured")
        events = transport.parse(item.payload)
        for index, event in enumerate(events):
            identity: ActorIdentity | None
            staff_reply: Reply | None = None
            if (
                self._consumer_key == "staff"
                and item.provider is Provider.MAX
                and self._staff_roles is not None
            ):
                identity, staff_reply = await self._resolve_shared_staff(item, event)
            else:
                identity = await self._identities.resolve(
                    organization_id=item.organization_id,
                    provider=item.provider,
                    external_user_id=event.external_user_id,
                    consumer_key=self._consumer_key,
                )
            binding_reply: Reply | None = None
            if identity is None and staff_reply is None:
                identity, binding_reply = await self._maybe_bind_staff(item, event)
            if identity is None:
                if binding_reply is None:
                    identity = await self._maybe_register_requester(item, event)
            if staff_reply is not None:
                response = staff_reply
            elif binding_reply is not None:
                response = binding_reply
            elif identity is None:
                response: Reply = Reply(
                    "Пользователь не зарегистрирован. "
                    "Передайте администратору ваш ID: "
                    f"{event.external_user_id}."
                )
            else:
                try:
                    response = await self._dispatch(identity, event)
                except DomainError as exc:
                    response = Reply(f"Команда не выполнена: {exc}")
            reply = response
            if reply.text or reply.buttons:
                buttons = await self._mint_buttons(item.organization_id, reply.buttons)
                reply_consumer = self._consumer_key or _role_to_consumer_key(
                    identity.role if identity else "client"
                )
                await self._outbound.enqueue(
                    deduplication_key=(
                        f"inbox:{item.organization_id}:{item.provider.value}:"
                        f"{item.consumer_key or self._consumer_key}:"
                        f"{item.external_event_id}:{index}:{event.external_user_id}"
                    ),
                    organization_id=item.organization_id,
                    provider=item.provider,
                    recipient_id=event.chat_id,
                    text=reply.text,
                    buttons=buttons,
                    consumer_key=reply_consumer,
                )
            if event.callback_id:
                try:
                    await transport.answer_callback(
                        event.callback_id, reply.text or None
                    )
                except (RuntimeError, TelegramRateLimitError, MaxRateLimitError):
                    logger.debug(
                        "answer_callback failed for %s — durable reply still queued",
                        event.callback_id,
                    )

    async def _resolve_shared_staff(
        self,
        item: InboundEnvelope,
        event: InboundEvent,
    ) -> tuple[ActorIdentity | None, Reply | None]:
        coordinator = self._staff_roles
        if coordinator is None:
            return None, None
        base = await self._identities.resolve(
            organization_id=item.organization_id,
            provider=item.provider,
            external_user_id=event.external_user_id,
        )
        if base is None or not coordinator.available_roles(base):
            return None, None
        if event.kind is EventKind.START:
            return base, await coordinator.start(base)
        if event.kind is EventKind.CALLBACK and event.callback_token:
            action = await self._callbacks.peek_action(
                token=event.callback_token,
                organization_id=item.organization_id,
            )
            if action == STAFF_SELECT_ROLE:
                callback = await self._callbacks.resolve(
                    token=event.callback_token,
                    organization_id=item.organization_id,
                    actor_role=base.role,
                )
                if callback is None:
                    return base, coordinator.menu(
                        base,
                        note="Кнопка выбора роли устарела.",
                    )
                role, reply = await coordinator.select(base, dict(callback.payload))
                if role is None:
                    return base, reply
                selected = await self._identities.resolve(
                    organization_id=item.organization_id,
                    provider=item.provider,
                    external_user_id=event.external_user_id,
                    consumer_key=role,
                )
                if selected is None:
                    await coordinator.clear(base)
                    return base, coordinator.menu(
                        base,
                        note="Выбранная роль больше недоступна.",
                    )
                return selected, await self._role_landing(
                    selected,
                    note=reply.text,
                )
        role = await coordinator.selected(base)
        if role is not None:
            selected = await self._identities.resolve(
                organization_id=item.organization_id,
                provider=item.provider,
                external_user_id=event.external_user_id,
                consumer_key=role,
            )
            if selected is not None:
                return selected, None
            await coordinator.clear(base)
        return base, coordinator.menu(base)

    async def _maybe_bind_staff(
        self, item: InboundEnvelope, event: InboundEvent
    ) -> tuple[ActorIdentity | None, Reply | None]:
        role = _staff_frontend_role(self._consumer_key)
        sessions = self._binding_sessions
        if sessions is None or role is None:
            return None, None
        session_values = {
            "organization_id": item.organization_id,
            "provider": item.provider,
            "external_user_id": event.external_user_id,
            "consumer_key": role,
        }
        if event.kind is EventKind.START:
            await sessions.begin(**session_values)
            return None, Reply(
                "Введите 4-значный код, который выдал администратор. "
                "Код действует для роли "
                f"«{_role_label(role)}»; на ввод есть 5 попыток."
            )
        if event.kind is not EventKind.MESSAGE or not event.text:
            return None, None
        if not await sessions.is_active(**session_values):
            return None, Reply(
                "Сеанс привязки не запущен или истёк. Нажмите /start и "
                "затем введите код администратора."
            )

        code = event.text.strip()
        identity: ActorIdentity | None = None
        if len(code) == 4 and code.isascii() and code.isdigit():
            identity = await self._identities.bind_actor_by_code(
                organization_id=item.organization_id,
                bind_code=code,
                provider=item.provider,
                external_user_id=event.external_user_id,
                consumer_key=role,
            )
        if identity is not None:
            await sessions.clear(**session_values)
            if role == "staff" and self._staff_roles is not None:
                return identity, self._staff_roles.menu(
                    identity,
                    note="Привязка выполнена.",
                )
            if identity.role in {"operator", "master"}:
                return identity, await self._role_landing(
                    identity,
                    note="Привязка выполнена.",
                )
            return identity, Reply(
                f"Привязка выполнена. Вы вошли как {identity.display_name} "
                f"({_role_label(identity.role)})."
            )

        attempts = await sessions.take_attempt(**session_values)
        if attempts is None:
            return None, Reply(
                "Сеанс привязки истёк. Нажмите /start, чтобы начать заново."
            )
        if attempts >= sessions.MAX_ATTEMPTS:
            await sessions.clear(**session_values)
            return None, Reply(
                "Попытки закончились. Нажмите /start, чтобы начать заново, "
                "или запросите новый код у администратора."
            )
        remaining = sessions.MAX_ATTEMPTS - attempts
        return None, Reply(
            "Код неверен, истёк или предназначен для другого ролевого бота. "
            f"Осталось попыток: {remaining}."
        )

    async def _maybe_register_requester(
        self, item: InboundEnvelope, event: InboundEvent
    ) -> ActorIdentity | None:
        if self._intake is None:
            return None
        frontend_role = self._consumer_key or "client"
        if frontend_role != "client":
            return None
        if event.kind not in {EventKind.MESSAGE, EventKind.START}:
            return None
        existing = await self._identities.resolve(
            organization_id=item.organization_id,
            provider=item.provider,
            external_user_id=event.external_user_id,
        )
        if existing is not None:
            await self._identities.grant_role(
                organization_id=item.organization_id,
                actor_id=existing.actor_id,
                role="client",
            )
            return ActorIdentity(
                organization_id=existing.organization_id,
                actor_id=existing.actor_id,
                role="client",
                display_name=existing.display_name,
                provider=existing.provider,
                external_user_id=existing.external_user_id,
                roles=existing.roles.union({"client"}),
            )
        actor_id = f"{item.provider.value}:{event.external_user_id}"
        await self._identities.upsert_actor(
            organization_id=item.organization_id,
            actor_id=actor_id,
            role="client",
            display_name=event.external_user_id,
            provider=item.provider,
            external_user_id=event.external_user_id,
        )
        return ActorIdentity(
            organization_id=item.organization_id,
            actor_id=actor_id,
            role="client",
            display_name=event.external_user_id,
            provider=item.provider,
            external_user_id=event.external_user_id,
            roles=frozenset({"client"}),
        )

    async def _mint_buttons(
        self, organization_id: str, buttons: tuple[Any, ...]
    ) -> tuple[OutboundButton, ...]:
        minted: list[OutboundButton] = []
        for button in buttons:
            if button.url is not None:
                minted.append(
                    OutboundButton(
                        text=button.text,
                        url=button.url,
                        row=button.row,
                    )
                )
                continue
            if button.request_location:
                minted.append(
                    OutboundButton(
                        text=button.text,
                        request_location=True,
                        row=button.row,
                    )
                )
                continue
            callback = await self._callbacks.create(
                organization_id=organization_id,
                action=button.action,
                payload=dict(button.payload),
                allowed_role=button.allowed_role,
            )
            minted.append(
                OutboundButton(
                    text=button.text,
                    callback_token=callback.token,
                    row=button.row,
                )
            )
        return tuple(minted)

    async def _dispatch(self, identity: ActorIdentity, event: InboundEvent) -> Reply:
        if event.kind is EventKind.CALLBACK:
            return await self._callback(identity, event)
        if identity.role == "admin" and self._config is not None:
            if event.kind is EventKind.START:
                return await self._config.start(identity)
            if event.kind is EventKind.MESSAGE and event.text:
                return await self._config.handle_text(identity, event.text)
        if identity.role == "operator" and self._operator is not None:
            if event.kind is EventKind.START:
                return await self._operator.start(identity)
            if event.kind is EventKind.MESSAGE and event.text:
                return await self._operator.handle_text(identity, event.text)
        if identity.role == "client" and self._intake is not None:
            if event.kind is EventKind.START:
                return await self._intake.start(identity)
            if event.kind is EventKind.MESSAGE and event.text:
                return await self._intake.handle_text(identity, event.text)
            if event.kind is EventKind.LOCATION:
                return await self._intake.handle_location(
                    identity,
                    latitude=event.latitude,
                    longitude=event.longitude,
                    method=event.provider.value,
                )
        if identity.role == "master" and self._master is not None:
            if event.kind is EventKind.START:
                return await self._master.start(identity)
            if (
                event.kind is EventKind.MESSAGE
                and event.text
                and event.text.lstrip().startswith("/")
            ):
                return await self._master.handle_text(identity, event.text)
        execution = await self._executions.active_for_executor(
            identity.organization_id,
            identity.actor_id,
        )
        if event.kind is EventKind.LOCATION:
            if execution is None or execution.tracking_session_id is None:
                return Reply("Нет активного маршрута для этой геопозиции.")
            await self._service.record_location(
                organization_id=identity.organization_id,
                executor_id=identity.actor_id,
                session_id=execution.tracking_session_id,
                latitude=event.latitude,
                longitude=event.longitude,
                source=_location_source(event.provider),
                source_event_id=event.external_event_id,
            )
            return Reply("Геопозиция сохранена.")
        if event.kind is EventKind.PHOTO:
            if execution is None or execution.status != "in_progress":
                return Reply("Фото отчёта принимается только во время работы.")
            count = await self._drafts.append_photo(
                organization_id=identity.organization_id,
                order_id=execution.order_id,
                executor_id=identity.actor_id,
                photo_ref=f"{event.provider.value}:{event.media_id}",
            )
            return Reply(f"Фото добавлено в отчёт: {count}.")
        if event.kind is EventKind.MESSAGE and event.text:
            if execution is not None and execution.status == "in_progress":
                await self._drafts.set_comment(
                    organization_id=identity.organization_id,
                    order_id=execution.order_id,
                    executor_id=identity.actor_id,
                    comment=event.text,
                )
                return Reply("Комментарий отчёта сохранён.")
            if identity.role == "master" and self._master is not None:
                return await self._master.handle_text(identity, event.text)
            return Reply("Используйте кнопки под активной заявкой.")
        if event.kind is EventKind.START:
            return Reply(
                f"Вы зарегистрированы как {identity.display_name} ({identity.role})."
            )
        return Reply("Событие принято.")

    async def _callback(self, identity: ActorIdentity, event: InboundEvent) -> Reply:
        if event.callback_token is None:
            raise InvalidTransition("callback token is missing")
        callback = await self._callbacks.resolve(
            token=event.callback_token,
            organization_id=identity.organization_id,
            actor_role=identity.role,
        )
        if callback is None:
            raise InvalidTransition("кнопка устарела или недоступна для этой роли")
        payload = dict(callback.payload)
        logger.info(
            "callback action=%s role=%s org=%s",
            callback.action,
            identity.role,
            identity.organization_id,
        )
        if self._intake is not None and callback.action in INTAKE_ACTIONS:
            return await self._intake.handle_callback(
                identity, callback.action, payload
            )
        if self._config is not None and callback.action in CONFIG_ACTIONS:
            self._require_role(identity, "admin")
            return await self._config.handle_callback(
                identity, callback.action, payload
            )
        if self._operator is not None and callback.action in OPERATOR_ACTIONS:
            self._require_role(identity, "operator")
            return await self._operator.handle_callback(
                identity, callback.action, payload
            )
        if self._master is not None and callback.action in MASTER_ACTIONS:
            self._require_role(identity, "master")
            return await self._master.handle_callback(
                identity, callback.action, payload
            )
        if callback.action == "pool_publish":
            return await self._publish_pool(identity, payload)
        order_id = str(payload.get("order_id") or "")
        if not order_id:
            raise InvalidTransition("callback does not reference a work order")

        if callback.action == "pool_interest":
            self._require_role(identity, "master")
            try:
                await self._service.express_interest(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                response = order.pool_responses.get(identity.actor_id)
                if response is None or response.status not in {
                    PoolResponseStatus.INTERESTED,
                    PoolResponseStatus.SELECTED,
                }:
                    raise
            return Reply("Отклик отправлен оператору.")
        if callback.action == "pool_claim":
            self._require_role(identity, "master")
            try:
                await self._service.claim_first(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id != identity.actor_id:
                    raise
            return Reply("Заявка закреплена за вами.")
        if callback.action == "assign":
            self._require_role(identity, "operator", "admin")
            executor_id = str(payload.get("executor_id") or "")
            try:
                await self._service.assign_order(
                    identity.organization_id,
                    order_id,
                    executor_id,
                    actor_id=identity.actor_id,
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id != executor_id:
                    raise
            return Reply(f"Мастер {executor_id} выбран.")
        if callback.action == "accept":
            self._require_role(identity, "master")
            try:
                await self._service.accept_order(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id != identity.actor_id or order.status not in {
                    WorkOrderStatus.ACCEPTED,
                    WorkOrderStatus.EN_ROUTE,
                    WorkOrderStatus.IN_PROGRESS,
                    WorkOrderStatus.COMPLETED,
                }:
                    raise
                if order.status == WorkOrderStatus.COMPLETED:
                    return Reply("Заявка уже завершена.")
            return Reply("Заявка принята.")
        if callback.action == "reject":
            self._require_role(identity, "master")
            try:
                await self._service.reject_assignment(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id is not None:
                    raise
            return Reply("Заявка возвращена оператору.")
        if callback.action == "start_travel":
            self._require_role(identity, "master")
            execution = await self._executions.active_for_executor(
                identity.organization_id,
                identity.actor_id,
            )
            if execution is not None and execution.status in {
                "en_route",
                "in_progress",
            }:
                return Reply("Выезд уже начат.")
            session_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"dispatch:tracking:{identity.organization_id}:{order_id}",
                )
            )
            await self._service.start_travel(
                identity.organization_id,
                order_id,
                identity.actor_id,
                session_id=session_id,
            )
            return Reply("Выезд начат. Отправляйте геопозицию в пути.")
        if callback.action == "start_work":
            self._require_role(identity, "master")
            try:
                await self._service.start_work(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id != identity.actor_id or order.status not in {
                    WorkOrderStatus.IN_PROGRESS,
                    WorkOrderStatus.COMPLETED,
                }:
                    raise
                if order.status == WorkOrderStatus.COMPLETED:
                    return Reply("Заявка уже завершена.")
            return Reply("Работа начата. Пришлите фото и комментарий отчёта.")
        if callback.action == "submit_report":
            self._require_role(identity, "master")
            report = await self._drafts.get(
                organization_id=identity.organization_id,
                order_id=order_id,
                executor_id=identity.actor_id,
            )
            try:
                await self._service.complete_order(
                    identity.organization_id,
                    order_id,
                    identity.actor_id,
                    report,
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.status is not WorkOrderStatus.COMPLETED:
                    raise
            await self._drafts.clear(
                organization_id=identity.organization_id,
                order_id=order_id,
                executor_id=identity.actor_id,
            )
            return Reply("Отчёт принят, заявка завершена.")
        raise InvalidTransition(f"unknown callback action {callback.action!r}")

    async def _role_landing(
        self,
        identity: ActorIdentity,
        *,
        note: str | None = None,
    ) -> Reply:
        if identity.role == "admin" and self._config is not None:
            reply = await self._config.start(identity)
        elif identity.role == "operator" and self._operator is not None:
            reply = await self._operator.start(identity)
        elif identity.role == "master" and self._master is not None:
            reply = await self._master.start(identity)
        else:
            reply = Reply(f"Вы вошли как {identity.display_name} ({identity.role}).")
        if not note:
            return reply
        text = f"{note}\n\n{reply.text}" if reply.text else note
        return Reply(text, buttons=reply.buttons)

    async def _publish_pool(
        self, identity: ActorIdentity, payload: dict[str, Any]
    ) -> Reply:
        self._require_role(identity, "operator", "admin")
        order_id = str(payload.get("order_id") or "")
        if not order_id:
            raise InvalidTransition("callback does not reference a work order")
        mode = PoolMode.CURATED
        if self._packs is not None:
            pack = await self._packs.active(identity.organization_id)
            if pack is not None:
                mode = pack.default_pool_mode
        try:
            await self._service.publish_pool(
                identity.organization_id,
                order_id,
                mode,
                actor_id=identity.actor_id,
            )
        except InvalidTransition:
            order = await self._reader.get(identity.organization_id, order_id)
            if order.status is WorkOrderStatus.SUBMITTED:
                raise
        return Reply("Заявка опубликована в пул.")

    @staticmethod
    def _require_role(identity: ActorIdentity, *roles: str) -> None:
        if identity.role not in roles:
            raise InvalidTransition("действие недоступно для вашей роли")


def _location_source(provider: Provider):
    from dispatch_core.domain.tracking import LocationSource

    return (
        LocationSource.TELEGRAM if provider is Provider.TELEGRAM else LocationSource.MAX
    )


def _role_to_consumer_key(role: str) -> str:
    """Map actor role to outbound consumer key for routing."""
    return role if role in {"admin", "operator", "master", "client"} else ""


def _staff_frontend_role(consumer_key: str) -> str | None:
    if consumer_key in {"operator", "master", "staff"}:
        return consumer_key
    if consumer_key.startswith("staff:"):
        role = consumer_key.partition(":")[2]
        return role if role in {"operator", "master"} else None
    return None


def _role_label(role: str) -> str:
    return {
        "admin": "администратор",
        "operator": "оператор",
        "master": "мастер",
        "client": "клиент",
        "staff": "сотрудник",
    }.get(role, role)
