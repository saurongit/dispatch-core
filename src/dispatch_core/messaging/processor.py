from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import DomainError, InvalidTransition
from dispatch_core.domain.work_order import PoolResponseStatus, WorkOrderStatus
from dispatch_core.infrastructure.messaging import (
    PostgresCallbackStore,
    PostgresInboxStore,
    PostgresOutboundStore,
)
from dispatch_core.infrastructure.read_models import OrderReader
from dispatch_core.infrastructure.workflow_store import (
    PostgresExecutionStore,
    PostgresIdentityStore,
    PostgresReportDraftStore,
)
from dispatch_core.messaging.models import InboundEnvelope, Provider
from dispatch_core.transports.contracts import EventKind, InboundEvent, Transport


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

    async def run_once(self, *, limit: int = 50) -> int:
        items = await self._inbox.claim(limit=limit)
        processed = 0
        for item in items:
            try:
                await self._process(item)
            except Exception as exc:
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
            identity = await self._identities.resolve(
                organization_id=item.organization_id,
                provider=item.provider,
                external_user_id=event.external_user_id,
            )
            if identity is None:
                response = (
                    "Пользователь не зарегистрирован. "
                    "Передайте администратору ваш ID: "
                    f"{event.external_user_id}."
                )
            else:
                try:
                    response = await self._dispatch(identity, event)
                except DomainError as exc:
                    response = f"Команда не выполнена: {exc}"
            if response:
                await self._outbound.enqueue(
                    deduplication_key=(
                        f"inbox:{item.provider.value}:{item.external_event_id}:"
                        f"{index}:{event.external_user_id}"
                    ),
                    organization_id=item.organization_id,
                    provider=item.provider,
                    recipient_id=event.chat_id,
                    text=response,
                )
            if event.callback_id:
                try:
                    await transport.answer_callback(event.callback_id, response)
                except Exception:
                    # A callback spinner is ephemeral; the durable reply remains queued.
                    pass

    async def _dispatch(
        self, identity: ActorIdentity, event: InboundEvent
    ) -> str:
        if event.kind is EventKind.CALLBACK:
            return await self._callback(identity, event)
        execution = await self._executions.active_for_executor(
            identity.organization_id,
            identity.actor_id,
        )
        if event.kind is EventKind.LOCATION:
            if execution is None or execution.tracking_session_id is None:
                return "Нет активного маршрута для этой геопозиции."
            await self._service.record_location(
                organization_id=identity.organization_id,
                session_id=execution.tracking_session_id,
                latitude=event.latitude,
                longitude=event.longitude,
                source=_location_source(event.provider),
                source_event_id=event.external_event_id,
            )
            return "Геопозиция сохранена."
        if event.kind is EventKind.PHOTO:
            if execution is None or execution.status != "in_progress":
                return "Фото отчёта принимается только во время работы."
            count = await self._drafts.append_photo(
                organization_id=identity.organization_id,
                order_id=execution.order_id,
                executor_id=identity.actor_id,
                photo_ref=f"{event.provider.value}:{event.media_id}",
            )
            return f"Фото добавлено в отчёт: {count}."
        if event.kind is EventKind.MESSAGE and event.text:
            if execution is not None and execution.status == "in_progress":
                await self._drafts.set_comment(
                    organization_id=identity.organization_id,
                    order_id=execution.order_id,
                    executor_id=identity.actor_id,
                    comment=event.text,
                )
                return "Комментарий отчёта сохранён."
            return "Используйте кнопки под активной заявкой."
        if event.kind is EventKind.START:
            return (
                f"Вы зарегистрированы как {identity.display_name} "
                f"({identity.role})."
            )
        return "Событие принято."

    async def _callback(
        self, identity: ActorIdentity, event: InboundEvent
    ) -> str:
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
        order_id = str(payload.get("order_id") or "")
        if not order_id:
            raise InvalidTransition("callback does not reference a work order")

        if callback.action == "pool_interest":
            self._require_role(identity, "executor")
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
            return "Отклик отправлен оператору."
        if callback.action == "pool_claim":
            self._require_role(identity, "executor")
            try:
                await self._service.claim_first(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id != identity.actor_id:
                    raise
            return "Заявка закреплена за вами."
        if callback.action == "assign":
            self._require_role(identity, "coordinator", "admin")
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
            return f"Мастер {executor_id} выбран."
        if callback.action == "accept":
            self._require_role(identity, "executor")
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
            return "Заявка принята."
        if callback.action == "reject":
            self._require_role(identity, "executor")
            try:
                await self._service.reject_assignment(
                    identity.organization_id, order_id, identity.actor_id
                )
            except InvalidTransition:
                order = await self._reader.get(identity.organization_id, order_id)
                if order.assignee_id is not None:
                    raise
            return "Заявка возвращена оператору."
        if callback.action == "start_travel":
            self._require_role(identity, "executor")
            execution = await self._executions.active_for_executor(
                identity.organization_id,
                identity.actor_id,
            )
            if execution is not None and execution.status in {
                "en_route",
                "in_progress",
            }:
                return "Выезд уже начат."
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
            return "Выезд начат. Отправляйте геопозицию в пути."
        if callback.action == "start_work":
            self._require_role(identity, "executor")
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
            return "Работа начата. Пришлите фото и комментарий отчёта."
        if callback.action == "submit_report":
            self._require_role(identity, "executor")
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
            return "Отчёт принят, заявка завершена."
        raise InvalidTransition(f"unknown callback action {callback.action!r}")

    @staticmethod
    def _require_role(identity: ActorIdentity, *roles: str) -> None:
        if identity.role not in roles:
            raise InvalidTransition("действие недоступно для вашей роли")


def _location_source(provider: Provider):
    from dispatch_core.domain.tracking import LocationSource

    return (
        LocationSource.TELEGRAM
        if provider is Provider.TELEGRAM
        else LocationSource.MAX
    )
