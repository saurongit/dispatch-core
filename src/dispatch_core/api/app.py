from __future__ import annotations

import json
import logging
import secrets
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from uuid import NAMESPACE_URL, uuid5

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.application.identity import ActorIdentity
from dispatch_core.domain.errors import (
    ConcurrencyConflict,
    DomainError,
    NotFound,
)
from dispatch_core.domain.work_order import CompletionReport, EvidenceRequirements
from dispatch_core.infrastructure.messaging import PostgresInboxStore
from dispatch_core.infrastructure.postgres import (
    PostgresDatabase,
    PostgresUnitOfWorkFactory,
)
from dispatch_core.infrastructure.read_models import OrderReader, PostgresOrderReader
from dispatch_core.infrastructure.workflow_store import PostgresIdentityStore
from dispatch_core.messaging.models import Provider
from dispatch_core.transports.common import (
    decode_executor_token,
    encode_executor_token,
)
from dispatch_core.transports.contracts import Transport

from .schemas import (
    ActorCreateInput,
    ActorInput,
    ActorResponse,
    AssignInput,
    CancelInput,
    CompleteInput,
    CreateOrderInput,
    ExecutorTokenRequest,
    ExecutorTokenResponse,
    LocationInput,
    LocationRecordedResponse,
    PublishPoolInput,
    TravelInput,
    TravelStartedResponse,
    WorkOrderResponse,
)
from .settings import Settings

logger = logging.getLogger(__name__)

_AUTH_ATTEMPTS: dict[str, list[float]] = defaultdict(list)
_AUTH_WINDOW = 60.0
_AUTH_MAX_ATTEMPTS = 10
_AUTH_SWEEPCounter: int = 0


def create_app(
    settings: Settings | None = None,
    *,
    service: AsyncDispatchService | None = None,
    reader: OrderReader | None = None,
    inbox: PostgresInboxStore | None = None,
    identities: PostgresIdentityStore | None = None,
    transports: dict[Provider, Transport] | None = None,
) -> FastAPI:
    resolved_settings = settings or Settings()  # type: ignore[call-arg]
    injected = service is not None and reader is not None
    database: PostgresDatabase | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal database, service, reader, inbox, identities
        if not injected:
            database = PostgresDatabase(
                resolved_settings.database_url.get_secret_value()
            )
            await database.connect()
            if resolved_settings.auto_migrate:
                # Bounded path lookup runs once during process startup.
                migration_directory = resolved_settings.migrations_directory or (
                    Path(__file__).resolve().parents[3]  # noqa: ASYNC240
                    / "migrations"
                )
                await database.migrate(migration_directory)
            if database.pool is None:
                raise RuntimeError("database pool was not initialized")
            async with database.pool.acquire() as connection:
                await connection.execute(
                    """
                    INSERT INTO organizations(id, name)
                    VALUES ($1, $2)
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                    """,
                    resolved_settings.organization_id,
                    resolved_settings.organization_name,
                )
            service = AsyncDispatchService(PostgresUnitOfWorkFactory(database.pool))
            reader = PostgresOrderReader(database.pool)
            inbox = PostgresInboxStore(database.pool)
            identities = PostgresIdentityStore(database.pool)
        app.state.database = database
        app.state.service = service
        app.state.reader = reader
        app.state.inbox = inbox
        app.state.identities = identities
        try:
            yield
        finally:
            for transport in (transports or {}).values():
                await transport.close()
            if database is not None:
                await database.close()

    app = FastAPI(
        title="Dispatch Core API",
        version="0.1.0-dev",
        lifespan=lifespan,
        docs_url=(
            None
            if resolved_settings.environment.casefold() == "production"
            else "/docs"
        ),
        redoc_url=None,
        openapi_url=(
            None
            if resolved_settings.environment.casefold() == "production"
            else "/openapi.json"
        ),
    )
    app.state.settings = resolved_settings
    app.state.transports = transports or {}

    @app.exception_handler(NotFound)
    async def not_found_handler(request: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(DomainError)
    async def domain_handler(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(ConcurrencyConflict)
    async def conflict_handler(
        request: Request, exc: ConcurrencyConflict
    ) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    async def authenticate(
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> None:
        global _AUTH_SWEEPCounter
        client_ip = request.client.host if request.client else "unknown"
        now = time.monotonic()
        _AUTH_ATTEMPTS[client_ip] = [
            t for t in _AUTH_ATTEMPTS[client_ip] if now - t < _AUTH_WINDOW
        ]
        if not _AUTH_ATTEMPTS[client_ip]:
            _AUTH_ATTEMPTS.pop(client_ip, None)
        _AUTH_SWEEPCounter += 1
        if _AUTH_SWEEPCounter % 100 == 0:
            stale = [
                ip
                for ip, timestamps in _AUTH_ATTEMPTS.items()
                if not timestamps or now - timestamps[-1] >= _AUTH_WINDOW
            ]
            for ip in stale:
                _AUTH_ATTEMPTS.pop(ip, None)
        if len(_AUTH_ATTEMPTS[client_ip]) >= _AUTH_MAX_ATTEMPTS:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many authentication attempts",
            )
        expected = resolved_settings.admin_api_key.get_secret_value()
        prefix = "Bearer "
        if authorization is None or not authorization.startswith(prefix):
            _AUTH_ATTEMPTS[client_ip].append(now)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Bearer authentication required",
            )
        if not secrets.compare_digest(authorization.removeprefix(prefix), expected):
            _AUTH_ATTEMPTS[client_ip].append(now)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )
        _AUTH_ATTEMPTS.pop(client_ip, None)

    async def authenticate_executor(
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> ActorIdentity:
        global _AUTH_SWEEPCounter
        client_ip = request.client.host if request.client else "unknown"
        now = time.monotonic()
        _AUTH_ATTEMPTS[client_ip] = [
            t for t in _AUTH_ATTEMPTS[client_ip] if now - t < _AUTH_WINDOW
        ]
        if not _AUTH_ATTEMPTS[client_ip]:
            _AUTH_ATTEMPTS.pop(client_ip, None)
        if len(_AUTH_ATTEMPTS[client_ip]) >= _AUTH_MAX_ATTEMPTS:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many authentication attempts",
            )
        signing = resolved_settings.executor_token_secret
        if signing is None:
            raise HTTPException(status_code=503, detail="Executor auth not configured")
        prefix = "Bearer "
        if authorization is None or not authorization.startswith(prefix):
            _AUTH_ATTEMPTS[client_ip].append(now)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Bearer authentication required",
            )
        token = authorization.removeprefix(prefix)
        result = decode_executor_token(token, signing_secret=signing.get_secret_value())
        if result is None:
            _AUTH_ATTEMPTS[client_ip].append(now)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired executor token",
            )
        org_id, actor_id = result
        identities = get_identities(request)
        if identities is not None:
            identity = await identities.resolve(
                organization_id=org_id,
                provider=Provider.TELEGRAM,
                external_user_id=actor_id,
            )
        else:
            identity = None
        if identity is None:
            identity = ActorIdentity(
                organization_id=org_id,
                actor_id=actor_id,
                role="master",
                display_name=actor_id,
                provider=Provider.TELEGRAM,
                external_user_id=actor_id,
            )
        _AUTH_ATTEMPTS.pop(client_ip, None)
        return identity

    def get_service(request: Request) -> AsyncDispatchService:
        value = request.app.state.service
        if value is None:
            raise HTTPException(status_code=503, detail="Service is not ready")
        return value

    def get_identities(request: Request) -> PostgresIdentityStore | None:
        return getattr(request.app.state, "identities", None)

    def get_reader(request: Request) -> OrderReader:
        value = request.app.state.reader
        if value is None:
            raise HTTPException(status_code=503, detail="Reader is not ready")
        return value

    @app.get("/health/live", tags=["health"])
    async def live() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready", tags=["health"])
    async def ready(request: Request) -> JSONResponse:
        active_database = request.app.state.database
        if injected:
            return JSONResponse(status_code=200, content={"status": "ready"})
        if active_database is not None and await active_database.health():
            return JSONResponse(status_code=200, content={"status": "ready"})
        return JSONResponse(status_code=503, content={"status": "not_ready"})

    @app.post(
        "/v1/orders",
        response_model=WorkOrderResponse,
        status_code=201,
        tags=["orders"],
    )
    async def create_order(
        body: CreateOrderInput,
        request: Request,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=8, max_length=200)
        ],
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order_id = str(
            uuid5(
                NAMESPACE_URL,
                f"dispatch:{resolved_settings.organization_id}:order:{idempotency_key}",
            )
        )
        command_service = get_service(request)
        try:
            order = await command_service.create_order(
                order_id=order_id,
                organization_id=resolved_settings.organization_id,
                work_type=body.work_type,
                source=body.source,
                details=body.details,
                requester_id=body.requester_id,
                evidence_requirements=EvidenceRequirements(
                    **body.evidence.model_dump()
                ),
            )
        except ConcurrencyConflict:
            order = await get_reader(request).get(
                resolved_settings.organization_id, order_id
            )
            if _create_fingerprint(order) != _body_fingerprint(body):
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency-Key was already used for another request",
                ) from None
        return WorkOrderResponse.from_domain(order)

    @app.get(
        "/v1/orders/{order_id}",
        response_model=WorkOrderResponse,
        tags=["orders"],
    )
    async def get_order(
        order_id: str,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_reader(request).get(
            resolved_settings.organization_id, order_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post(
        "/v1/auth/executor-token",
        response_model=ExecutorTokenResponse,
        tags=["auth"],
    )
    async def create_executor_token(
        body: ExecutorTokenRequest,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> ExecutorTokenResponse:
        signing = resolved_settings.executor_token_secret
        if signing is None:
            raise HTTPException(
                status_code=503,
                detail="executor_token_secret is not configured",
            )
        token, expires_at = encode_executor_token(
            resolved_settings.organization_id,
            body.actor_id,
            signing_secret=signing.get_secret_value(),
            ttl_seconds=resolved_settings.executor_token_ttl_seconds,
        )
        return ExecutorTokenResponse(token=token, expires_at=expires_at)

    @app.post(
        "/v1/actors",
        response_model=ActorResponse,
        status_code=201,
        tags=["configuration"],
    )
    async def upsert_actor(
        body: ActorCreateInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> ActorResponse:
        identity_store = request.app.state.identities
        if identity_store is None:
            raise HTTPException(
                status_code=503,
                detail="Identity configuration is not available",
            )
        await identity_store.upsert_actor(
            organization_id=resolved_settings.organization_id,
            actor_id=body.actor_id,
            role=body.role,
            display_name=body.display_name,
            provider=body.provider,
            external_user_id=body.external_user_id,
        )
        return ActorResponse(**body.model_dump())

    @app.post("/v1/orders/{order_id}/coordination:claim", tags=["commands"])
    async def claim_coordination(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).claim_coordination(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/pool:publish", tags=["commands"])
    async def publish_pool(
        order_id: str,
        body: PublishPoolInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).publish_pool(
            resolved_settings.organization_id,
            order_id,
            body.mode,
            actor_id=body.actor_id,
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/pool:interest", tags=["commands"])
    async def express_interest(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).express_interest(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/pool:claim", tags=["commands"])
    async def claim_first(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).claim_first(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/assign", tags=["commands"])
    async def assign_order(
        order_id: str,
        body: AssignInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).assign_order(
            resolved_settings.organization_id,
            order_id,
            body.executor_id,
            actor_id=body.actor_id,
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/accept", tags=["commands"])
    async def accept_order(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).accept_order(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/reject", tags=["commands"])
    async def reject_order(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).reject_assignment(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post(
        "/v1/orders/{order_id}/travel:start",
        response_model=TravelStartedResponse,
        tags=["commands"],
    )
    async def start_travel(
        order_id: str,
        body: TravelInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> TravelStartedResponse:
        order, tracking = await get_service(request).start_travel(
            resolved_settings.organization_id,
            order_id,
            body.executor_id,
            session_id=body.session_id,
        )
        return TravelStartedResponse(
            order=WorkOrderResponse.from_domain(order),
            tracking_session_id=tracking.id,
        )

    @app.post(
        "/v1/tracking/{session_id}/points",
        response_model=LocationRecordedResponse,
        tags=["tracking"],
    )
    async def record_location(
        session_id: str,
        body: LocationInput,
        request: Request,
        identity: ActorIdentity = Depends(authenticate_executor),  # noqa: B008
    ) -> LocationRecordedResponse:
        tracking = await get_service(request).record_location(
            organization_id=identity.organization_id,
            executor_id=identity.actor_id,
            session_id=session_id,
            latitude=body.latitude,
            longitude=body.longitude,
            source=body.source,
            accuracy_m=body.accuracy_m,
        )
        return LocationRecordedResponse(
            tracking_session_id=tracking.id,
            point_count=len(tracking.points),
            version=tracking.version,
        )

    @app.post("/v1/orders/{order_id}/work:start", tags=["commands"])
    async def start_work(
        order_id: str,
        body: ActorInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).start_work(
            resolved_settings.organization_id, order_id, body.actor_id
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/complete", tags=["commands"])
    async def complete_order(
        order_id: str,
        body: CompleteInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).complete_order(
            resolved_settings.organization_id,
            order_id,
            body.executor_id,
            CompletionReport(
                photo_refs=body.photo_refs,
                comment=body.comment,
                signature_ref=body.signature_ref,
                customer_code=body.customer_code,
            ),
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/v1/orders/{order_id}/cancel", tags=["commands"])
    async def cancel_order(
        order_id: str,
        body: CancelInput,
        request: Request,
        _authenticated: None = Depends(authenticate),
    ) -> WorkOrderResponse:
        order = await get_service(request).cancel_order(
            resolved_settings.organization_id,
            order_id,
            body.reason,
            actor_id=body.actor_id,
        )
        return WorkOrderResponse.from_domain(order)

    @app.post("/webhooks/{provider}", tags=["webhooks"])
    async def webhook(provider: Provider, request: Request) -> dict[str, bool]:
        transport = request.app.state.transports.get(provider)
        event_inbox = request.app.state.inbox
        if transport is None or event_inbox is None:
            raise HTTPException(status_code=404, detail="Webhook is not enabled")
        _verify_webhook_secret(provider, request, resolved_settings)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > resolved_settings.webhook_max_body_bytes:
                    raise HTTPException(status_code=413, detail="Payload too large")
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="Invalid Content-Length"
                ) from None
        raw = await request.body()
        if len(raw) > resolved_settings.webhook_max_body_bytes:
            raise HTTPException(status_code=413, detail="Payload too large")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise HTTPException(status_code=400, detail="Invalid JSON") from None
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")
        await event_inbox.accept(
            provider=provider,
            external_event_id=transport.external_event_id(payload),
            organization_id=resolved_settings.organization_id,
            payload=payload,
            consumer_key=resolved_settings.consumer_key,
        )
        return {"ok": True}

    return app


def _verify_webhook_secret(
    provider: Provider,
    request: Request,
    settings: Settings,
) -> None:
    if provider is Provider.TELEGRAM:
        expected_secret = settings.telegram_webhook_secret
        header = "X-Telegram-Bot-Api-Secret-Token"
    else:
        expected_secret = settings.max_webhook_secret
        header = "X-Max-Bot-Api-Secret"
    if expected_secret is None:
        raise HTTPException(status_code=503, detail="Webhook secret is not configured")
    provided = request.headers.get(header, "")
    if not secrets.compare_digest(provided, expected_secret.get_secret_value()):
        raise HTTPException(status_code=403, detail="Forbidden")


def _body_fingerprint(body: CreateOrderInput) -> str:
    return json.dumps(
        body.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )


def _create_fingerprint(order: Any) -> str:
    value = {
        "work_type": order.work_type,
        "source": order.source,
        "requester_id": order.requester_id,
        "details": dict(order.details),
        "evidence": {
            "minimum_photos": order.evidence_requirements.minimum_photos,
            "comment_required": order.evidence_requirements.comment_required,
            "signature_required": order.evidence_requirements.signature_required,
            "customer_code_required": (
                order.evidence_requirements.customer_code_required
            ),
        },
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
