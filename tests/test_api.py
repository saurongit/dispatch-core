from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from dispatch_core.api import create_app
from dispatch_core.api.settings import Settings
from dispatch_core.application.async_service import AsyncDispatchService
from dispatch_core.infrastructure.async_memory import (
    AsyncMemoryStore,
    AsyncMemoryUnitOfWorkFactory,
)
from dispatch_core.infrastructure.read_models import AsyncMemoryOrderReader
from dispatch_core.messaging.models import Provider

ADMIN_KEY = "test-admin-key-that-is-at-least-32-characters"


@dataclass
class FakeInbox:
    accepted: list[dict[str, Any]] = field(default_factory=list)

    async def accept(self, **values: Any) -> bool:
        self.accepted.append(values)
        return True


@dataclass
class FakeTransport:
    provider: Provider
    closed: bool = False

    def external_event_id(self, payload: dict[str, Any]) -> str:
        return f"{self.provider.value}:{payload.get('update_id', 'fallback')}"

    async def close(self) -> None:
        self.closed = True


@dataclass
class FakeIdentityConfig:
    actors: list[dict[str, Any]] = field(default_factory=list)

    async def upsert_actor(self, **values: Any) -> None:
        self.actors.append(values)


@asynccontextmanager
async def api_client(
    *,
    inbox: FakeInbox | None = None,
    transports: dict[Provider, FakeTransport] | None = None,
    identities: FakeIdentityConfig | None = None,
    telegram_secret: str | None = "telegram-webhook-secret",
    max_secret: str | None = "max-webhook-secret",
    webhook_max_body_bytes: int = 1_048_576,
    environment: str = "production",
) -> AsyncIterator[tuple[httpx.AsyncClient, AsyncMemoryStore]]:
    store = AsyncMemoryStore()
    factory = AsyncMemoryUnitOfWorkFactory(store)
    settings = Settings(
        database_url="postgresql://not-used",
        admin_api_key=ADMIN_KEY,
        organization_id="org-1",
        organization_name="Test Organization",
        telegram_webhook_secret=telegram_secret,
        max_webhook_secret=max_secret,
        webhook_max_body_bytes=webhook_max_body_bytes,
        environment=environment,
    )
    app = create_app(
        settings,
        service=AsyncDispatchService(factory),
        reader=AsyncMemoryOrderReader(store),
        inbox=inbox,  # type: ignore[arg-type]
        identities=identities,  # type: ignore[arg-type]
        transports=transports,  # type: ignore[arg-type]
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            yield client, store


def auth_headers(*, idempotency_key: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {ADMIN_KEY}"}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def order_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "work_type": "lift_repair",
        "source": "phone",
        "requester_id": "resident-7",
        "details": {"address": "Demo street", "asset": "lift-42"},
        "evidence": {"minimum_photos": 1, "comment_required": True},
    }
    body.update(overrides)
    return body


async def create_order(
    client: httpx.AsyncClient,
    *,
    key: str = "request-key-0001",
    body: dict[str, Any] | None = None,
) -> httpx.Response:
    return await client.post(
        "/v1/orders",
        headers=auth_headers(idempotency_key=key),
        json=body or order_body(),
    )


@pytest.mark.asyncio
async def test_health_endpoints_are_public_and_ready_when_injected() -> None:
    async with api_client() as (client, _):
        live = await client.get("/health/live")
        ready = await client.get("/health/ready")
    assert live.status_code == 200
    assert live.json() == {"status": "alive"}
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_schema_endpoints_are_hidden_in_production_and_visible_in_development(
) -> None:
    async with api_client() as (client, _):
        production = await client.get("/openapi.json")
    async with api_client(environment="development") as (client, _):
        development = await client.get("/openapi.json")
    assert production.status_code == 404
    assert development.status_code == 200


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Basic abc"},
        {"Authorization": "Bearer wrong-key"},
    ],
)
@pytest.mark.asyncio
async def test_order_api_rejects_missing_or_invalid_bearer(
    headers: dict[str, str],
) -> None:
    async with api_client() as (client, _):
        response = await client.get("/v1/orders/missing", headers=headers)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_and_read_order() -> None:
    async with api_client() as (client, _):
        created = await create_order(client)
        order_id = created.json()["id"]
        fetched = await client.get(
            f"/v1/orders/{order_id}", headers=auth_headers()
        )
    assert created.status_code == 201
    assert created.json()["status"] == "submitted"
    assert created.json()["details"]["asset"] == "lift-42"
    assert fetched.status_code == 200
    assert fetched.json() == created.json()


@pytest.mark.asyncio
async def test_create_requires_idempotency_key() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/orders",
            headers=auth_headers(),
            json=order_body(),
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_retry_returns_same_order_without_second_event() -> None:
    async with api_client() as (client, store):
        first = await create_order(client)
        second = await create_order(client)
    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    assert len(store.orders) == 1
    assert [event.name for event in store.outbox_events] == [
        "work_order.submitted"
    ]


@pytest.mark.asyncio
async def test_idempotency_key_cannot_be_reused_for_different_body() -> None:
    async with api_client() as (client, _):
        first = await create_order(client)
        second = await create_order(
            client,
            body=order_body(work_type="another_job"),
        )
    assert first.status_code == 201
    assert second.status_code == 409
    assert "another request" in second.json()["detail"]


@pytest.mark.asyncio
async def test_unknown_order_returns_404() -> None:
    async with api_client() as (client, _):
        response = await client.get(
            "/v1/orders/unknown", headers=auth_headers()
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_admin_can_bind_actor_to_messenger_identity() -> None:
    identities = FakeIdentityConfig()
    async with api_client(identities=identities) as (client, _):
        response = await client.post(
            "/v1/actors",
            headers=auth_headers(),
            json={
                "actor_id": "executor-1",
                "role": "executor",
                "display_name": "Executor One",
                "provider": "telegram",
                "external_user_id": "7001",
            },
        )
    assert response.status_code == 201
    assert response.json()["provider"] == "telegram"
    assert identities.actors == [
        {
            "organization_id": "org-1",
            "actor_id": "executor-1",
            "role": "executor",
            "display_name": "Executor One",
            "provider": Provider.TELEGRAM,
            "external_user_id": "7001",
        }
    ]


@pytest.mark.parametrize(
    "body",
    [
        {
            "actor_id": "executor-1",
            "role": "executor",
            "display_name": "Executor",
            "provider": "telegram",
        },
        {
            "actor_id": "executor-1",
            "role": "executor",
            "display_name": "Executor",
            "external_user_id": "7001",
        },
    ],
)
@pytest.mark.asyncio
async def test_actor_identity_fields_must_be_paired(
    body: dict[str, Any],
) -> None:
    async with api_client(identities=FakeIdentityConfig()) as (client, _):
        response = await client.post(
            "/v1/actors",
            headers=auth_headers(),
            json=body,
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_full_curated_api_flow_with_tracking_and_evidence() -> None:
    async with api_client() as (client, store):
        created = await create_order(client)
        order_id = created.json()["id"]
        commands = [
            ("coordination:claim", {"actor_id": "operator-1"}),
            (
                "pool:publish",
                {"mode": "curated", "actor_id": "operator-1"},
            ),
            ("pool:interest", {"actor_id": "executor-1"}),
            ("pool:interest", {"actor_id": "executor-2"}),
            (
                "assign",
                {"executor_id": "executor-2", "actor_id": "operator-1"},
            ),
            ("accept", {"actor_id": "executor-2"}),
        ]
        for suffix, body in commands:
            response = await client.post(
                f"/v1/orders/{order_id}/{suffix}",
                headers=auth_headers(),
                json=body,
            )
            assert response.status_code == 200, response.text

        travel = await client.post(
            f"/v1/orders/{order_id}/travel:start",
            headers=auth_headers(),
            json={"executor_id": "executor-2", "session_id": "track-1"},
        )
        location = await client.post(
            "/v1/tracking/track-1/points",
            headers=auth_headers(),
            json={
                "latitude": 53.75,
                "longitude": 87.1,
                "accuracy_m": 8.0,
                "source": "telegram",
            },
        )
        started = await client.post(
            f"/v1/orders/{order_id}/work:start",
            headers=auth_headers(),
            json={"actor_id": "executor-2"},
        )
        completed = await client.post(
            f"/v1/orders/{order_id}/complete",
            headers=auth_headers(),
            json={
                "executor_id": "executor-2",
                "photo_refs": ["object://photo-1"],
                "comment": "Лифт восстановлен",
            },
        )

    assert travel.status_code == 200
    assert travel.json()["tracking_session_id"] == "track-1"
    assert location.json()["point_count"] == 1
    assert started.json()["status"] == "in_progress"
    assert completed.status_code == 200
    assert completed.json()["status"] == "completed"
    assert len(store.outbox_events) == 13


@pytest.mark.asyncio
async def test_first_claim_api_race_has_exactly_one_winner() -> None:
    async with api_client() as (client, _):
        created = await create_order(client)
        order_id = created.json()["id"]
        published = await client.post(
            f"/v1/orders/{order_id}/pool:publish",
            headers=auth_headers(),
            json={"mode": "first_claim"},
        )
        assert published.status_code == 200

        async def claim(executor_id: str) -> httpx.Response:
            return await client.post(
                f"/v1/orders/{order_id}/pool:claim",
                headers=auth_headers(),
                json={"actor_id": executor_id},
            )

        responses = await asyncio.gather(
            claim("executor-1"),
            claim("executor-2"),
        )
        fetched = await client.get(
            f"/v1/orders/{order_id}", headers=auth_headers()
        )

    assert sorted(response.status_code for response in responses) == [200, 409]
    assert fetched.json()["assignee_id"] in {"executor-1", "executor-2"}


@pytest.mark.asyncio
async def test_executor_cannot_hold_two_active_orders() -> None:
    async with api_client() as (client, _):
        first = await create_order(client, key="request-key-first")
        second = await create_order(client, key="request-key-second")
        first_assignment = await client.post(
            f"/v1/orders/{first.json()['id']}/assign",
            headers=auth_headers(),
            json={"executor_id": "executor-1"},
        )
        second_assignment = await client.post(
            f"/v1/orders/{second.json()['id']}/assign",
            headers=auth_headers(),
            json={"executor_id": "executor-1"},
        )
    assert first_assignment.status_code == 200
    assert second_assignment.status_code == 409
    assert "another active" in second_assignment.json()["detail"]


@pytest.mark.asyncio
async def test_completion_validation_does_not_change_state() -> None:
    async with api_client() as (client, _):
        created = await create_order(client)
        order_id = created.json()["id"]
        for suffix in ("assign", "accept", "work:start"):
            body = (
                {"executor_id": "executor-1"}
                if suffix == "assign"
                else {"actor_id": "executor-1"}
            )
            response = await client.post(
                f"/v1/orders/{order_id}/{suffix}",
                headers=auth_headers(),
                json=body,
            )
            assert response.status_code == 200
        failed = await client.post(
            f"/v1/orders/{order_id}/complete",
            headers=auth_headers(),
            json={"executor_id": "executor-1"},
        )
        fetched = await client.get(
            f"/v1/orders/{order_id}", headers=auth_headers()
        )
    assert failed.status_code == 409
    assert fetched.json()["status"] == "in_progress"


@pytest.mark.parametrize(
    ("provider", "header", "secret"),
    [
        (
            Provider.TELEGRAM,
            "X-Telegram-Bot-Api-Secret-Token",
            "telegram-webhook-secret",
        ),
        (Provider.MAX, "X-Max-Bot-Api-Secret", "max-webhook-secret"),
    ],
)
@pytest.mark.asyncio
async def test_webhook_persists_raw_event_before_processing(
    provider: Provider,
    header: str,
    secret: str,
) -> None:
    inbox = FakeInbox()
    transport = FakeTransport(provider)
    async with api_client(
        inbox=inbox,
        transports={provider: transport},
    ) as (client, _):
        response = await client.post(
            f"/webhooks/{provider.value}",
            headers={header: secret},
            json={"update_id": 77, "message": {"text": "hello"}},
        )
    assert response.status_code == 200
    assert inbox.accepted == [
        {
            "provider": provider,
            "external_event_id": f"{provider.value}:77",
            "organization_id": "org-1",
            "payload": {"update_id": 77, "message": {"text": "hello"}},
        }
    ]
    assert transport.closed


@pytest.mark.parametrize("provided", [None, "", "wrong-secret"])
@pytest.mark.asyncio
async def test_telegram_webhook_rejects_invalid_secret(
    provided: str | None,
) -> None:
    headers = {}
    if provided is not None:
        headers["X-Telegram-Bot-Api-Secret-Token"] = provided
    inbox = FakeInbox()
    async with api_client(
        inbox=inbox,
        transports={
            Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)
        },
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers=headers,
            json={"update_id": 1},
        )
    assert response.status_code == 403
    assert inbox.accepted == []


@pytest.mark.asyncio
async def test_disabled_webhook_is_not_discoverable() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "telegram-webhook-secret"
            },
            json={"update_id": 1},
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_webhook_rejects_non_object_json() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={
            Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)
        },
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "telegram-webhook-secret"
            },
            json=[1, 2, 3],
        )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_json() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "telegram-webhook-secret",
                "Content-Type": "application/json",
            },
            content=b"{not-json",
        )
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid JSON"


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_content_length() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "telegram-webhook-secret",
                "Content-Length": "invalid",
            },
            content=b"{}",
        )
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid Content-Length"


@pytest.mark.asyncio
async def test_webhook_rejects_declared_oversized_payload() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
        webhook_max_body_bytes=1024,
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": "telegram-webhook-secret",
                "Content-Length": "1025",
            },
            content=b"{}",
        )
    assert response.status_code == 413


@pytest.mark.asyncio
async def test_webhook_requires_configured_provider_secret() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
        telegram_secret=None,
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            json={"update_id": 1},
        )
    assert response.status_code == 503
