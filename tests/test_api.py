from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
from dispatch_core.infrastructure.operations import (
    OperationsSnapshot,
    QueueStatus,
    WorkerHeartbeat,
)
from dispatch_core.infrastructure.read_models import (
    AsyncMemoryOrderReader,
    AsyncMemoryTrackingViewReader,
)
from dispatch_core.infrastructure.workflow_store import IntakeAddressSelection
from dispatch_core.messaging.models import Provider
from dispatch_core.transports.common import encode_executor_token

ADMIN_KEY = "test-admin-key-that-is-at-least-32-characters"
EXECUTOR_SECRET = "test-executor-token-signing-secret-32-chars!"
TELEGRAM_WEBHOOK_SECRET = "telegram-webhook-secret-at-least-32-characters"
MAX_WEBHOOK_SECRET = "max-webhook-secret-at-least-32-characters"


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


@dataclass
class FakeIntakeAddressStore:
    selection: IntakeAddressSelection | None = None
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def select_address_by_token(
        self, **values: Any
    ) -> IntakeAddressSelection | None:
        self.calls.append(values)
        return self.selection


@dataclass
class FakeOperationsStore:
    calls: list[tuple[str, int]] = field(default_factory=list)

    async def snapshot(
        self,
        organization_id: str,
        *,
        worker_stale_after_seconds: int,
    ) -> OperationsSnapshot:
        self.calls.append((organization_id, worker_stale_after_seconds))
        now = datetime.now(UTC)
        return OperationsSnapshot(
            organization_id=organization_id,
            generated_at=now,
            queues=(QueueStatus("inbox", "dead", 2, now),),
            workers=(WorkerHeartbeat("client", "worker-1", now, now, True),),
        )


@asynccontextmanager
async def api_client(
    *,
    inbox: FakeInbox | None = None,
    transports: dict[Provider, FakeTransport] | None = None,
    identities: FakeIdentityConfig | None = None,
    operations: FakeOperationsStore | None = None,
    intake_sessions: FakeIntakeAddressStore | None = None,
    telegram_secret: str | None = TELEGRAM_WEBHOOK_SECRET,
    max_secret: str | None = MAX_WEBHOOK_SECRET,
    webhook_max_body_bytes: int = 1_048_576,
    environment: str = "production",
    executor_token_secret: str | None = EXECUTOR_SECRET,
    public_base_url: str | None = "https://dispatch.example",
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
        executor_token_secret=executor_token_secret,
        public_base_url=public_base_url,
    )
    app = create_app(
        settings,
        service=AsyncDispatchService(factory),
        reader=AsyncMemoryOrderReader(store),
        tracking_reader=AsyncMemoryTrackingViewReader(store),
        intake_sessions=intake_sessions,  # type: ignore[arg-type]
        inbox=inbox,  # type: ignore[arg-type]
        identities=identities,  # type: ignore[arg-type]
        operations=operations,  # type: ignore[arg-type]
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


def executor_headers(organization_id: str, actor_id: str) -> dict[str, str]:
    token, _ = encode_executor_token(
        organization_id, actor_id, signing_secret=EXECUTOR_SECRET
    )
    return {"Authorization": f"Bearer {token}"}


def order_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "work_type": "lift_repair",
        "source": "phone",
        "requester_id": "resident-7",
        "details": {
            "address": "Demo street",
            "asset": "lift-42",
            "service_location": {"latitude": 53.76, "longitude": 87.12},
        },
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


async def start_tracking(
    client: httpx.AsyncClient,
    *,
    key: str = "tracking-request-0001",
    executor_id: str = "executor-1",
) -> tuple[str, httpx.Response]:
    created = await create_order(client, key=key)
    order_id = created.json()["id"]
    assigned = await client.post(
        f"/v1/orders/{order_id}/assign",
        headers=auth_headers(),
        json={"executor_id": executor_id},
    )
    assert assigned.status_code == 200, assigned.text
    accepted = await client.post(
        f"/v1/orders/{order_id}/accept",
        headers=auth_headers(),
        json={"actor_id": executor_id},
    )
    assert accepted.status_code == 200, accepted.text
    travel = await client.post(
        f"/v1/orders/{order_id}/travel:start",
        headers=auth_headers(),
        json={"executor_id": executor_id},
    )
    assert travel.status_code == 200, travel.text
    return order_id, travel


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
async def test_operations_queue_status_requires_admin_auth() -> None:
    operations = FakeOperationsStore()
    async with api_client(operations=operations) as (client, _):
        unauthorized = await client.get("/v1/operations/queues")
        response = await client.get(
            "/v1/operations/queues",
            headers=auth_headers(),
        )
    assert unauthorized.status_code == 401
    assert response.status_code == 200
    assert response.json()["queues"][0] == {
        "queue": "inbox",
        "status": "dead",
        "count": 2,
        "oldest_at": response.json()["queues"][0]["oldest_at"],
    }
    assert response.json()["workers"][0]["healthy"] is True
    assert operations.calls == [("org-1", 45)]


@pytest.mark.asyncio
async def test_schema_endpoints_visibility_depends_on_environment() -> None:
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
        fetched = await client.get(f"/v1/orders/{order_id}", headers=auth_headers())
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
    assert [event.name for event in store.outbox_events] == ["work_order.submitted"]


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
        response = await client.get("/v1/orders/unknown", headers=auth_headers())
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
                "role": "master",
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
            "role": "master",
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
            "role": "master",
            "display_name": "Executor",
            "provider": "telegram",
        },
        {
            "actor_id": "executor-1",
            "role": "master",
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
            headers=executor_headers("org-1", "executor-2"),
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
    assert location.status_code == 200, location.json()
    assert location.json()["point_count"] == 1
    assert started.json()["status"] == "in_progress"
    assert completed.status_code == 200
    assert completed.json()["status"] == "completed"
    assert len(store.outbox_events) == 13


@pytest.mark.asyncio
async def test_public_tracking_returns_only_latest_safe_snapshot() -> None:
    async with api_client() as (client, _):
        _, travel = await start_tracking(client)
        tracking_url = travel.json()["tracking_url"]
        token = tracking_url.split("#", maxsplit=1)[1]
        recorded = await client.post(
            f"/v1/tracking/{travel.json()['tracking_session_id']}/points",
            headers=executor_headers("org-1", "executor-1"),
            json={
                "latitude": 53.7557,
                "longitude": 87.1099,
                "accuracy_m": 7.5,
                "source": "telegram",
            },
        )
        response = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": token},
        )

    assert recorded.status_code == 200
    assert tracking_url.startswith("https://dispatch.example/track#")
    assert "?" not in tracking_url
    assert response.status_code == 200
    assert response.json() == {
        "brand": "Test Organization",
        "public_number": "A000",
        "work_type": "lift_repair",
        "address": "Demo street",
        "client_point": {"latitude": 53.76, "longitude": 87.12},
        "master_name": "executor-1",
        "order_status": "en_route",
        "tracking_status": "active",
        "point_count": 1,
        "latest_point": {
            "latitude": 53.7557,
            "longitude": 87.1099,
            "captured_at": response.json()["latest_point"]["captured_at"],
            "accuracy_m": 7.5,
        },
        "updated_at": response.json()["updated_at"],
    }
    serialized = response.text
    assert token not in serialized
    assert "resident-7" not in serialized
    assert "phone" not in serialized
    assert response.headers["cache-control"] == "no-store, private"
    assert response.headers["x-robots-tag"] == "noindex, nofollow"


@pytest.mark.asyncio
async def test_public_tracking_uses_generic_not_found_for_bad_credentials() -> None:
    async with api_client() as (client, _):
        missing = await client.get("/v1/public/tracking")
        malformed = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": "short"},
        )
        unknown = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": "x" * 43},
        )

    assert {missing.status_code, malformed.status_code, unknown.status_code} == {404}
    assert (
        missing.json()
        == malformed.json()
        == unknown.json()
        == {"detail": "Tracking link is unavailable"}
    )


@pytest.mark.asyncio
async def test_tracking_page_keeps_token_in_fragment_and_has_security_headers() -> None:
    async with api_client() as (client, _):
        response = await client.get("/track")

    assert response.status_code == 200
    assert "OpenStreetMap contributors" in response.text
    assert "Место выполнения работы" in response.text
    assert "🚗" in response.text
    assert "status-flow" in response.text
    assert "location.hash" in response.text
    assert "X-Tracking-Token" in response.text
    assert "tile.openstreetmap.org" in response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"


@pytest.mark.asyncio
async def test_address_page_explains_vpn_and_uses_locked_fragment_flow() -> None:
    async with api_client() as (client, _):
        response = await client.get("/address")

    assert response.status_code == 200
    assert "VPN обычно не меняет GPS" in response.text
    assert "Первое нажатие разблокирует карту" in response.text
    assert "location.hash" in response.text
    assert "X-Intake-Token" in response.text
    assert "OpenStreetMap contributors" in response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


@pytest.mark.asyncio
async def test_address_capability_saves_point_without_echoing_token() -> None:
    token = "i" * 43
    store = FakeIntakeAddressStore(
        selection=IntakeAddressSelection(
            organization_id="org-1",
            actor_id="client-1",
            provider=Provider.TELEGRAM,
            address="Ленина 10",
            latitude=53.75,
            longitude=87.1,
        )
    )
    async with api_client(intake_sessions=store) as (client, _):
        response = await client.post(
            "/v1/public/intake/location",
            headers={"X-Intake-Token": token},
            json={
                "latitude": 53.75,
                "longitude": 87.1,
                "address": "Ленина 10",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"saved": True, "address": "Ленина 10"}
    assert token not in response.text
    assert store.calls == [
        {
            "token": token,
            "latitude": 53.75,
            "longitude": 87.1,
            "address": "Ленина 10",
        }
    ]
    assert response.headers["cache-control"] == "no-store, private"


@pytest.mark.asyncio
async def test_address_capability_hides_missing_and_unknown_tokens() -> None:
    store = FakeIntakeAddressStore()
    async with api_client(intake_sessions=store) as (client, _):
        missing = await client.post(
            "/v1/public/intake/location",
            json={"latitude": 53.75, "longitude": 87.1},
        )
        unknown = await client.post(
            "/v1/public/intake/location",
            headers={"X-Intake-Token": "u" * 43},
            json={"latitude": 53.75, "longitude": 87.1},
        )
    assert missing.status_code == unknown.status_code == 404
    assert missing.json() == unknown.json() == {"detail": "Address link is unavailable"}


@pytest.mark.asyncio
async def test_browser_location_capability_records_point_for_active_master() -> None:
    async with api_client() as (client, store):
        _, travel = await start_tracking(client)
        session = next(iter(store.tracking_sessions.values()))
        location_token = session.location_token
        assert location_token is not None
        submitted = await client.post(
            "/v1/public/location",
            headers={"X-Location-Token": location_token},
            json={
                "latitude": 53.8,
                "longitude": 87.2,
                "accuracy_m": 12.0,
                "captured_at": "2026-07-22T09:30:00Z",
                "event_id": "browser-fix-1",
            },
        )
        viewer_token = travel.json()["tracking_url"].split("#", maxsplit=1)[1]
        viewed = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": viewer_token},
        )

    assert submitted.status_code == 200
    assert submitted.json()["point_count"] == 1
    assert viewed.json()["latest_point"]["latitude"] == 53.8
    assert location_token not in submitted.text


@pytest.mark.asyncio
async def test_tracking_capabilities_cannot_be_used_for_opposite_operation() -> None:
    async with api_client() as (client, store):
        _, travel = await start_tracking(client)
        viewer_token = travel.json()["tracking_url"].split("#", maxsplit=1)[1]
        location_token = next(iter(store.tracking_sessions.values())).location_token
        assert location_token is not None
        viewer_cannot_write = await client.post(
            "/v1/public/location",
            headers={"X-Location-Token": viewer_token},
            json={
                "latitude": 53.8,
                "longitude": 87.2,
                "event_id": "wrong-capability",
            },
        )
        sender_cannot_read = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": location_token},
        )

    assert viewer_cannot_write.status_code == 404
    assert sender_cannot_read.status_code == 404


@pytest.mark.asyncio
async def test_location_share_page_uses_fragment_and_browser_geolocation() -> None:
    async with api_client() as (client, _):
        response = await client.get("/track/share")

    assert response.status_code == 200
    assert "location.hash" in response.text
    assert "X-Location-Token" in response.text
    assert "navigator.geolocation.watchPosition" in response.text
    assert "geolocation=(self)" in response.headers["permissions-policy"]
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_completion_immediately_revokes_public_tracking_link() -> None:
    async with api_client() as (client, _):
        order_id, travel = await start_tracking(client)
        token = travel.json()["tracking_url"].split("#", maxsplit=1)[1]
        started = await client.post(
            f"/v1/orders/{order_id}/work:start",
            headers=auth_headers(),
            json={"actor_id": "executor-1"},
        )
        assert started.status_code == 200
        completed = await client.post(
            f"/v1/orders/{order_id}/complete",
            headers=auth_headers(),
            json={
                "executor_id": "executor-1",
                "photo_refs": ["object://proof"],
                "comment": "done",
            },
        )
        revoked = await client.get(
            "/v1/public/tracking",
            headers={"X-Tracking-Token": token},
        )

    assert completed.status_code == 200
    assert revoked.status_code == 404


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
        fetched = await client.get(f"/v1/orders/{order_id}", headers=auth_headers())

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
        fetched = await client.get(f"/v1/orders/{order_id}", headers=auth_headers())
    assert failed.status_code == 409
    assert fetched.json()["status"] == "in_progress"


@pytest.mark.parametrize(
    ("provider", "header", "secret"),
    [
        (
            Provider.TELEGRAM,
            "X-Telegram-Bot-Api-Secret-Token",
            TELEGRAM_WEBHOOK_SECRET,
        ),
        (Provider.MAX, "X-Max-Bot-Api-Secret", MAX_WEBHOOK_SECRET),
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
            "consumer_key": "",
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
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
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
            headers={"X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET},
            json={"update_id": 1},
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_webhook_rejects_non_object_json() -> None:
    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={"X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET},
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
                "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET,
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
                "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET,
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
                "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET,
                "Content-Length": "1025",
            },
            content=b"{}",
        )
    assert response.status_code == 413


@pytest.mark.asyncio
async def test_webhook_stops_buffering_oversized_chunked_payload() -> None:
    async def body():
        yield b"{" + (b"x" * 700)
        yield b"y" * 400

    async with api_client(
        inbox=FakeInbox(),
        transports={Provider.TELEGRAM: FakeTransport(Provider.TELEGRAM)},
        webhook_max_body_bytes=1024,
    ) as (client, _):
        response = await client.post(
            "/webhooks/telegram",
            headers={
                "X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET,
                "Content-Type": "application/json",
            },
            content=body(),
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


# ---------------------------------------------------------------------------
# Executor auth negative tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracking_rejects_missing_executor_token() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/tracking/track-1/points",
            json={
                "latitude": 55.0,
                "longitude": 37.0,
                "source": "telegram",
            },
        )
    assert response.status_code == 401
    assert "Bearer" in response.json()["detail"]


@pytest.mark.asyncio
async def test_tracking_rejects_invalid_executor_token() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/tracking/track-1/points",
            headers={"Authorization": "Bearer dt1:org-1:ivan:9999999999:bad"},
            json={
                "latitude": 55.0,
                "longitude": 37.0,
                "source": "telegram",
            },
        )
    assert response.status_code == 401
    assert "Invalid" in response.json()["detail"]


@pytest.mark.asyncio
async def test_tracking_rejects_token_with_wrong_secret() -> None:
    other_token, _ = encode_executor_token(
        "org-1", "ivan-7", signing_secret="wrong-secret-32-chars-long!!!!!"
    )
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/tracking/track-1/points",
            headers={"Authorization": f"Bearer {other_token}"},
            json={
                "latitude": 55.0,
                "longitude": 37.0,
                "source": "telegram",
            },
        )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_tracking_rejects_token_for_wrong_org() -> None:
    token, _ = encode_executor_token(
        "other-org", "executor-2", signing_secret=EXECUTOR_SECRET
    )
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/tracking/track-1/points",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "latitude": 55.0,
                "longitude": 37.0,
                "source": "telegram",
            },
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_executor_token_endpoint_requires_admin_auth() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/v1/auth/executor-token",
            json={"actor_id": "ivan-7"},
        )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_executor_token_endpoint_rejects_missing_secret() -> None:
    async with api_client(executor_token_secret=None) as (client, _):
        response = await client.post(
            "/v1/auth/executor-token",
            headers=auth_headers(),
            json={"actor_id": "ivan-7"},
        )
    assert response.status_code == 503
    assert "not configured" in response.json()["detail"]
