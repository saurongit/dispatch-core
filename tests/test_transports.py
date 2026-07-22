from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from dispatch_core.messaging.models import (
    OutboundButton,
    OutboundEnvelope,
    Provider,
)
from dispatch_core.transports.common import (
    decode_callback,
    encode_callback,
    group_buttons,
    stable_payload_id,
)
from dispatch_core.transports.contracts import EventKind
from dispatch_core.transports.max import MaxTransport
from dispatch_core.transports.telegram import TelegramTransport

_TEST_SECRET = "test-signing-secret-32-chars!!"


def outbound(
    provider: Provider,
    *,
    buttons: tuple[OutboundButton, ...] = (),
) -> OutboundEnvelope:
    return OutboundEnvelope(
        message_id=1,
        deduplication_key="event-1:user-1",
        organization_id="org-1",
        provider=provider,
        recipient_id="7001",
        text="Новая заявка",
        buttons=buttons,
        attempts=1,
    )


@pytest.mark.parametrize("token", ["a", "abc_123", "x" * 38])
def test_callback_codec_round_trip(token: str) -> None:
    encoded = encode_callback(token, signing_secret=_TEST_SECRET)
    assert decode_callback(encoded, signing_secret=_TEST_SECRET) == token
    assert encoded.startswith("dc2:")
    assert len(encoded.split(":")) == 3


@pytest.mark.parametrize("value", ["", "plain", "dc1:", "dc1:" + "x" * 49, None, 42])
def test_callback_decoder_rejects_invalid_or_foreign_values(value: object) -> None:
    assert decode_callback(value) is None


def test_callback_decoder_accepts_legacy_dc1_without_secret() -> None:
    assert decode_callback("dc1:legacy-token") == "legacy-token"


def test_callback_decoder_rejects_dc2_with_wrong_secret() -> None:
    encoded = encode_callback("my-token", signing_secret=_TEST_SECRET)
    assert decode_callback(encoded, signing_secret="wrong-secret") is None


def test_callback_decoder_rejects_dc2_without_secret() -> None:
    encoded = encode_callback("my-token", signing_secret=_TEST_SECRET)
    assert decode_callback(encoded) is None


@pytest.mark.parametrize("token", ["", "x" * 39])
def test_callback_encoder_rejects_invalid_length(token: str) -> None:
    with pytest.raises(ValueError, match="length"):
        encode_callback(token, signing_secret=_TEST_SECRET)


def test_stable_payload_id_ignores_mapping_order() -> None:
    first = stable_payload_id("telegram", {"a": 1, "b": 2})
    second = stable_payload_id("telegram", {"b": 2, "a": 1})
    assert first == second


def test_group_buttons_orders_rows_and_preserves_order_inside_row() -> None:
    buttons = (
        OutboundButton("third", callback_token="3", row=2),
        OutboundButton("first", callback_token="1", row=0),
        OutboundButton("second", callback_token="2", row=0),
    )
    rows = group_buttons(buttons)
    assert [[button.text for button in row] for row in rows] == [
        ["first", "second"],
        ["third"],
    ]


@pytest.mark.parametrize(
    "payload",
    [
        {"update_id": 7},
        {"update_id": 0},
        {"update_id": 2**63},
    ],
)
def test_telegram_external_event_id_uses_update_id(
    payload: dict[str, Any],
) -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    assert transport.external_event_id(payload) == f"telegram:{payload['update_id']}"


def test_telegram_parse_text_message() -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    payload = {
        "update_id": 101,
        "message": {
            "from": {"id": 7},
            "chat": {"id": 9},
            "text": "Принял",
        },
    }
    event = transport.parse(payload)[0]
    assert event.kind is EventKind.MESSAGE
    assert event.external_user_id == "7"
    assert event.chat_id == "9"
    assert event.text == "Принял"


@pytest.mark.parametrize("container", ["message", "edited_message"])
def test_telegram_parse_live_location_from_message_variants(container: str) -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    payload = {
        "update_id": 102,
        container: {
            "from": {"id": 7},
            "chat": {"id": 7},
            "location": {"latitude": 53.75, "longitude": 87.1},
        },
    }
    event = transport.parse(payload)[0]
    assert event.kind is EventKind.LOCATION
    assert event.latitude == 53.75
    assert event.longitude == 87.1


def test_telegram_parse_contact() -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_id": 103,
            "message": {
                "from": {"id": 7},
                "chat": {"id": 7},
                "contact": {"phone_number": "+70000000000"},
            },
        }
    )[0]
    assert event.kind is EventKind.CONTACT
    assert event.text == "+70000000000"


def test_telegram_parse_largest_photo() -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_id": 104,
            "message": {
                "from": {"id": 7},
                "chat": {"id": 7},
                "caption": "готово",
                "photo": [{"file_id": "small"}, {"file_id": "large"}],
            },
        }
    )[0]
    assert event.kind is EventKind.PHOTO
    assert event.media_id == "large"
    assert event.text == "готово"


def test_telegram_parse_start_with_deep_link_payload() -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_id": 105,
            "message": {
                "from": {"id": 7},
                "chat": {"id": 7},
                "text": "/start installation_3",
            },
        }
    )[0]
    assert event.kind is EventKind.START
    assert event.text == "/start installation_3"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"update_id": 1, "message": {}},
        {"update_id": 1, "callback_query": {"data": "foreign"}},
        {
            "update_id": 1,
            "message": {
                "from": {"id": 1},
                "chat": {"id": 1},
                "location": {"latitude": "bad", "longitude": 1},
            },
        },
    ],
)
def test_telegram_parse_ignores_unsupported_or_malformed_updates(
    payload: dict[str, Any],
) -> None:
    transport = TelegramTransport("token", client=httpx.AsyncClient())
    assert transport.parse(payload) == ()


def test_telegram_parse_dispatch_callback() -> None:
    transport = TelegramTransport(
        "token", signing_secret=_TEST_SECRET, client=httpx.AsyncClient()
    )
    callback_data = encode_callback("opaque-token", signing_secret=_TEST_SECRET)
    event = transport.parse(
        {
            "update_id": 106,
            "callback_query": {
                "id": "callback-1",
                "from": {"id": 7},
                "message": {"chat": {"id": 9}},
                "data": callback_data,
            },
        }
    )[0]
    assert event.kind is EventKind.CALLBACK
    assert event.callback_token == "opaque-token"
    assert event.callback_id == "callback-1"
    assert event.chat_id == "9"


@pytest.mark.asyncio
async def test_telegram_send_serializes_inline_buttons() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["json"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 88}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = TelegramTransport(
        "secret-token", signing_secret=_TEST_SECRET, client=client
    )
    result = await transport.send(
        outbound(
            Provider.TELEGRAM,
            buttons=(
                OutboundButton("Готов взять", callback_token="take-token"),
                OutboundButton("Инструкция", url="https://example.test", row=1),
            ),
        )
    )
    await client.aclose()

    assert captured["path"].endswith("/sendMessage")
    expected_hmac = encode_callback("take-token", signing_secret=_TEST_SECRET)
    assert captured["json"]["reply_markup"]["inline_keyboard"] == [
        [{"text": "Готов взять", "callback_data": expected_hmac}],
        [{"text": "Инструкция", "url": "https://example.test"}],
    ]
    assert result.external_message_id == "88"


@pytest.mark.asyncio
async def test_telegram_get_updates_returns_durable_next_offset() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["offset"] == 40
        assert "edited_message" in body["allowed_updates"]
        return httpx.Response(
            200,
            json={"ok": True, "result": [{"update_id": 40}, {"update_id": 44}]},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = TelegramTransport("token", client=client)
    updates, next_offset = await transport.get_updates(offset=40, timeout_seconds=1)
    await client.aclose()
    assert len(updates) == 2
    assert next_offset == 45


@pytest.mark.asyncio
async def test_telegram_rejects_provider_level_error_even_on_http_200() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"ok": False, "description": "bad chat"}
            )
        )
    )
    transport = TelegramTransport("token", signing_secret=_TEST_SECRET, client=client)
    with pytest.raises(RuntimeError, match="bad chat"):
        await transport.send(outbound(Provider.TELEGRAM))
    await client.aclose()


def test_max_parse_callback() -> None:
    transport = MaxTransport(
        "token", signing_secret=_TEST_SECRET, client=httpx.AsyncClient()
    )
    callback_payload = encode_callback("token-1", signing_secret=_TEST_SECRET)
    event = transport.parse(
        {
            "update_type": "message_callback",
            "callback": {
                "callback_id": "cb-1",
                "payload": callback_payload,
                "user": {"user_id": 77},
            },
        }
    )[0]
    assert event.kind is EventKind.CALLBACK
    assert event.external_user_id == "77"
    assert event.callback_token == "token-1"
    assert event.callback_id == "cb-1"


def test_max_parse_image_attachment() -> None:
    transport = MaxTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_type": "message_created",
            "message": {
                "sender": {"user_id": 77},
                "body": {
                    "mid": "mid-1",
                    "text": "отчёт",
                    "attachments": [
                        {"type": "image", "payload": {"url": "https://cdn.max.ru/a"}}
                    ],
                },
            },
        }
    )[0]
    assert event.kind is EventKind.PHOTO
    assert event.media_id == "https://cdn.max.ru/a"
    assert event.text == "отчёт"


def test_max_parse_location_attachment() -> None:
    transport = MaxTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_type": "message_created",
            "message": {
                "sender": {"user_id": 77},
                "body": {
                    "mid": "mid-2",
                    "attachments": [
                        {"type": "location", "latitude": 53.75, "longitude": 87.1}
                    ],
                },
            },
        }
    )[0]
    assert event.kind is EventKind.LOCATION
    assert event.latitude == 53.75
    assert event.longitude == 87.1


def test_max_parse_bot_started() -> None:
    transport = MaxTransport("token", client=httpx.AsyncClient())
    event = transport.parse(
        {
            "update_type": "bot_started",
            "user": {"id": 77},
            "text": "payload",
        }
    )[0]
    assert event.kind is EventKind.START
    assert event.text == "payload"


@pytest.mark.parametrize(
    "payload",
    [
        {"update_type": "unknown"},
        {"update_type": "message_created", "message": {}},
        {
            "update_type": "message_callback",
            "callback": {"user": {"id": 1}, "payload": "foreign"},
        },
    ],
)
def test_max_ignores_unsupported_or_malformed_updates(
    payload: dict[str, Any],
) -> None:
    transport = MaxTransport("token", client=httpx.AsyncClient())
    assert transport.parse(payload) == ()


@pytest.mark.asyncio
async def test_max_send_uses_authorization_header_and_native_keyboard() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers.get("Authorization")
        captured["user_id"] = request.url.params.get("user_id")
        captured["json"] = json.loads(request.content)
        return httpx.Response(200, json={"message": {"body": {"mid": "m-7"}}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = MaxTransport(
        "max-secret", signing_secret=_TEST_SECRET, client=client
    )
    result = await transport.send(
        outbound(
            Provider.MAX,
            buttons=(
                OutboundButton("Принять", callback_token="accept-token"),
                OutboundButton("Геопозиция", request_location=True, row=1),
            ),
        )
    )
    await client.aclose()

    assert captured["authorization"] == "max-secret"
    assert captured["user_id"] == "7001"
    expected_hmac = encode_callback("accept-token", signing_secret=_TEST_SECRET)
    buttons = captured["json"]["attachments"][0]["payload"]["buttons"]
    assert buttons == [
        [{"type": "callback", "text": "Принять", "payload": expected_hmac}],
        [{"type": "request_geo_location", "text": "Геопозиция"}],
    ]
    assert result.external_message_id == "m-7"


@pytest.mark.asyncio
async def test_max_get_updates_preserves_and_returns_marker() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("marker") == "50"
        return httpx.Response(
            200,
            json={"updates": [{"update_id": "u-1"}], "marker": 55},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = MaxTransport("token", client=client)
    updates, marker = await transport.get_updates(marker=50, timeout_seconds=1)
    await client.aclose()
    assert len(updates) == 1
    assert marker == 55


@pytest.mark.parametrize("requests_per_second", [0, 31, -1, 100])
def test_max_rejects_rate_above_official_boundary(
    requests_per_second: int,
) -> None:
    with pytest.raises(ValueError, match="between 1 and 30"):
        MaxTransport("token", requests_per_second=requests_per_second)


@pytest.mark.asyncio
async def test_max_send_retries_on_attachment_not_ready() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json={"code": "attachment.not.ready"})
        return httpx.Response(200, json={"message": {"body": {"mid": "m-1"}}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = MaxTransport("token", client=client)
    result = await transport.send(outbound(Provider.MAX))
    await client.aclose()
    assert call_count == 2
    assert result.external_message_id == "m-1"


@pytest.mark.asyncio
async def test_telegram_truncates_long_text() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["json"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = TelegramTransport("token", signing_secret=_TEST_SECRET, client=client)
    long_text = "x" * 5000
    await transport.send(
        OutboundEnvelope(
            message_id=1,
            deduplication_key="test",
            organization_id="org",
            provider=Provider.TELEGRAM,
            recipient_id="123",
            text=long_text,
            buttons=(),
            attempts=1,
        )
    )
    await client.aclose()
    assert len(captured["json"]["text"]) == 4096


@pytest.mark.asyncio
async def test_telegram_raises_on_429() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(
                429,
                json={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 10},
                },
            )
        )
    )
    transport = TelegramTransport("token", signing_secret=_TEST_SECRET, client=client)
    from dispatch_core.transports.telegram import TelegramRateLimitError

    with pytest.raises(TelegramRateLimitError) as exc_info:
        await transport.send(outbound(Provider.TELEGRAM))
    await client.aclose()
    assert exc_info.value.retry_after == 10


@pytest.mark.asyncio
async def test_max_raises_on_429() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(
                429, json={"code": "rate_limit", "retry_after": 15}
            )
        )
    )
    transport = MaxTransport("token", client=client)
    from dispatch_core.transports.max import MaxRateLimitError

    with pytest.raises(MaxRateLimitError) as exc_info:
        await transport.send(outbound(Provider.MAX))
    await client.aclose()
    assert exc_info.value.retry_after == 15
