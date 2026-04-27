"""Tests for the Webhooks resource."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from thesma._generated.models import (
    WebhookCreateResponse,
    WebhookDeliveryResponse,
    WebhookEventTypeResponse,
    WebhookReplayResponse,
    WebhookResponse,
    WebhookSecretRotateResponse,
    WebhookTestResponse,
)
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import BadRequestError, ThesmaError
from thesma.resources.webhooks import Webhooks

BASE = "https://api.thesma.dev"


EVENT_TYPES_JSON = {
    "data": [
        {
            "event_type": "filing.created",
            "description": "Triggered when a new SEC filing is indexed.",
            "category": "filings",
            "payload_schema_url": "https://docs.thesma.dev/webhooks/schemas/filing.created",
        },
        {
            "event_type": "corporate_event.created",
            "description": "Triggered when a corporate event is recorded.",
            "category": "corporate_events",
            "payload_schema_url": "https://docs.thesma.dev/webhooks/schemas/corporate_event.created",
        },
        {
            "event_type": "compensation.filed",
            "description": "Triggered when an executive compensation filing is processed.",
            "category": "compensation",
            "payload_schema_url": "https://docs.thesma.dev/webhooks/schemas/compensation.filed",
        },
        {
            "event_type": "board.changed",
            "description": "Triggered when a board membership change is detected.",
            "category": "governance",
            "payload_schema_url": "https://docs.thesma.dev/webhooks/schemas/board.changed",
        },
        {
            "event_type": "amendment.filed",
            "description": "Triggered when a filing amendment is indexed.",
            "category": "filings",
            "payload_schema_url": "https://docs.thesma.dev/webhooks/schemas/amendment.filed",
        },
    ],
}


WEBHOOK_ITEM = {
    "id": "sub_123",
    "url": "https://example.com/hook",
    "events": ["filing.created"],
    "filing_types": None,
    "is_active": True,
    "consecutive_failure_count": 0,
    "description": None,
    "created_at": "2026-04-26T00:00:00Z",
    "updated_at": "2026-04-26T00:00:00Z",
    "last_delivery_at": None,
    "success_rate_last_100": None,
}


WEBHOOK_RESPONSE_JSON = {"data": WEBHOOK_ITEM}

WEBHOOK_LIST_JSON = {"data": [WEBHOOK_ITEM]}


WEBHOOK_CREATE_RESPONSE_JSON = {
    "data": {
        **WEBHOOK_ITEM,
        "secret": "wh_secret_0123456789abcdef0123456789abcdef",
    },
}


DELIVERY_ITEM = {
    "id": "del_456",
    "subscription_id": "sub_123",
    "event_type": "filing.created",
    "payload": {"foo": "bar"},
    "status": "delivered",
    "http_status": 200,
    "attempt_count": 1,
    "next_retry_at": None,
    "created_at": "2026-04-26T00:00:00Z",
    "completed_at": "2026-04-26T00:00:01Z",
    "response_body": "ok",
    "triggered_by": "original",
}


DELIVERIES_PAGINATED_JSON = {
    "data": [DELIVERY_ITEM],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


ROTATE_SECRET_JSON = {
    "data": {
        "id": "sub_123",
        "secret": "wh_secret_fedcba9876543210fedcba9876543210",
    },
}


TEST_DELIVERY_JSON = {
    "data": {
        "test_delivery_id": "del_test_001",
        "queued_at": "2026-04-26T00:00:00Z",
    },
}


REPLAY_JSON = {
    "data": {
        "replay_delivery_id": "del_replay_001",
        "queued_at": "2026-04-26T00:00:00Z",
    },
}


class TestWebhookEventTypes:
    @respx.mock
    def test_list_event_types(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/event-types").mock(
            return_value=httpx.Response(200, json=EVENT_TYPES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.list_event_types()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 5
        assert isinstance(result.data[0], WebhookEventTypeResponse)
        assert result.data[0].event_type == "filing.created"
        client.close()


class TestWebhookList:
    @respx.mock
    def test_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(200, json=WEBHOOK_LIST_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.list()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 1
        assert isinstance(result.data[0], WebhookResponse)
        assert result.data[0].id == "sub_123"
        client.close()


class TestWebhookCreate:
    @respx.mock
    def test_create_minimal_body(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=WEBHOOK_CREATE_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.create(
            url="https://example.com/hook",
            events=["filing.created"],
        )

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {
            "url": "https://example.com/hook",
            "events": ["filing.created"],
        }
        assert "filing_types" not in body
        assert "description" not in body
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookCreateResponse)
        client.close()

    @respx.mock
    def test_create_full_body(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=WEBHOOK_CREATE_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.create(
            url="https://example.com/hook",
            events=["filing.created", "board.changed"],
            filing_types=["10-K", "10-Q"],
            description="my hook",
        )

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {
            "url": "https://example.com/hook",
            "events": ["filing.created", "board.changed"],
            "filing_types": ["10-K", "10-Q"],
            "description": "my hook",
        }
        client.close()

    @respx.mock
    def test_create_response_includes_secret(self, api_key: str) -> None:
        respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=WEBHOOK_CREATE_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.create(
            url="https://example.com/hook",
            events=["filing.created"],
        )

        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookCreateResponse)
        assert result.data.secret
        assert result.data.secret == "wh_secret_0123456789abcdef0123456789abcdef"
        client.close()


class TestWebhookGet:
    @respx.mock
    def test_get(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/sub_123").mock(
            return_value=httpx.Response(200, json=WEBHOOK_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.get("sub_123")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookResponse)
        assert result.data.id == "sub_123"
        client.close()


class TestWebhookUpdate:
    @respx.mock
    def test_update_single_field(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/sub_123").mock(
            return_value=httpx.Response(200, json=WEBHOOK_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.update("sub_123", is_active=False)

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {"is_active": False}
        client.close()

    @respx.mock
    def test_update_multiple_fields(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/sub_123").mock(
            return_value=httpx.Response(200, json=WEBHOOK_RESPONSE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.update(
            "sub_123",
            url="https://new.com/hook",
            events=["board.changed"],
        )

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {
            "url": "https://new.com/hook",
            "events": ["board.changed"],
        }
        # None-valued kwargs are stripped.
        assert "filing_types" not in body
        assert "is_active" not in body
        assert "description" not in body
        client.close()

    @respx.mock
    def test_update_no_fields_raises_bad_request(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/sub_123").mock(
            return_value=httpx.Response(
                400,
                json={"detail": "No fields to update.", "code": "bad_request"},
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError) as exc_info:
            client.webhooks.update("sub_123")

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {}
        assert exc_info.value.error_code == "bad_request"
        client.close()


class TestWebhookDelete:
    @respx.mock
    def test_delete_returns_none_on_204(self, api_key: str) -> None:
        route = respx.delete(f"{BASE}/v1/webhooks/sub_123").mock(
            return_value=httpx.Response(204),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.delete("sub_123")

        assert route.called
        assert result is None
        client.close()


class TestWebhookListDeliveries:
    @respx.mock
    def test_list_deliveries_default_pagination(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/sub_123/deliveries").mock(
            return_value=httpx.Response(200, json=DELIVERIES_PAGINATED_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.list_deliveries("sub_123")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert isinstance(result.data[0], WebhookDeliveryResponse)
        client.close()

    @respx.mock
    def test_list_deliveries_custom_pagination(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/sub_123/deliveries").mock(
            return_value=httpx.Response(200, json=DELIVERIES_PAGINATED_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.list_deliveries("sub_123", page=3, per_page=50)

        assert route.called
        request = route.calls.last.request
        assert "page=3" in str(request.url)
        assert "per_page=50" in str(request.url)
        client.close()


class TestWebhookRotateSecret:
    @respx.mock
    def test_rotate_secret(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks/sub_123/rotate-secret").mock(
            return_value=httpx.Response(200, json=ROTATE_SECRET_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.rotate_secret("sub_123")

        assert route.called
        request = route.calls.last.request
        # No body should be sent.
        assert request.content in (b"", b"null")
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookSecretRotateResponse)
        assert result.data.secret == "wh_secret_fedcba9876543210fedcba9876543210"
        client.close()


class TestWebhookSendTest:
    @respx.mock
    def test_send_test(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks/sub_123/test").mock(
            return_value=httpx.Response(200, json=TEST_DELIVERY_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.send_test("sub_123")

        assert route.called
        request = route.calls.last.request
        assert request.content in (b"", b"null")
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookTestResponse)
        assert result.data.test_delivery_id == "del_test_001"
        client.close()


class TestWebhookReplayDelivery:
    @respx.mock
    def test_replay_delivery_happy_path(self, api_key: str) -> None:
        route = respx.post(
            f"{BASE}/v1/webhooks/sub_123/deliveries/del_456/replay",
        ).mock(
            return_value=httpx.Response(200, json=REPLAY_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.replay_delivery("sub_123", "del_456")

        assert route.called
        request = route.calls.last.request
        assert request.content in (b"", b"null")
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookReplayResponse)
        assert result.data.replay_delivery_id == "del_replay_001"
        client.close()

    @respx.mock
    def test_replay_delivery_410_raises_generic_thesma_error(self, api_key: str) -> None:
        route = respx.post(
            f"{BASE}/v1/webhooks/sub_123/deliveries/del_old/replay",
        ).mock(
            return_value=httpx.Response(
                410,
                json={
                    "detail": "Delivery outside retention window.",
                    "code": "delivery_expired",
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(ThesmaError) as exc_info:
            client.webhooks.replay_delivery("sub_123", "del_old")

        assert route.called
        # 410 is intentionally not in _STATUS_MAP — generic ThesmaError, not a subclass.
        assert type(exc_info.value) is ThesmaError
        client.close()


class TestWebhookAsyncParity:
    @respx.mock
    @pytest.mark.asyncio
    async def test_async_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(200, json=WEBHOOK_LIST_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.webhooks.list()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_create(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=WEBHOOK_CREATE_RESPONSE_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.webhooks.create(
                url="https://example.com/hook",
                events=["filing.created"],
            )

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content.decode())
        assert body == {
            "url": "https://example.com/hook",
            "events": ["filing.created"],
        }
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookCreateResponse)

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_replay_delivery(self, api_key: str) -> None:
        route = respx.post(
            f"{BASE}/v1/webhooks/sub_x/deliveries/del_y/replay",
        ).mock(
            return_value=httpx.Response(200, json=REPLAY_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.webhooks.replay_delivery("sub_x", "del_y")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookReplayResponse)

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_delete_204(self, api_key: str) -> None:
        route = respx.delete(f"{BASE}/v1/webhooks/sub_x").mock(
            return_value=httpx.Response(204),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.webhooks.delete("sub_x")

        assert route.called
        assert result is None


def test_webhooks_reexported_from_resources() -> None:
    """Pin the `__all__` re-export so `from thesma.resources import Webhooks` works."""
    from thesma.resources import Webhooks as ReexportedWebhooks

    assert ReexportedWebhooks is Webhooks
