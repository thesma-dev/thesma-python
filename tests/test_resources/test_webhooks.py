"""Tests for the Webhooks resource."""

from __future__ import annotations

import json

import httpx
import respx

from thesma._generated.models import WebhookCreateResponse, WebhookDeliveryResponse, WebhookResponse
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

WEBHOOK_RESPONSE_JSON = {
    "id": "wh_abc123",
    "url": "https://example.com/webhook",
    "events": ["filing.processed"],
    "is_active": True,
    "consecutive_failure_count": 0,
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-01T00:00:00Z",
}

LIST_WEBHOOKS_JSON = {
    "data": [WEBHOOK_RESPONSE_JSON],
}

GET_WEBHOOK_JSON = {
    "data": WEBHOOK_RESPONSE_JSON,
}

CREATE_WEBHOOK_JSON = {
    "data": {
        **WEBHOOK_RESPONSE_JSON,
        "secret": "whsec_test123",
    },
}

DELIVERIES_JSON = {
    "data": [
        {
            "id": "del_abc123",
            "subscription_id": "wh_abc123",
            "event_type": "filing.processed",
            "payload": {"filing_id": "0000320193-24-000081"},
            "status": "delivered",
            "http_status": 200,
            "attempt_count": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "completed_at": "2024-01-01T00:00:01Z",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestWebhooksList:
    @respx.mock
    def test_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(200, json=LIST_WEBHOOKS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.list()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 1
        client.close()


class TestWebhooksCreate:
    @respx.mock
    def test_create_with_json_body(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=CREATE_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.create(
            url="https://example.com/webhook",
            events=["filing.processed"],
        )

        assert route.called
        request = route.calls.last.request
        body = json.loads(request.content)
        assert body["url"] == "https://example.com/webhook"
        assert body["events"] == ["filing.processed"]
        assert "secret" not in body
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookCreateResponse)
        assert result.data.secret == "whsec_test123"
        client.close()

    @respx.mock
    def test_create_with_secret(self, api_key: str) -> None:
        route = respx.post(f"{BASE}/v1/webhooks").mock(
            return_value=httpx.Response(201, json=CREATE_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.create(
            url="https://example.com/webhook",
            events=["filing.processed"],
            secret="my_secret",
        )

        body = json.loads(route.calls.last.request.content)
        assert body["secret"] == "my_secret"
        client.close()


class TestWebhooksGet:
    @respx.mock
    def test_get(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/wh_abc123").mock(
            return_value=httpx.Response(200, json=GET_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.get("wh_abc123")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, WebhookResponse)
        assert result.data.id == "wh_abc123"
        client.close()


class TestWebhooksUpdate:
    @respx.mock
    def test_update_partial(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/wh_abc123").mock(
            return_value=httpx.Response(200, json=GET_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.update("wh_abc123", url="https://new.example.com/webhook")

        body = json.loads(route.calls.last.request.content)
        assert body == {"url": "https://new.example.com/webhook"}
        assert "events" not in body
        assert "is_active" not in body
        client.close()

    @respx.mock
    def test_update_active_flag(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/wh_abc123").mock(
            return_value=httpx.Response(200, json=GET_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.update("wh_abc123", active=False)

        body = json.loads(route.calls.last.request.content)
        assert body == {"is_active": False}
        client.close()

    @respx.mock
    def test_update_all_fields(self, api_key: str) -> None:
        route = respx.patch(f"{BASE}/v1/webhooks/wh_abc123").mock(
            return_value=httpx.Response(200, json=GET_WEBHOOK_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.webhooks.update(
            "wh_abc123",
            url="https://new.example.com/webhook",
            events=["filing.corrected"],
            active=True,
        )

        body = json.loads(route.calls.last.request.content)
        assert body["url"] == "https://new.example.com/webhook"
        assert body["events"] == ["filing.corrected"]
        assert body["is_active"] is True
        client.close()


class TestWebhooksDelete:
    @respx.mock
    def test_delete_returns_none(self, api_key: str) -> None:
        route = respx.delete(f"{BASE}/v1/webhooks/wh_abc123").mock(
            return_value=httpx.Response(204),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.delete("wh_abc123")

        assert route.called
        assert result is None
        client.close()


class TestWebhooksDeliveries:
    @respx.mock
    def test_deliveries(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/webhooks/wh_abc123/deliveries").mock(
            return_value=httpx.Response(200, json=DELIVERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.webhooks.deliveries("wh_abc123")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        assert isinstance(result.data[0], WebhookDeliveryResponse)
        client.close()
