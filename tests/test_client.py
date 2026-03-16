"""Tests for ThesmaClient and AsyncThesmaClient."""

from __future__ import annotations

import httpx
import pytest
import respx
from pydantic import BaseModel

from thesma.client import AsyncThesmaClient, ThesmaClient

# --- Construction ---


class TestClientConstruction:
    def test_sync_client_stores_config(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)
        assert client.api_key == api_key
        assert client.base_url == "https://api.thesma.dev"
        assert client.timeout == 30

    def test_sync_client_empty_key_raises(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            ThesmaClient(api_key="")

    def test_sync_client_custom_base_url(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key, base_url="https://staging.thesma.dev")
        assert client.base_url == "https://staging.thesma.dev"

    def test_sync_client_custom_timeout(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key, timeout=60)
        assert client.timeout == 60

    def test_async_client_stores_config(self, api_key: str) -> None:
        client = AsyncThesmaClient(api_key=api_key)
        assert client.api_key == api_key
        assert client.base_url == "https://api.thesma.dev"
        assert client.timeout == 30

    def test_async_client_empty_key_raises(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            AsyncThesmaClient(api_key="")


# --- Context managers ---


class TestContextManager:
    def test_sync_context_manager(self, api_key: str) -> None:
        with ThesmaClient(api_key=api_key) as client:
            assert client._client is not None
        assert client._client is None

    async def test_async_context_manager(self, api_key: str) -> None:
        async with AsyncThesmaClient(api_key=api_key) as client:
            assert client._client is not None
        assert client._client is None


# --- Repr ---


class TestRepr:
    def test_sync_repr_redacts_api_key(self) -> None:
        client = ThesmaClient(api_key="th_live_abc123")
        r = repr(client)
        assert "th_live_" in r
        assert "abc123" not in r
        assert "***" in r

    def test_async_repr_redacts_api_key(self) -> None:
        client = AsyncThesmaClient(api_key="th_live_abc123")
        r = repr(client)
        assert "th_live_" in r
        assert "abc123" not in r
        assert "***" in r


# --- HTTP requests ---


class _TestModel(BaseModel):
    name: str
    value: int


class TestHTTPRequests:
    @respx.mock
    def test_get_sends_correct_url_and_auth_header(self, api_key: str) -> None:
        route = respx.get("https://api.thesma.dev/v1/companies").mock(
            return_value=httpx.Response(200, json={"name": "Acme", "value": 42})
        )
        client = ThesmaClient(api_key=api_key)
        result = client.request("GET", "/v1/companies", response_model=_TestModel)
        assert route.called
        request = route.calls.last.request
        assert request.headers["X-API-Key"] == api_key
        assert result is not None
        assert result.name == "Acme"
        assert result.value == 42
        client.close()

    @respx.mock
    def test_none_params_are_stripped(self, api_key: str) -> None:
        route = respx.get("https://api.thesma.dev/v1/companies").mock(
            return_value=httpx.Response(200, json={"name": "Acme", "value": 1})
        )
        client = ThesmaClient(api_key=api_key)
        client.request(
            "GET",
            "/v1/companies",
            params={"cik": "0001234", "name": None},
            response_model=_TestModel,
        )
        request = route.calls.last.request
        assert "cik=0001234" in str(request.url)
        assert "name" not in str(request.url)
        client.close()

    @respx.mock
    def test_post_with_json_body(self, api_key: str) -> None:
        route = respx.post("https://api.thesma.dev/v1/items").mock(
            return_value=httpx.Response(200, json={"name": "New", "value": 99})
        )
        client = ThesmaClient(api_key=api_key)
        result = client.request(
            "POST",
            "/v1/items",
            json={"name": "New"},
            response_model=_TestModel,
        )
        request = route.calls.last.request
        assert request.headers["content-type"] == "application/json"
        assert result is not None
        assert result.name == "New"
        client.close()

    @respx.mock
    def test_successful_response_parsed_into_model(self, api_key: str) -> None:
        respx.get("https://api.thesma.dev/v1/test").mock(
            return_value=httpx.Response(200, json={"name": "Test", "value": 7})
        )
        client = ThesmaClient(api_key=api_key)
        result = client.request("GET", "/v1/test", response_model=_TestModel)
        assert isinstance(result, _TestModel)
        assert result.name == "Test"
        assert result.value == 7
        client.close()
