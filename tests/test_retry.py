"""Tests for rate-limit retry logic."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx
from pydantic import BaseModel

from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import RateLimitError, ServerError

BASE = "https://api.thesma.dev"


class _TestModel(BaseModel):
    name: str
    value: int


class TestRetryDisabled:
    @respx.mock
    def test_auto_retry_false_raises_rate_limit_immediately(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/test").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limited"},
                headers={"Retry-After": "2"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError):
            client.request("GET", "/v1/test", response_model=_TestModel)

        client.close()


class TestSyncRetry:
    @respx.mock
    def test_429_retried_after_retry_after_with_jitter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(429, json={"detail": "Rate limited"}, headers={"Retry-After": "2"}),
                httpx.Response(200, json={"name": "OK", "value": 1}),
            ],
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            result = client.request("GET", "/v1/test", response_model=_TestModel)

            assert route.call_count == 2
            mock_sleep.assert_called_once()
            sleep_duration = mock_sleep.call_args[0][0]
            assert 2.0 <= sleep_duration <= 2.5  # Retry-After + jitter [0, 0.5]

        assert result is not None
        assert result.name == "OK"
        client.close()

    @respx.mock
    def test_max_retries_exhausted_raises_rate_limit(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/test").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limited"},
                headers={"Retry-After": "1"},
            ),
        )

        with patch("time.sleep"):
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=3)

            with pytest.raises(RateLimitError):
                client.request("GET", "/v1/test", response_model=_TestModel)

        client.close()

    @respx.mock
    def test_retry_succeeds_on_second_attempt(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(429, json={"detail": "Rate limited"}, headers={"Retry-After": "1"}),
                httpx.Response(200, json={"name": "Success", "value": 42}),
            ],
        )

        with patch("time.sleep"):
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            result = client.request("GET", "/v1/test", response_model=_TestModel)

        assert route.call_count == 2
        assert result is not None
        assert result.name == "Success"
        assert result.value == 42
        client.close()

    @respx.mock
    def test_429_without_retry_after_uses_default_backoff(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(429, json={"detail": "Rate limited"}),
                httpx.Response(200, json={"name": "OK", "value": 1}),
            ],
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            client.request("GET", "/v1/test", response_model=_TestModel)

            mock_sleep.assert_called_once()
            sleep_duration = mock_sleep.call_args[0][0]
            # Default backoff is 1 second + jitter [0, 0.5]
            assert 1.0 <= sleep_duration <= 1.5

        client.close()

    @respx.mock
    def test_retry_not_triggered_on_500(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            return_value=httpx.Response(500, json={"detail": "Internal Server Error"}),
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True)

            with pytest.raises(ServerError):
                client.request("GET", "/v1/test", response_model=_TestModel)

            # 500 should NOT be retried
            assert route.call_count == 1
            mock_sleep.assert_not_called()

        client.close()


class TestAsyncRetry:
    @respx.mock
    async def test_async_retry_uses_asyncio_sleep(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(429, json={"detail": "Rate limited"}, headers={"Retry-After": "1"}),
                httpx.Response(200, json={"name": "OK", "value": 1}),
            ],
        )

        with (
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_async_sleep,
            patch("time.sleep") as mock_sync_sleep,
        ):
            client = AsyncThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            result = await client.request("GET", "/v1/test", response_model=_TestModel)

            assert route.call_count == 2
            mock_async_sleep.assert_called_once()
            mock_sync_sleep.assert_not_called()
            assert result is not None
            assert result.name == "OK"

        await client.close()
