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
    def test_500_retried_with_exponential_backoff(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(500, json={"detail": "Internal Server Error"}),
                httpx.Response(500, json={"detail": "Internal Server Error"}),
                httpx.Response(200, json={"name": "OK", "value": 1}),
            ],
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=2)
            result = client.request("GET", "/v1/test", response_model=_TestModel)

            assert route.call_count == 3
            assert mock_sleep.call_count == 2
            # First sleep: 2^0 + jitter = 1.0-1.5
            assert 1.0 <= mock_sleep.call_args_list[0][0][0] <= 1.5
            # Second sleep: 2^1 + jitter = 2.0-2.5
            assert 2.0 <= mock_sleep.call_args_list[1][0][0] <= 2.5

        assert result is not None
        assert result.name == "OK"
        client.close()

    @respx.mock
    def test_connection_error_retried(self, api_key: str) -> None:
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("connection failed")
            return httpx.Response(200, json={"name": "OK", "value": 1})

        respx.get(f"{BASE}/v1/test").mock(side_effect=side_effect)

        with patch("time.sleep"):
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            result = client.request("GET", "/v1/test", response_model=_TestModel)

        assert call_count == 2
        assert result is not None
        assert result.name == "OK"
        client.close()

    @respx.mock
    def test_timeout_error_retried(self, api_key: str) -> None:
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectTimeout("timed out")
            return httpx.Response(200, json={"name": "OK", "value": 1})

        respx.get(f"{BASE}/v1/test").mock(side_effect=side_effect)

        with patch("time.sleep"):
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            result = client.request("GET", "/v1/test", response_model=_TestModel)

        assert call_count == 2
        assert result is not None
        assert result.name == "OK"
        client.close()

    @respx.mock
    def test_5xx_max_retries_exhausted_raises_server_error(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            return_value=httpx.Response(500, json={"detail": "Internal Server Error"}),
        )

        with patch("time.sleep"):
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=2)

            with pytest.raises(ServerError):
                client.request("GET", "/v1/test", response_model=_TestModel)

            # 1 initial + 2 retries = 3 total
            assert route.call_count == 3

        client.close()

    @respx.mock
    def test_429_still_uses_retry_after_header(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(429, json={"detail": "Rate limited"}, headers={"Retry-After": "3"}),
                httpx.Response(200, json={"name": "OK", "value": 1}),
            ],
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=1)
            client.request("GET", "/v1/test", response_model=_TestModel)

            mock_sleep.assert_called_once()
            sleep_duration = mock_sleep.call_args[0][0]
            # Retry-After=3 + jitter [0, 0.5] → 3.0-3.5 (not exponential)
            assert 3.0 <= sleep_duration <= 3.5

        client.close()

    @respx.mock
    def test_backoff_capped_at_30s(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/test").mock(
            return_value=httpx.Response(500, json={"detail": "Internal Server Error"}),
        )

        with patch("time.sleep") as mock_sleep:
            client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=6)

            with pytest.raises(ServerError):
                client.request("GET", "/v1/test", response_model=_TestModel)

            # 6 sleeps (attempts 0-5), attempt 6 raises
            assert mock_sleep.call_count == 6
            for call in mock_sleep.call_args_list:
                assert call[0][0] <= 30.5

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

    @respx.mock
    async def test_async_500_retried(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/test").mock(
            side_effect=[
                httpx.Response(500, json={"detail": "Internal Server Error"}),
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
            sleep_duration = mock_async_sleep.call_args[0][0]
            # 2^0 + jitter = 1.0-1.5
            assert 1.0 <= sleep_duration <= 1.5
            assert result is not None
            assert result.name == "OK"

        await client.close()
