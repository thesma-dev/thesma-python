"""Tests for the Export / AsyncExport resource and _stream_get / _async_stream_get client methods."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest
import respx

from thesma._export import ExportResult, ExportStream
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import ExportInProgressError, ForbiddenError, RateLimitError

BASE = "https://api.thesma.dev"


class _AsyncSleepMock(AsyncMock):
    """AsyncMock suitable for ``new_callable`` when patching ``asyncio.sleep``."""

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        return super().__call__(*args, **kwargs)


JSONL_COMPANIES = '{"cik":"320193","ticker":"AAPL","name":"Apple Inc"}\n{"__export_complete":true}\n'
CSV_COMPANIES = "cik,ticker,name\n320193,AAPL,Apple Inc\n"
JSONL_TWO_ROWS = '{"cik":"320193","ticker":"AAPL"}\n{"cik":"1067983","ticker":"BRK-B"}\n{"__export_complete":true}\n'


# --- _stream_get (sync) ---


class TestExportStreamGet:
    @respx.mock
    def test_stream_get_returns_streaming_response(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        response = client._stream_get("/v1/us/sec/export/companies")

        assert route.called
        assert isinstance(response, httpx.Response)
        response.close()
        client.close()

    @respx.mock
    def test_stream_get_403_raises_forbidden(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                403,
                json={"detail": "Plan insufficient", "code": "plan_insufficient"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(ForbiddenError):
            client._stream_get("/v1/us/sec/export/companies")

        client.close()

    @respx.mock
    def test_stream_get_429_raises_rate_limit(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "60"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError) as exc_info:
            client._stream_get("/v1/us/sec/export/companies")

        assert exc_info.value.retry_after == 60.0
        assert type(exc_info.value) is RateLimitError
        client.close()

    @respx.mock
    def test_stream_get_no_retry(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Rate limit exceeded"},
                    headers={"Retry-After": "60"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=3)

        with pytest.raises(RateLimitError):
            client._stream_get("/v1/us/sec/export/companies")

        # _stream_get bypasses retry logic for plain 429 — only 1 call
        assert route.call_count == 1
        client.close()

    @respx.mock
    def test_stream_get_timeout_override(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key, timeout=5)
        response = client._stream_get("/v1/us/sec/export/companies")

        # Just verify the call succeeds — the 5-min read timeout is internal to httpx
        assert route.called
        response.close()
        client.close()


# --- Export.companies (sync) ---


class TestExportCompanies:
    @respx.mock
    def test_jsonl_returns_export_stream(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies()

        assert isinstance(stream, ExportStream)
        rows = list(stream)
        assert len(rows) == 1
        assert rows[0]["cik"] == "320193"
        assert stream.complete is True
        stream.close()
        client.close()

    @respx.mock
    def test_csv_returns_export_stream(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=CSV_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies(format="csv")

        rows = list(stream)
        assert len(rows) == 1
        assert rows[0]["cik"] == "320193"
        assert rows[0]["ticker"] == "AAPL"
        assert rows[0]["name"] == "Apple Inc"
        stream.close()
        client.close()

    @respx.mock
    def test_file_output_returns_export_result(self, api_key: str, tmp_path: Path) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "out.jsonl")
        result = client.export.companies(output=out_path)

        assert isinstance(result, ExportResult)
        assert result.rows == 1
        assert result.complete is True
        assert Path(result.path).exists()
        # File should contain data but not sentinel
        content = Path(result.path).read_text()
        assert "320193" in content
        assert "__export_complete" not in content
        client.close()

    @respx.mock
    def test_cik_filter_sent_as_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies(cik="320193")

        request = route.calls.last.request
        assert "cik=320193" in str(request.url)
        stream.close()
        client.close()

    @respx.mock
    def test_ticker_filter_sent_as_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies(ticker="AAPL")

        request = route.calls.last.request
        assert "ticker=AAPL" in str(request.url)
        stream.close()
        client.close()

    def test_cik_and_ticker_raises_value_error(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(ValueError):
            client.export.companies(cik="320193", ticker="AAPL")

        client.close()

    @respx.mock
    def test_since_datetime_serialized(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies(since=datetime(2026, 1, 1, tzinfo=timezone.utc))

        request = route.calls.last.request
        assert "since=2026-01-01T00%3A00%3A00" in str(request.url) or "since=2026-01-01T00:00:00" in str(request.url)
        stream.close()
        client.close()

    @respx.mock
    def test_since_date_serialized(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.companies(since=date(2026, 1, 1))

        request = route.calls.last.request
        assert "since=2026-01-01" in str(request.url)
        stream.close()
        client.close()

    def test_output_bad_parent_raises_file_not_found(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(FileNotFoundError):
            client.export.companies(output="/nonexistent/dir/out.jsonl")

        client.close()

    @respx.mock
    def test_output_file_no_sentinel_in_content(self, api_key: str, tmp_path: Path) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "out.jsonl")
        client.export.companies(output=out_path)

        content = Path(out_path).read_text()
        assert "__export_complete" not in content
        assert "320193" in content
        client.close()


# --- Export.holdings (sync) ---


class TestExportHoldings:
    @respx.mock
    def test_holdings_sends_correct_path(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/holdings").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.holdings()

        assert route.called
        stream.close()
        client.close()

    @respx.mock
    def test_holdings_cik_filter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/holdings").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = client.export.holdings(cik="1067983")

        request = route.calls.last.request
        assert "cik=1067983" in str(request.url)
        stream.close()
        client.close()


# --- All endpoint paths (parametrized) ---


class TestExportEndpointPaths:
    @pytest.mark.parametrize(
        ("method_name", "expected_path"),
        [
            ("companies", "/v1/us/sec/export/companies"),
            ("financials", "/v1/us/sec/export/financials"),
            ("insider_trades", "/v1/us/sec/export/insider-trades"),
            ("events", "/v1/us/sec/export/events"),
            ("ratios", "/v1/us/sec/export/ratios"),
            ("holdings", "/v1/us/sec/export/holdings"),
            ("compensation", "/v1/us/sec/export/compensation"),
            ("beneficial_ownership", "/v1/us/sec/export/beneficial-ownership"),
        ],
    )
    @respx.mock
    def test_all_endpoints_send_correct_path(self, api_key: str, method_name: str, expected_path: str) -> None:
        route = respx.get(f"{BASE}{expected_path}").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key)
        stream = getattr(client.export, method_name)()

        assert route.called
        stream.close()
        client.close()


# --- Async export ---


class TestAsyncExport:
    @respx.mock
    async def test_async_jsonl_stream(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = AsyncThesmaClient(api_key=api_key)
        stream = await client.export.companies()

        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 1
        assert rows[0]["cik"] == "320193"
        assert stream.complete is True
        await stream.close()
        await client.close()

    @respx.mock
    async def test_async_file_output(self, api_key: str, tmp_path: Path) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = AsyncThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "out.jsonl")
        result = await client.export.companies(output=out_path)

        assert isinstance(result, ExportResult)
        assert Path(out_path).exists()
        await client.close()


# --- export_in_progress retry — sync ---


class TestExportInProgressRetrySync:
    @respx.mock
    def test_stream_get_retries_export_in_progress(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                ),
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key)

        with patch("thesma.client.time.sleep"):
            response = client._stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 3
        assert response.status_code == 200
        response.close()
        client.close()

    @respx.mock
    def test_stream_get_export_in_progress_exhausted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                )
                for _ in range(7)
            ],
        )
        client = ThesmaClient(api_key=api_key)

        with patch("thesma.client.time.sleep"), pytest.raises(ExportInProgressError):
            client._stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 7
        client.close()

    @respx.mock
    def test_stream_get_respects_retry_after(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "10"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key)

        with patch("thesma.client.time.sleep") as mock_sleep:
            response = client._stream_get("/v1/us/sec/export/companies")

        assert mock_sleep.call_count == 1
        slept = mock_sleep.call_args[0][0]
        assert slept >= 10.0
        assert slept <= 10.5
        response.close()
        client.close()

    @respx.mock
    def test_stream_get_export_in_progress_missing_retry_after(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key)

        with patch("thesma.client.time.sleep") as mock_sleep:
            response = client._stream_get("/v1/us/sec/export/companies")

        assert mock_sleep.call_count == 1
        slept = mock_sleep.call_args[0][0]
        assert slept >= 30.0
        assert slept <= 30.5
        response.close()
        client.close()

    @respx.mock
    def test_stream_get_regular_429_no_retry(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "60"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError) as exc_info:
            client._stream_get("/v1/us/sec/export/companies")

        assert type(exc_info.value) is RateLimitError
        assert route.call_count == 1
        client.close()


# --- export_in_progress retry — async ---


class TestExportInProgressRetryAsync:
    @respx.mock
    async def test_async_stream_get_retries_export_in_progress(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                ),
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = AsyncThesmaClient(api_key=api_key)

        with patch("thesma.client.asyncio.sleep", new_callable=_AsyncSleepMock):
            response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 3
        assert response.status_code == 200
        await response.aclose()
        await client.close()

    @respx.mock
    async def test_async_stream_get_export_in_progress_exhausted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "1"},
                )
                for _ in range(7)
            ],
        )
        client = AsyncThesmaClient(api_key=api_key)

        with (
            patch("thesma.client.asyncio.sleep", new_callable=_AsyncSleepMock),
            pytest.raises(ExportInProgressError),
        ):
            await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 7
        await client.close()

    @respx.mock
    async def test_async_stream_get_respects_retry_after(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "10"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = AsyncThesmaClient(api_key=api_key)

        with patch("thesma.client.asyncio.sleep", new_callable=_AsyncSleepMock) as mock_sleep:
            response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert mock_sleep.call_count == 1
        slept = mock_sleep.call_args[0][0]
        assert slept >= 10.0
        assert slept <= 10.5
        await response.aclose()
        await client.close()

    @respx.mock
    async def test_async_stream_get_export_in_progress_missing_retry_after(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = AsyncThesmaClient(api_key=api_key)

        with patch("thesma.client.asyncio.sleep", new_callable=_AsyncSleepMock) as mock_sleep:
            response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert mock_sleep.call_count == 1
        slept = mock_sleep.call_args[0][0]
        assert slept >= 30.0
        assert slept <= 30.5
        await response.aclose()
        await client.close()

    @respx.mock
    async def test_async_stream_get_regular_429_no_retry(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "60"},
            ),
        )
        client = AsyncThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError) as exc_info:
            await client._async_stream_get("/v1/us/sec/export/companies")

        assert type(exc_info.value) is RateLimitError
        assert route.call_count == 1
        await client.close()


# --- Stream timeout ---


class TestStreamTimeout:
    def test_stream_timeout_default_300(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)
        assert client.stream_timeout == 300
        client.close()

    @respx.mock
    def test_stream_timeout_configurable(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key, stream_timeout=600)
        assert client.stream_timeout == 600

        response = client._stream_get("/v1/us/sec/export/companies")

        assert route.called
        request = route.calls.last.request
        timeout_dict = request.extensions["timeout"]
        assert timeout_dict["read"] == 600.0
        response.close()
        client.close()

    @respx.mock
    async def test_async_stream_timeout_configurable(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = AsyncThesmaClient(api_key=api_key, stream_timeout=600)
        assert client.stream_timeout == 600

        response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.called
        request = route.calls.last.request
        timeout_dict = request.extensions["timeout"]
        assert timeout_dict["read"] == 600.0
        await response.aclose()
        await client.close()


# --- Helpers for resource-layer resume tests ---


def _iter_then_raise(lines: list[str], exc: Exception) -> Iterator[str]:
    """Yield lines then raise — simulates a mid-stream connection drop."""
    yield from lines
    raise exc


async def _async_iter_then_raise(lines: list[str], exc: Exception) -> AsyncIterator[str]:
    """Async variant: yield lines then raise."""
    for line in lines:
        yield line
    raise exc


def _make_sync_error_response(lines: list[str], exc: Exception) -> MagicMock:
    """Create a mock response whose iter_lines() yields lines then raises."""
    response = MagicMock()
    response.iter_lines.return_value = _iter_then_raise(lines, exc)
    return response


def _make_sync_response(lines: list[str]) -> MagicMock:
    """Create a mock httpx.Response with iter_lines() returning the given lines."""
    response = MagicMock()
    response.iter_lines.return_value = iter(lines)
    return response


async def _async_iter(items: list[str]) -> AsyncIterator[str]:
    for item in items:
        yield item


def _make_async_response(lines: list[str]) -> MagicMock:
    """Create a mock httpx.Response with aiter_lines() returning an async iterator."""
    response = MagicMock()
    response.aiter_lines.return_value = _async_iter(lines)
    response.aclose = AsyncMock()
    return response


def _make_async_error_response(lines: list[str], exc: Exception) -> MagicMock:
    """Create a mock response whose aiter_lines() yields lines then raises."""
    response = MagicMock()
    response.aiter_lines.return_value = _async_iter_then_raise(lines, exc)
    response.aclose = AsyncMock()
    return response


# --- Resource-layer resume tests (sync) ---


class TestExportCompaniesResume:
    def test_file_output_with_resume(self, api_key: str, tmp_path: Path) -> None:
        """Patch _stream_get to return two mock responses: first yields rows then raises,
        second yields rows + sentinel. Assert complete, retries, and file content."""
        exc = httpx.ReadError("connection reset")
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        call_count = 0

        def mock_stream_get(path: str, params: Any = None, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp1
            return resp2

        with patch.object(client, "_stream_get", side_effect=mock_stream_get):
            out = str(tmp_path / "resume.jsonl")
            result = client.export.companies(output=out)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 1
        assert result.rows == 3
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 3
        client.close()

    def test_max_resume_retries_param_passed(self, api_key: str, tmp_path: Path) -> None:
        """Call with max_resume_retries=5, fail 5 times then succeed."""
        exc = httpx.ReadError("connection reset")
        rows_fail = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        rows_ok = [
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
            '{"__export_complete":true}',
        ]

        client = ThesmaClient(api_key=api_key)
        call_count = 0

        def mock_stream_get(path: str, params: Any = None, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count <= 6:  # initial + 5 retries = 6 total calls that fail
                return _make_sync_error_response(rows_fail, exc)
            return _make_sync_response(rows_ok)

        # We need initial + retries. 5 retries means calls 1-6 fail, call 7 succeeds
        # But we said fail 5 times then succeed. So initial fails, then 5 retries,
        # of which the 5th succeeds.
        call_count2 = 0

        def mock_stream_get2(path: str, params: Any = None, **kwargs: Any) -> MagicMock:
            nonlocal call_count2
            call_count2 += 1
            if call_count2 <= 5:  # initial + 4 retries fail
                return _make_sync_error_response(rows_fail, exc)
            return _make_sync_response(rows_ok)  # 5th retry succeeds

        with patch.object(client, "_stream_get", side_effect=mock_stream_get2):
            out = str(tmp_path / "retries5.jsonl")
            result = client.export.companies(output=out, max_resume_retries=5)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 5
        client.close()

    def test_resume_since_param_sent_correctly(self, api_key: str, tmp_path: Path) -> None:
        """Verify that the second _stream_get call has since=<max_updated_at> in params."""
        exc = httpx.ReadError("connection reset")
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        rows2 = ['{"__export_complete":true}']
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        mock_fn = Mock(side_effect=[resp1, resp2])

        with patch.object(client, "_stream_get", mock_fn):
            out = str(tmp_path / "since.jsonl")
            client.export.companies(output=out)

        # Second call should have since=max updated_at
        assert mock_fn.call_count == 2
        second_call_params = mock_fn.call_args_list[1][1].get("params") or mock_fn.call_args_list[1][0][1]
        assert second_call_params["since"] == "2026-01-15T12:00:00+00:00"
        client.close()

    def test_resume_preserves_other_params(self, api_key: str, tmp_path: Path) -> None:
        """Call with cik='320193' and format='jsonl'. Trigger a resume.
        Verify retry call params preserve cik and format alongside new since."""
        exc = httpx.ReadError("connection reset")
        rows1 = ['{"cik":"320193","ticker":"AAPL","updated_at":"2026-01-15T10:00:00+00:00"}']
        rows2 = ['{"__export_complete":true}']
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        mock_fn = Mock(side_effect=[resp1, resp2])

        with patch.object(client, "_stream_get", mock_fn):
            out = str(tmp_path / "preserve.jsonl")
            client.export.companies(output=out, cik="320193")

        assert mock_fn.call_count == 2
        second_call_params = mock_fn.call_args_list[1][1].get("params") or mock_fn.call_args_list[1][0][1]
        assert second_call_params["cik"] == "320193"
        assert second_call_params["format"] == "jsonl"
        assert second_call_params["since"] == "2026-01-15T10:00:00+00:00"
        client.close()


# --- Resource-layer resume tests (async) ---


class TestAsyncExportResume:
    async def test_async_file_output_with_resume(self, api_key: str, tmp_path: Path) -> None:
        """Async equivalent: patch _async_stream_get, verify resume works."""
        exc = httpx.ReadError("connection reset")
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        resp1 = _make_async_error_response(rows1, exc)
        resp2 = _make_async_response(rows2)

        client = AsyncThesmaClient(api_key=api_key)
        call_count = 0

        async def mock_async_stream_get(path: str, params: Any = None, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp1
            return resp2

        with patch.object(client, "_async_stream_get", side_effect=mock_async_stream_get):
            out = str(tmp_path / "async_resume.jsonl")
            result = await client.export.companies(output=out)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 1
        assert result.rows == 3
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 3
        await client.close()

    async def test_async_max_resume_retries_param_passed(self, api_key: str, tmp_path: Path) -> None:
        """Async equivalent: fail 5 times then succeed with max_resume_retries=5."""
        exc = httpx.ReadError("connection reset")
        rows_fail = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        rows_ok = [
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
            '{"__export_complete":true}',
        ]

        client = AsyncThesmaClient(api_key=api_key)
        call_count = 0

        async def mock_async_stream_get(path: str, params: Any = None, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                return _make_async_error_response(rows_fail, exc)
            return _make_async_response(rows_ok)

        with patch.object(client, "_async_stream_get", side_effect=mock_async_stream_get):
            out = str(tmp_path / "async_retries5.jsonl")
            result = await client.export.companies(output=out, max_resume_retries=5)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 5
        await client.close()
