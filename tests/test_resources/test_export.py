"""Tests for the Export / AsyncExport resource and _stream_get / _async_stream_get client methods."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from thesma._export import ExportResult, ExportStream
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import ForbiddenError, RateLimitError

BASE = "https://api.thesma.dev"

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
                json={"detail": "Export in progress", "code": "export_in_progress"},
                headers={"Retry-After": "60"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError) as exc_info:
            client._stream_get("/v1/us/sec/export/companies")

        assert exc_info.value.retry_after == 60.0
        client.close()

    @respx.mock
    def test_stream_get_no_retry(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress"},
                    headers={"Retry-After": "60"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key, auto_retry=True, max_retries=3)

        with pytest.raises(RateLimitError):
            client._stream_get("/v1/us/sec/export/companies")

        # _stream_get bypasses retry logic — only 1 call
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
