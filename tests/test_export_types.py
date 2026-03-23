"""Tests for ExportResult, ExportStream, AsyncExportStream, and helper functions."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from thesma._export import (
    AsyncExportStream,
    ExportResult,
    ExportStream,
    _build_export_params,
    _serialize_since,
)

# --- Helpers for mock responses ---


def _make_sync_response(lines: list[str]) -> MagicMock:
    """Create a mock httpx.Response with iter_lines() returning the given lines."""
    response = MagicMock()
    response.iter_lines.return_value = iter(lines)
    return response


async def _async_iter(items: list[str]):
    for item in items:
        yield item


def _make_async_response(lines: list[str]) -> MagicMock:
    """Create a mock httpx.Response with aiter_lines() returning an async iterator."""
    response = MagicMock()
    response.aiter_lines.return_value = _async_iter(lines)
    response.aclose = AsyncMock()
    return response


# --- ExportResult ---


class TestExportResult:
    def test_export_result_fields(self) -> None:
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl")
        assert result.path == "/tmp/out.jsonl"
        assert result.rows == 100
        assert result.complete is True
        assert result.format == "jsonl"


# --- _serialize_since ---


class TestSerializeSince:
    def test_serialize_since_string_passthrough(self) -> None:
        assert _serialize_since("2026-01-01") == "2026-01-01"

    def test_serialize_since_date(self) -> None:
        assert _serialize_since(date(2026, 1, 1)) == "2026-01-01"

    def test_serialize_since_datetime_aware(self) -> None:
        result = _serialize_since(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc))
        assert "2026-01-01" in result
        assert "12:00" in result
        # Should include timezone info
        assert "+" in result or "Z" in result or "UTC" in result

    def test_serialize_since_datetime_naive(self) -> None:
        result = _serialize_since(datetime(2026, 1, 1, 12, 0))
        assert "2026-01-01" in result
        assert "12:00" in result
        # Naive datetime — no timezone suffix
        assert result.endswith("00:00") or result.endswith("00")
        assert "+" not in result

    def test_serialize_since_none(self) -> None:
        assert _serialize_since(None) is None


# --- _build_export_params ---


class TestBuildExportParams:
    def test_build_params_strips_none(self) -> None:
        result = _build_export_params(fmt="jsonl", since=None, cik=None, ticker=None)
        assert result == {"format": "jsonl"}
        assert "since" not in result
        assert "cik" not in result
        assert "ticker" not in result

    def test_build_params_all_set(self) -> None:
        result = _build_export_params(fmt="csv", since="2026-01-01", cik="320193", ticker=None)
        assert result["format"] == "csv"
        assert result["since"] == "2026-01-01"
        assert result["cik"] == "320193"
        assert "ticker" not in result


# --- ExportStream (sync) ---


class TestExportStream:
    def test_jsonl_iteration(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
            '{"__export_complete":true}',
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert rows[0]["cik"] == "320193"
        assert rows[1]["cik"] == "1067983"
        assert stream.complete is True

    def test_jsonl_sentinel_not_yielded(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"__export_complete":true}',
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        for row in rows:
            assert "__export_complete" not in row

    def test_jsonl_missing_sentinel(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        list(stream)

        assert stream.complete is False

    def test_csv_iteration(self) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
            "1067983,BRK-B,Berkshire Hathaway",
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "csv")
        rows = list(stream)

        assert len(rows) == 2
        assert rows[0]["cik"] == "320193"
        assert rows[0]["ticker"] == "AAPL"
        assert rows[0]["name"] == "Apple Inc"
        assert rows[1]["ticker"] == "BRK-B"

    def test_csv_complete_always_true(self) -> None:
        lines = [
            "cik,ticker",
            "320193,AAPL",
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "csv")
        list(stream)

        assert stream.complete is True

    def test_empty_jsonl(self) -> None:
        lines = ['{"__export_complete":true}']
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert rows == []
        assert stream.complete is True

    def test_empty_csv(self) -> None:
        lines = ["cik,ticker,name"]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "csv")
        rows = list(stream)

        assert rows == []

    def test_context_manager_closes_response(self) -> None:
        response = _make_sync_response(['{"__export_complete":true}'])
        with ExportStream(response, "jsonl") as _stream:
            pass
        response.close.assert_called()


# --- AsyncExportStream ---


class TestAsyncExportStream:
    async def test_async_jsonl_iteration(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
            '{"__export_complete":true}',
        ]
        response = _make_async_response(lines)
        stream = AsyncExportStream(response, "jsonl")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert rows[0]["cik"] == "320193"
        assert rows[1]["cik"] == "1067983"
        assert stream.complete is True

    async def test_async_sentinel_detection(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
        ]
        response = _make_async_response(lines)
        stream = AsyncExportStream(response, "jsonl")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 1
        # No sentinel → incomplete
        assert stream.complete is False

    async def test_async_csv_iteration(self) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
            "1067983,BRK-B,Berkshire Hathaway",
        ]
        response = _make_async_response(lines)
        stream = AsyncExportStream(response, "csv")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert rows[0]["cik"] == "320193"
        assert rows[0]["name"] == "Apple Inc"

    async def test_async_context_manager(self) -> None:
        response = _make_async_response(['{"__export_complete":true}'])
        async with AsyncExportStream(response, "jsonl") as _stream:
            pass
        response.aclose.assert_called()
