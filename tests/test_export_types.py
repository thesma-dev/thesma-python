"""Tests for ExportResult, ExportStream, AsyncExportStream, and helper functions."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import httpx

from thesma._export import (
    AsyncExportStream,
    ExportResult,
    ExportStream,
    _build_export_params,
    _serialize_since,
    _write_to_file_async,
    _write_to_file_sync,
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


def _iter_then_raise(lines: list[str], exc: Exception) -> Iterator[str]:
    """Yield lines then raise -- simulates a mid-stream connection drop."""
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


def _make_async_error_response(lines: list[str], exc: Exception) -> MagicMock:
    """Create a mock response whose aiter_lines() yields lines then raises."""
    response = MagicMock()
    response.aiter_lines.return_value = _async_iter_then_raise(lines, exc)
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
        assert result.retries == 0

    def test_export_result_retries_field(self) -> None:
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl", retries=2)
        assert result.retries == 2

    def test_export_result_retries_default_zero(self) -> None:
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl")
        assert result.retries == 0


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

    def test_jsonl_read_error_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    def test_jsonl_read_timeout_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.ReadTimeout("read timed out")
        response = _make_sync_error_response(lines, exc)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    def test_jsonl_remote_protocol_error_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.RemoteProtocolError("peer closed connection")
        response = _make_sync_error_response(lines, exc)
        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    def test_csv_read_error_sets_incomplete(self) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        stream = ExportStream(response, "csv")
        rows = list(stream)

        assert len(rows) == 1
        assert stream.complete is False
        assert stream.error is exc

    def test_error_property_none_on_success(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"__export_complete":true}',
        ]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        list(stream)

        assert stream.error is None

    def test_error_accessible_after_context_manager_exit(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        with ExportStream(response, "jsonl") as stream:
            list(stream)
        # After context manager exit, error is still accessible
        assert stream.error is exc


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

    async def test_async_jsonl_read_error_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(lines, exc)
        stream = AsyncExportStream(response, "jsonl")
        rows: list[dict[str, Any]] = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    async def test_async_read_timeout_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.ReadTimeout("read timed out")
        response = _make_async_error_response(lines, exc)
        stream = AsyncExportStream(response, "jsonl")
        rows: list[dict[str, Any]] = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    async def test_async_remote_protocol_error_sets_incomplete(self) -> None:
        lines = [
            '{"cik":"320193","ticker":"AAPL"}',
            '{"cik":"1067983","ticker":"BRK-B"}',
        ]
        exc = httpx.RemoteProtocolError("peer closed connection")
        response = _make_async_error_response(lines, exc)
        stream = AsyncExportStream(response, "jsonl")
        rows: list[dict[str, Any]] = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is exc

    async def test_async_csv_read_error_sets_incomplete(self) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(lines, exc)
        stream = AsyncExportStream(response, "csv")
        rows: list[dict[str, Any]] = []
        async for row in stream:
            rows.append(row)

        # CSV collects all lines first, then parses. Error during aiter_lines
        # means we got partial lines, but DictReader may not yield rows from them.
        assert stream.complete is False
        assert stream.error is exc


# --- File write — partial results on stream error ---


class TestWriteToFilePartial:
    def test_write_to_file_sync_partial_on_read_error(self, tmp_path: Path) -> None:
        lines = [
            '{"cik":"1","ticker":"A"}',
            '{"cik":"2","ticker":"B"}',
            '{"cik":"3","ticker":"C"}',
            '{"cik":"4","ticker":"D"}',
            '{"cik":"5","ticker":"E"}',
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        out = str(tmp_path / "partial.jsonl")

        result = _write_to_file_sync(response, out, "jsonl")

        assert result.complete is False
        assert result.rows == 5
        content = Path(out).read_text()
        assert len(content.strip().split("\n")) == 5

    def test_write_to_file_sync_partial_csv_on_read_error(self, tmp_path: Path) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
            "1067983,BRK-B,Berkshire Hathaway",
            "789019,MSFT,Microsoft Corp",
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        out = str(tmp_path / "partial.csv")

        result = _write_to_file_sync(response, out, "csv")

        assert result.complete is False
        # 4 lines written (header + 3 data), minus 1 for header = 3 rows
        assert result.rows == 3
        content = Path(out).read_text()
        assert len(content.strip().split("\n")) == 4  # header + 3 data lines

    async def test_write_to_file_async_partial_on_read_error(self, tmp_path: Path) -> None:
        lines = [
            '{"cik":"1","ticker":"A"}',
            '{"cik":"2","ticker":"B"}',
            '{"cik":"3","ticker":"C"}',
            '{"cik":"4","ticker":"D"}',
            '{"cik":"5","ticker":"E"}',
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(lines, exc)
        out = str(tmp_path / "partial.jsonl")

        result = await _write_to_file_async(response, out, "jsonl")

        assert result.complete is False
        assert result.rows == 5
        content = Path(out).read_text()
        assert len(content.strip().split("\n")) == 5

    async def test_write_to_file_async_partial_csv_on_read_error(self, tmp_path: Path) -> None:
        lines = [
            "cik,ticker,name",
            "320193,AAPL,Apple Inc",
            "1067983,BRK-B,Berkshire Hathaway",
            "789019,MSFT,Microsoft Corp",
        ]
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(lines, exc)
        out = str(tmp_path / "partial.csv")

        result = await _write_to_file_async(response, out, "csv")

        assert result.complete is False
        # 4 lines written (header + 3 data), minus 1 for header = 3 rows
        assert result.rows == 3
        content = Path(out).read_text()
        assert len(content.strip().split("\n")) == 4  # header + 3 data lines


# --- Helpers for resume loop tests ---


def _make_stream_fn_mock(responses: list[MagicMock]) -> Mock:
    """Return a Mock callable that returns successive responses and records call args."""
    mock = Mock(side_effect=responses)
    return mock


def _make_async_stream_fn_mock(responses: list[MagicMock]) -> AsyncMock:
    """Return an AsyncMock callable that returns successive responses and records call args."""
    mock = AsyncMock(side_effect=responses)
    return mock


# --- Resume loop — sync JSONL ---


class TestWriteToFileResume:
    def test_write_to_file_sync_resumes_on_read_error(self, tmp_path: Path) -> None:
        """First stream yields 3 rows then ReadError, second yields 2 rows + sentinel."""
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T09:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"4","ticker":"D","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"cik":"5","ticker":"E","updated_at":"2026-01-15T13:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        exc = httpx.ReadError("connection reset")
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)
        stream_fn = _make_stream_fn_mock([resp2])
        params = {"format": "jsonl"}
        out = str(tmp_path / "resume.jsonl")

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3)

        assert result.complete is True
        assert result.rows == 5
        assert result.retries == 1
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5
        # Verify the resume call used since=max(updated_at) = 2026-01-15T11:00:00+00:00
        assert stream_fn.call_count == 1
        resume_params = stream_fn.call_args_list[0][0][0]
        assert resume_params["since"] == "2026-01-15T11:00:00+00:00"

    def test_write_to_file_sync_resumes_multiple_times(self, tmp_path: Path) -> None:
        """Mock 3 failures then success. Assert retries == 3, complete is True."""
        exc = httpx.ReadError("connection reset")
        rows_a = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        rows_b = ['{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}']
        rows_c = ['{"cik":"3","ticker":"C","updated_at":"2026-01-15T12:00:00+00:00"}']
        rows_d = [
            '{"cik":"4","ticker":"D","updated_at":"2026-01-15T13:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        resp_initial = _make_sync_error_response(rows_a, exc)
        resp_retry1 = _make_sync_error_response(rows_b, exc)
        resp_retry2 = _make_sync_error_response(rows_c, exc)
        resp_retry3 = _make_sync_response(rows_d)
        stream_fn = _make_stream_fn_mock([resp_retry1, resp_retry2, resp_retry3])
        params = {"format": "jsonl"}
        out = str(tmp_path / "resume_multi.jsonl")

        result = _write_to_file_sync(
            resp_initial, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is True
        assert result.retries == 3
        assert result.rows == 4
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 4

    def test_write_to_file_sync_exhausts_retries(self, tmp_path: Path) -> None:
        """All 4 attempts fail. Assert complete is False, retries == 3."""
        exc = httpx.ReadError("connection reset")
        rows = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        resp_initial = _make_sync_error_response(rows, exc)
        resp_retry1 = _make_sync_error_response(rows, exc)
        resp_retry2 = _make_sync_error_response(rows, exc)
        resp_retry3 = _make_sync_error_response(rows, exc)
        stream_fn = _make_stream_fn_mock([resp_retry1, resp_retry2, resp_retry3])
        params = {"format": "jsonl"}
        out = str(tmp_path / "exhausted.jsonl")

        result = _write_to_file_sync(
            resp_initial, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is False
        assert result.retries == 3
        assert result.rows == 4  # 1 row per attempt * 4 attempts

    def test_write_to_file_sync_csv_no_resume(self, tmp_path: Path) -> None:
        """CSV response that fails mid-stream. Assert complete is False, retries == 0."""
        lines = ["cik,ticker,name", "320193,AAPL,Apple Inc"]
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(lines, exc)
        stream_fn = _make_stream_fn_mock([])
        params = {"format": "csv"}
        out = str(tmp_path / "partial.csv")

        result = _write_to_file_sync(response, out, "csv", stream_fn=stream_fn, params=params, max_resume_retries=3)

        assert result.complete is False
        assert result.retries == 0

    def test_write_to_file_sync_no_updated_at_no_resume(self, tmp_path: Path) -> None:
        """JSONL rows without updated_at that fail. Assert complete is False, retries == 0."""
        rows = ['{"cik":"1","ticker":"A"}', '{"cik":"2","ticker":"B"}']
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(rows, exc)
        stream_fn = _make_stream_fn_mock([])
        params = {"format": "jsonl"}
        out = str(tmp_path / "no_cursor.jsonl")

        result = _write_to_file_sync(response, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3)

        assert result.complete is False
        assert result.retries == 0

    def test_write_to_file_sync_appends_on_retry(self, tmp_path: Path) -> None:
        """First response yields 2 rows then fails, second yields 3 rows + sentinel.
        Verify file contains all 5 rows without truncation."""
        exc = httpx.ReadError("connection reset")
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"cik":"4","ticker":"D","updated_at":"2026-01-15T13:00:00+00:00"}',
            '{"cik":"5","ticker":"E","updated_at":"2026-01-15T14:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)
        stream_fn = _make_stream_fn_mock([resp2])
        params = {"format": "jsonl"}
        out = str(tmp_path / "append.jsonl")

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3)

        assert result.complete is True
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    def test_write_to_file_sync_zero_retries(self, tmp_path: Path) -> None:
        """Pass max_resume_retries=0. Assert complete is False, retries == 0."""
        rows = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        exc = httpx.ReadError("connection reset")
        response = _make_sync_error_response(rows, exc)
        stream_fn = _make_stream_fn_mock([])
        params = {"format": "jsonl"}
        out = str(tmp_path / "zero_retries.jsonl")

        result = _write_to_file_sync(response, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=0)

        assert result.complete is False
        assert result.retries == 0


# --- Timestamp tracking ---


class TestTimestampTracking:
    def test_updated_at_max_tracked_across_rows(self, tmp_path: Path) -> None:
        """Rows with non-ascending updated_at. Verify resume since is the max."""
        rows = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T09:00:00+00:00"}',
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        exc = httpx.ReadError("connection reset")
        resp_initial = _make_sync_error_response(rows, exc)
        resp_retry = _make_sync_response(['{"__export_complete":true}'])
        stream_fn = _make_stream_fn_mock([resp_retry])
        params = {"format": "jsonl"}
        out = str(tmp_path / "max_ts.jsonl")

        _write_to_file_sync(resp_initial, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3)

        resume_params = stream_fn.call_args_list[0][0][0]
        assert resume_params["since"] == "2026-01-15T11:00:00+00:00"

    def test_updated_at_ascending_order(self, tmp_path: Path) -> None:
        """Rows with strictly ascending updated_at. Verify resume since is the last (= max)."""
        rows = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T08:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T09:00:00+00:00"}',
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T10:00:00+00:00"}',
        ]
        exc = httpx.ReadError("connection reset")
        resp_initial = _make_sync_error_response(rows, exc)
        resp_retry = _make_sync_response(['{"__export_complete":true}'])
        stream_fn = _make_stream_fn_mock([resp_retry])
        params = {"format": "jsonl"}
        out = str(tmp_path / "asc_ts.jsonl")

        _write_to_file_sync(resp_initial, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3)

        resume_params = stream_fn.call_args_list[0][0][0]
        assert resume_params["since"] == "2026-01-15T10:00:00+00:00"


# --- Resume loop — async JSONL ---


class TestWriteToFileResumeAsync:
    async def test_write_to_file_async_resumes_on_read_error(self, tmp_path: Path) -> None:
        """First stream yields 3 rows then ReadError, second yields 2 rows + sentinel."""
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T09:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"4","ticker":"D","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"cik":"5","ticker":"E","updated_at":"2026-01-15T13:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        exc = httpx.ReadError("connection reset")
        resp1 = _make_async_error_response(rows1, exc)
        resp2 = _make_async_response(rows2)
        stream_fn = _make_async_stream_fn_mock([resp2])
        params = {"format": "jsonl"}
        out = str(tmp_path / "resume_async.jsonl")

        result = await _write_to_file_async(
            resp1, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is True
        assert result.rows == 5
        assert result.retries == 1
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    async def test_write_to_file_async_exhausts_retries(self, tmp_path: Path) -> None:
        """All 4 attempts fail."""
        exc = httpx.ReadError("connection reset")
        rows = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        resp_initial = _make_async_error_response(rows, exc)
        resp_retry1 = _make_async_error_response(rows, exc)
        resp_retry2 = _make_async_error_response(rows, exc)
        resp_retry3 = _make_async_error_response(rows, exc)
        stream_fn = _make_async_stream_fn_mock([resp_retry1, resp_retry2, resp_retry3])
        params = {"format": "jsonl"}
        out = str(tmp_path / "exhausted_async.jsonl")

        result = await _write_to_file_async(
            resp_initial, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is False
        assert result.retries == 3
        assert result.rows == 4

    async def test_write_to_file_async_csv_no_resume(self, tmp_path: Path) -> None:
        """CSV response that fails mid-stream."""
        lines = ["cik,ticker,name", "320193,AAPL,Apple Inc"]
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(lines, exc)
        stream_fn = _make_async_stream_fn_mock([])
        params = {"format": "csv"}
        out = str(tmp_path / "partial_async.csv")

        result = await _write_to_file_async(
            response, out, "csv", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is False
        assert result.retries == 0

    async def test_write_to_file_async_no_updated_at_no_resume(self, tmp_path: Path) -> None:
        """JSONL rows without updated_at that fail."""
        rows = ['{"cik":"1","ticker":"A"}', '{"cik":"2","ticker":"B"}']
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(rows, exc)
        stream_fn = _make_async_stream_fn_mock([])
        params = {"format": "jsonl"}
        out = str(tmp_path / "no_cursor_async.jsonl")

        result = await _write_to_file_async(
            response, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is False
        assert result.retries == 0

    async def test_write_to_file_async_appends_on_retry(self, tmp_path: Path) -> None:
        """First response yields 2 rows then fails, second yields 3 rows + sentinel."""
        exc = httpx.ReadError("connection reset")
        rows1 = [
            '{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}',
            '{"cik":"2","ticker":"B","updated_at":"2026-01-15T11:00:00+00:00"}',
        ]
        rows2 = [
            '{"cik":"3","ticker":"C","updated_at":"2026-01-15T12:00:00+00:00"}',
            '{"cik":"4","ticker":"D","updated_at":"2026-01-15T13:00:00+00:00"}',
            '{"cik":"5","ticker":"E","updated_at":"2026-01-15T14:00:00+00:00"}',
            '{"__export_complete":true}',
        ]
        resp1 = _make_async_error_response(rows1, exc)
        resp2 = _make_async_response(rows2)
        stream_fn = _make_async_stream_fn_mock([resp2])
        params = {"format": "jsonl"}
        out = str(tmp_path / "append_async.jsonl")

        result = await _write_to_file_async(
            resp1, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=3
        )

        assert result.complete is True
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    async def test_write_to_file_async_zero_retries(self, tmp_path: Path) -> None:
        """Pass max_resume_retries=0."""
        rows = ['{"cik":"1","ticker":"A","updated_at":"2026-01-15T10:00:00+00:00"}']
        exc = httpx.ReadError("connection reset")
        response = _make_async_error_response(rows, exc)
        stream_fn = _make_async_stream_fn_mock([])
        params = {"format": "jsonl"}
        out = str(tmp_path / "zero_retries_async.jsonl")

        result = await _write_to_file_async(
            response, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=0
        )

        assert result.complete is False
        assert result.retries == 0
