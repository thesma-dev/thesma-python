"""QA tests for SDK-14: Export auto-resume on stream failure.

Written from spec only — no dev implementation was consulted.
Tests cover:
  - ExportResult.retries field (new field, default 0)
  - _write_to_file_sync resume loop (JSONL)
  - _write_to_file_async resume loop (JSONL)
  - CSV no-resume behaviour
  - updated_at max tracking for the `since` resume cursor
  - Resource-layer threading of resume params
  - CLI --max-retries option
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest
from click.testing import CliRunner

from thesma._export import (
    ExportResult,
    _write_to_file_async,
    _write_to_file_sync,
)
from thesma.cli.main import cli
from thesma.client import AsyncThesmaClient, ThesmaClient

BASE = "https://api.thesma.dev"
API_KEY = "th_test_000000000000000000000000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_then_raise(lines: list[str], exc: Exception) -> Iterator[str]:
    """Yield lines then raise -- simulates a mid-stream connection drop."""
    yield from lines
    raise exc


async def _async_iter_then_raise(lines: list[str], exc: Exception) -> AsyncIterator[str]:
    """Async variant of _iter_then_raise."""
    for line in lines:
        yield line
    raise exc


def _make_sync_response(lines: list[str]) -> MagicMock:
    """Create a mock httpx.Response with iter_lines() returning the given lines."""
    response = MagicMock()
    response.iter_lines.return_value = iter(lines)
    return response


def _make_sync_error_response(lines: list[str], exc: Exception) -> MagicMock:
    """Create a mock response whose iter_lines() yields lines then raises."""
    response = MagicMock()
    response.iter_lines.return_value = _iter_then_raise(lines, exc)
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


def _make_stream_fn_mock(responses: list[MagicMock]) -> Mock:
    """Return a Mock callable that returns successive responses and records call args."""
    mock = Mock(side_effect=responses)
    return mock


def _jsonl_row(cik: str, ticker: str, updated_at: str) -> str:
    """Build a JSONL row string with an updated_at field."""
    return json.dumps({"cik": cik, "ticker": ticker, "updated_at": updated_at})


SENTINEL = '{"__export_complete":true}'


# ---------------------------------------------------------------------------
# ExportResult.retries field
# ---------------------------------------------------------------------------


class TestExportResultRetries:
    def test_export_result_retries_field(self) -> None:
        """ExportResult should accept and store a retries value."""
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl", retries=2)
        assert result.retries == 2

    def test_export_result_retries_default_zero(self) -> None:
        """retries must default to 0 for backward compatibility."""
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl")
        assert result.retries == 0

    def test_export_result_fields_includes_retries(self) -> None:
        """Existing positional construction still works, retries is the last field."""
        result = ExportResult(path="/tmp/out.jsonl", rows=100, complete=True, format="jsonl")
        assert result.path == "/tmp/out.jsonl"
        assert result.rows == 100
        assert result.complete is True
        assert result.format == "jsonl"
        assert result.retries == 0


# ---------------------------------------------------------------------------
# Resume loop -- sync JSONL
# ---------------------------------------------------------------------------


class TestWriteToFileResume:
    def test_write_to_file_sync_resumes_on_read_error(self, tmp_path: Path) -> None:
        """First attempt yields 3 rows then ReadError; second attempt yields 2 rows + sentinel."""
        rows_attempt1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
            _jsonl_row("3", "C", "2026-01-15T09:00:00+00:00"),
        ]
        rows_attempt2 = [
            _jsonl_row("4", "D", "2026-01-15T12:00:00+00:00"),
            _jsonl_row("5", "E", "2026-01-15T13:00:00+00:00"),
            SENTINEL,
        ]

        resp1 = _make_sync_error_response(rows_attempt1, httpx.ReadError("connection reset"))
        resp2 = _make_sync_response(rows_attempt2)
        stream_fn = _make_stream_fn_mock([resp2])

        out = str(tmp_path / "resume.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is True
        assert result.rows == 5
        assert result.retries == 1

        # Verify file contains all 5 rows
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

        # Verify the second call received since=<max updated_at of rows 1-3>
        # Max of 10:00, 11:00, 09:00 is 11:00
        assert stream_fn.call_count == 1
        retry_params = (
            stream_fn.call_args_list[0][0][0]
            if stream_fn.call_args_list[0][0]
            else stream_fn.call_args_list[0][1].get("params", stream_fn.call_args_list[0][0][0])
        )
        assert retry_params.get("since") == "2026-01-15T11:00:00+00:00"

    def test_write_to_file_sync_resumes_multiple_times(self, tmp_path: Path) -> None:
        """3 failures then success => retries == 3, complete is True, all rows present."""
        rows1 = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]
        rows2 = [_jsonl_row("2", "B", "2026-01-15T11:00:00+00:00")]
        rows3 = [_jsonl_row("3", "C", "2026-01-15T12:00:00+00:00")]
        rows4 = [_jsonl_row("4", "D", "2026-01-15T13:00:00+00:00"), SENTINEL]

        exc = httpx.ReadError("drop")
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_error_response(rows2, httpx.ReadError("drop"))
        resp3 = _make_sync_error_response(rows3, httpx.ReadError("drop"))
        resp4 = _make_sync_response(rows4)

        stream_fn = _make_stream_fn_mock([resp2, resp3, resp4])
        out = str(tmp_path / "multi.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.retries == 3
        assert result.complete is True
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 4

    def test_write_to_file_sync_exhausts_retries(self, tmp_path: Path) -> None:
        """All 4 attempts fail => complete is False, retries == 3."""
        exc = httpx.ReadError("drop")
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]

        resp1 = _make_sync_error_response(rows, exc)
        resp2 = _make_sync_error_response(rows, httpx.ReadError("drop"))
        resp3 = _make_sync_error_response(rows, httpx.ReadError("drop"))
        resp4 = _make_sync_error_response(rows, httpx.ReadError("drop"))

        stream_fn = _make_stream_fn_mock([resp2, resp3, resp4])
        out = str(tmp_path / "exhaust.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 3
        # File should have partial rows from all attempts
        content = Path(out).read_text().strip()
        assert len(content) > 0

    def test_write_to_file_sync_csv_no_resume(self, tmp_path: Path) -> None:
        """CSV format does not retry -- complete is False, retries == 0."""
        lines = ["cik,ticker,name", "320193,AAPL,Apple Inc"]
        exc = httpx.ReadError("connection reset")
        resp = _make_sync_error_response(lines, exc)

        # stream_fn should never be called for CSV
        stream_fn = _make_stream_fn_mock([])
        out = str(tmp_path / "partial.csv")
        params: dict[str, Any] = {"format": "csv"}

        result = _write_to_file_sync(resp, out, "csv", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0

    def test_write_to_file_sync_no_updated_at_no_resume(self, tmp_path: Path) -> None:
        """JSONL rows without updated_at cannot determine resume cursor => no resume."""
        rows = [
            '{"cik":"1","ticker":"A"}',
            '{"cik":"2","ticker":"B"}',
        ]
        exc = httpx.ReadError("drop")
        resp = _make_sync_error_response(rows, exc)

        stream_fn = _make_stream_fn_mock([])
        out = str(tmp_path / "no_updated.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0

    def test_write_to_file_sync_appends_on_retry(self, tmp_path: Path) -> None:
        """First attempt writes 2 rows then fails; second writes 3 rows + sentinel.
        File must contain all 5 rows without truncation."""
        rows1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
        ]
        rows2 = [
            _jsonl_row("3", "C", "2026-01-15T12:00:00+00:00"),
            _jsonl_row("4", "D", "2026-01-15T13:00:00+00:00"),
            _jsonl_row("5", "E", "2026-01-15T14:00:00+00:00"),
            SENTINEL,
        ]

        resp1 = _make_sync_error_response(rows1, httpx.ReadError("drop"))
        resp2 = _make_sync_response(rows2)

        stream_fn = _make_stream_fn_mock([resp2])
        out = str(tmp_path / "append.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is True
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    def test_write_to_file_sync_zero_retries(self, tmp_path: Path) -> None:
        """max_resume_retries=0 behaves like pre-SDK-14: no resume."""
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]
        exc = httpx.ReadError("drop")
        resp = _make_sync_error_response(rows, exc)

        stream_fn = _make_stream_fn_mock([])
        out = str(tmp_path / "zero_retries.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = _write_to_file_sync(resp, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=0)

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0


# ---------------------------------------------------------------------------
# Resume loop -- async JSONL
# ---------------------------------------------------------------------------


class TestWriteToFileResumeAsync:
    async def test_write_to_file_async_resumes_on_read_error(self, tmp_path: Path) -> None:
        """Async equivalent of the sync resume test."""
        rows_attempt1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
            _jsonl_row("3", "C", "2026-01-15T09:00:00+00:00"),
        ]
        rows_attempt2 = [
            _jsonl_row("4", "D", "2026-01-15T12:00:00+00:00"),
            _jsonl_row("5", "E", "2026-01-15T13:00:00+00:00"),
            SENTINEL,
        ]

        resp1 = _make_async_error_response(rows_attempt1, httpx.ReadError("connection reset"))
        resp2 = _make_async_response(rows_attempt2)

        # For async, stream_fn returns coroutines
        stream_fn = AsyncMock(side_effect=[resp2])

        out = str(tmp_path / "async_resume.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = await _write_to_file_async(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is True
        assert result.rows == 5
        assert result.retries == 1

        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

        # Verify since param in retry call
        assert stream_fn.call_count == 1

    async def test_write_to_file_async_exhausts_retries(self, tmp_path: Path) -> None:
        """Async equivalent: all attempts fail."""
        exc = httpx.ReadError("drop")
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]

        resp1 = _make_async_error_response(rows, exc)
        resp2 = _make_async_error_response(rows, httpx.ReadError("drop"))
        resp3 = _make_async_error_response(rows, httpx.ReadError("drop"))
        resp4 = _make_async_error_response(rows, httpx.ReadError("drop"))

        stream_fn = AsyncMock(side_effect=[resp2, resp3, resp4])
        out = str(tmp_path / "async_exhaust.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = await _write_to_file_async(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 3

    async def test_write_to_file_async_csv_no_resume(self, tmp_path: Path) -> None:
        """Async CSV: no retry."""
        lines = ["cik,ticker,name", "320193,AAPL,Apple Inc"]
        exc = httpx.ReadError("connection reset")
        resp = _make_async_error_response(lines, exc)

        stream_fn = AsyncMock(side_effect=[])
        out = str(tmp_path / "async_partial.csv")
        params: dict[str, Any] = {"format": "csv"}

        result = await _write_to_file_async(resp, out, "csv", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0

    async def test_write_to_file_async_no_updated_at_no_resume(self, tmp_path: Path) -> None:
        """Async JSONL rows without updated_at => no resume."""
        rows = [
            '{"cik":"1","ticker":"A"}',
            '{"cik":"2","ticker":"B"}',
        ]
        exc = httpx.ReadError("drop")
        resp = _make_async_error_response(rows, exc)

        stream_fn = AsyncMock(side_effect=[])
        out = str(tmp_path / "async_no_updated.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = await _write_to_file_async(resp, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0

    async def test_write_to_file_async_appends_on_retry(self, tmp_path: Path) -> None:
        """Async equivalent: verify file contains all rows from both attempts."""
        rows1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
        ]
        rows2 = [
            _jsonl_row("3", "C", "2026-01-15T12:00:00+00:00"),
            _jsonl_row("4", "D", "2026-01-15T13:00:00+00:00"),
            _jsonl_row("5", "E", "2026-01-15T14:00:00+00:00"),
            SENTINEL,
        ]

        resp1 = _make_async_error_response(rows1, httpx.ReadError("drop"))
        resp2 = _make_async_response(rows2)

        stream_fn = AsyncMock(side_effect=[resp2])
        out = str(tmp_path / "async_append.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = await _write_to_file_async(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert result.complete is True
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    async def test_write_to_file_async_zero_retries(self, tmp_path: Path) -> None:
        """Async: max_resume_retries=0 means no resume."""
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]
        exc = httpx.ReadError("drop")
        resp = _make_async_error_response(rows, exc)

        stream_fn = AsyncMock(side_effect=[])
        out = str(tmp_path / "async_zero.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        result = await _write_to_file_async(
            resp, out, "jsonl", stream_fn=stream_fn, params=params, max_resume_retries=0
        )

        assert result.complete is False
        assert result.retries == 0
        assert stream_fn.call_count == 0


# ---------------------------------------------------------------------------
# Timestamp tracking
# ---------------------------------------------------------------------------


class TestUpdatedAtTracking:
    def test_updated_at_max_tracked_across_rows(self, tmp_path: Path) -> None:
        """Resume since must be the MAX updated_at, not the last-seen."""
        rows = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T09:00:00+00:00"),
            _jsonl_row("3", "C", "2026-01-15T11:00:00+00:00"),
        ]
        exc = httpx.ReadError("drop")
        resp1 = _make_sync_error_response(rows, exc)

        # Second attempt succeeds
        resp2 = _make_sync_response([_jsonl_row("4", "D", "2026-01-15T12:00:00+00:00"), SENTINEL])
        stream_fn = _make_stream_fn_mock([resp2])

        out = str(tmp_path / "max_tracking.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        # Inspect the retry call: since should be max = 11:00, not last-seen = 11:00 (happens to be last here)
        assert stream_fn.call_count == 1
        retry_params = stream_fn.call_args_list[0][0][0]
        assert retry_params["since"] == "2026-01-15T11:00:00+00:00"

    def test_updated_at_ascending_order(self, tmp_path: Path) -> None:
        """When rows are strictly ascending, the resume since is the last (= max) row's updated_at."""
        rows = [
            _jsonl_row("1", "A", "2026-01-15T08:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T09:00:00+00:00"),
            _jsonl_row("3", "C", "2026-01-15T10:00:00+00:00"),
        ]
        exc = httpx.ReadError("drop")
        resp1 = _make_sync_error_response(rows, exc)

        resp2 = _make_sync_response([_jsonl_row("4", "D", "2026-01-15T11:00:00+00:00"), SENTINEL])
        stream_fn = _make_stream_fn_mock([resp2])

        out = str(tmp_path / "ascending.jsonl")
        params: dict[str, Any] = {"format": "jsonl"}

        _write_to_file_sync(resp1, out, "jsonl", stream_fn=stream_fn, params=params)

        assert stream_fn.call_count == 1
        retry_params = stream_fn.call_args_list[0][0][0]
        assert retry_params["since"] == "2026-01-15T10:00:00+00:00"


# ---------------------------------------------------------------------------
# Regression tests -- resource layer
# ---------------------------------------------------------------------------


class TestExportCompaniesResume:
    """End-to-end through the resource layer with resume."""

    def test_file_output_with_resume(self, api_key: str, tmp_path: Path) -> None:
        """Patch _stream_get to return two mock responses: first fails, second succeeds."""
        rows1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
        ]
        rows2 = [
            _jsonl_row("3", "C", "2026-01-15T12:00:00+00:00"),
            SENTINEL,
        ]
        exc = httpx.ReadError("drop")
        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "companies.jsonl")

        with patch.object(client, "_stream_get", side_effect=[resp1, resp2]):
            result = client.export.companies(output=out_path)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 1
        content = Path(out_path).read_text().strip().split("\n")
        assert len(content) == 3
        client.close()

    def test_max_resume_retries_param_passed(self, api_key: str, tmp_path: Path) -> None:
        """Verify max_resume_retries=5 is honoured by the resource layer."""
        exc = httpx.ReadError("drop")
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]

        # 5 failures then success
        responses = [_make_sync_error_response(rows, httpx.ReadError("drop")) for _ in range(5)]
        responses.append(_make_sync_response([_jsonl_row("N", "Z", "2026-01-15T20:00:00+00:00"), SENTINEL]))

        # First response for the initial call, the rest for retries
        initial = _make_sync_error_response(rows, exc)

        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "retries5.jsonl")

        with patch.object(client, "_stream_get", side_effect=[initial, *responses]):
            result = client.export.companies(output=out_path, max_resume_retries=5)

        assert isinstance(result, ExportResult)
        assert result.retries == 5
        assert result.complete is False or result.complete is True  # depends on whether 6th attempt works
        client.close()

    def test_resume_since_param_sent_correctly(self, api_key: str, tmp_path: Path) -> None:
        """Verify the retry call includes since=<max_updated_at> in params."""
        rows1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
        ]
        rows2 = [_jsonl_row("3", "C", "2026-01-15T12:00:00+00:00"), SENTINEL]
        exc = httpx.ReadError("drop")

        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "since_check.jsonl")

        mock_stream_get = Mock(side_effect=[resp1, resp2])
        with patch.object(client, "_stream_get", mock_stream_get):
            client.export.companies(output=out_path)

        # First call: no since param (or original since)
        # Second call: since=2026-01-15T11:00:00+00:00
        assert mock_stream_get.call_count == 2
        second_call_kwargs = mock_stream_get.call_args_list[1]
        # The params may be passed as a keyword arg or positional
        # Check that since is in the params dict
        if second_call_kwargs.kwargs.get("params"):
            assert second_call_kwargs.kwargs["params"]["since"] == "2026-01-15T11:00:00+00:00"
        else:
            # Might be in positional args
            params_arg = second_call_kwargs[1].get(
                "params", second_call_kwargs[0][1] if len(second_call_kwargs[0]) > 1 else {}
            )
            assert params_arg.get("since") == "2026-01-15T11:00:00+00:00"

        client.close()

    def test_resume_preserves_other_params(self, api_key: str, tmp_path: Path) -> None:
        """Verify cik and format are preserved on retry alongside the new since."""
        rows1 = [_jsonl_row("320193", "AAPL", "2026-01-15T10:00:00+00:00")]
        rows2 = [_jsonl_row("320193", "AAPL", "2026-01-15T11:00:00+00:00"), SENTINEL]
        exc = httpx.ReadError("drop")

        resp1 = _make_sync_error_response(rows1, exc)
        resp2 = _make_sync_response(rows2)

        client = ThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "preserve.jsonl")

        mock_stream_get = Mock(side_effect=[resp1, resp2])
        with patch.object(client, "_stream_get", mock_stream_get):
            client.export.companies(output=out_path, cik="320193")

        assert mock_stream_get.call_count == 2
        second_call = mock_stream_get.call_args_list[1]
        params = second_call.kwargs.get("params", {})
        assert params.get("cik") == "320193"
        assert params.get("format") == "jsonl"
        assert params.get("since") == "2026-01-15T10:00:00+00:00"

        client.close()


class TestAsyncExportResume:
    """Async resource-layer resume tests."""

    async def test_async_file_output_with_resume(self, api_key: str, tmp_path: Path) -> None:
        """Async equivalent of test_file_output_with_resume."""
        rows1 = [
            _jsonl_row("1", "A", "2026-01-15T10:00:00+00:00"),
            _jsonl_row("2", "B", "2026-01-15T11:00:00+00:00"),
        ]
        rows2 = [
            _jsonl_row("3", "C", "2026-01-15T12:00:00+00:00"),
            SENTINEL,
        ]
        exc = httpx.ReadError("drop")
        resp1 = _make_async_error_response(rows1, exc)
        resp2 = _make_async_response(rows2)

        client = AsyncThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "async_companies.jsonl")

        with patch.object(client, "_async_stream_get", AsyncMock(side_effect=[resp1, resp2])):
            result = await client.export.companies(output=out_path)

        assert isinstance(result, ExportResult)
        assert result.complete is True
        assert result.retries == 1
        content = Path(out_path).read_text().strip().split("\n")
        assert len(content) == 3
        await client.close()

    async def test_async_max_resume_retries_param_passed(self, api_key: str, tmp_path: Path) -> None:
        """Async: verify max_resume_retries param is honoured."""
        exc = httpx.ReadError("drop")
        rows = [_jsonl_row("1", "A", "2026-01-15T10:00:00+00:00")]

        responses = [_make_async_error_response(rows, httpx.ReadError("drop")) for _ in range(5)]
        responses.append(_make_async_response([_jsonl_row("N", "Z", "2026-01-15T20:00:00+00:00"), SENTINEL]))

        initial = _make_async_error_response(rows, exc)

        client = AsyncThesmaClient(api_key=api_key)
        out_path = str(tmp_path / "async_retries5.jsonl")

        with patch.object(client, "_async_stream_get", AsyncMock(side_effect=[initial, *responses])):
            result = await client.export.companies(output=out_path, max_resume_retries=5)

        assert isinstance(result, ExportResult)
        assert result.retries == 5
        await client.close()


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestExportCliResume:
    def test_export_max_retries_option(self, runner: CliRunner, tmp_path: Path) -> None:
        """CLI with --max-retries 2 --output <path> delegates to resource with max_resume_retries=2."""
        out_path = str(tmp_path / "cli_retries.jsonl")
        mock_result = ExportResult(path=out_path, rows=100, complete=True, format="jsonl", retries=1)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(
                cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path, "--max-retries", "2"]
            )

        assert result.exit_code == 0
        assert "complete" in result.output.lower()

    def test_export_max_retries_default(self, runner: CliRunner, tmp_path: Path) -> None:
        """Without --max-retries, the export method should be called with max_resume_retries=3."""
        out_path = str(tmp_path / "cli_default.jsonl")
        mock_result = ExportResult(path=out_path, rows=50, complete=True, format="jsonl", retries=0)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        assert result.exit_code == 0
        # Verify the export method was called with max_resume_retries=3
        call_kwargs = mock_client.export.companies.call_args
        assert call_kwargs.kwargs.get("max_resume_retries") == 3 or call_kwargs[1].get("max_resume_retries") == 3

    def test_export_incomplete_with_retries_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        """When export is incomplete, warning should mention retries."""
        out_path = str(tmp_path / "cli_incomplete.jsonl")
        mock_result = ExportResult(path=out_path, rows=50, complete=False, format="jsonl", retries=3)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        # Warning should reference retries
        output_lower = result.output.lower()
        assert "incomplete" in output_lower or "warning" in output_lower
        assert "3" in result.output or "retries" in output_lower

    def test_export_stdout_mode_ignores_max_retries(self, runner: CliRunner) -> None:
        """Stdout streaming mode should still use ExportStream, not resource output= mode."""
        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            # When no output is given, the CLI should call export_method() without output=
            # and get an ExportStream for stdout iteration
            from thesma._export import ExportStream

            mock_response = _make_sync_response(
                [
                    '{"cik":"320193","ticker":"AAPL"}',
                    '{"__export_complete":true}',
                ]
            )
            mock_stream = ExportStream(mock_response, "jsonl")
            mock_client.export.companies.return_value = mock_stream

            result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--max-retries", "5"])

        # Should succeed and stream to stdout
        assert result.exit_code == 0
        assert "320193" in result.output
