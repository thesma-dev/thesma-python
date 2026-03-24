"""QA tests for SDK-13: Resilient export streaming.

Written from spec only — no dev implementation consulted.
Covers: stream error handling, export_in_progress retry, configurable stream timeout.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from thesma._export import (
    AsyncExportStream,
    ExportResult,
    ExportStream,
    _write_to_file_async,
    _write_to_file_sync,
)
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import RateLimitError, raise_for_status

# Try importing ExportInProgressError — it may not exist yet (dev hasn't implemented).
# Tests that depend on it will fail at runtime with ImportError, which is the expected
# QA signal ("feature not implemented yet").
try:
    from thesma.errors import ExportInProgressError
except ImportError:
    ExportInProgressError = None  # type: ignore[assignment,misc]


BASE = "https://api.thesma.dev"

# ---------------------------------------------------------------------------
# Helpers — mid-stream error simulation
# ---------------------------------------------------------------------------


def _iter_then_raise(lines: list[str], exc: Exception):
    """Yield lines then raise — simulates a mid-stream connection drop."""
    yield from lines
    raise exc


async def _async_iter_then_raise(lines: list[str], exc: Exception):
    """Async variant: yield lines then raise."""
    for line in lines:
        yield line
    raise exc


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


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

JSONL_ROWS = [
    '{"cik":"320193","ticker":"AAPL","name":"Apple Inc"}',
    '{"cik":"1067983","ticker":"BRK-B","name":"Berkshire Hathaway"}',
]

JSONL_SENTINEL = '{"__export_complete":true}'

CSV_HEADER = "cik,ticker,name"
CSV_DATA_ROWS = [
    "320193,AAPL,Apple Inc",
    "1067983,BRK-B,Berkshire Hathaway",
    "789019,MSFT,Microsoft Corp",
]

JSONL_COMPANIES = '{"cik":"320193","ticker":"AAPL","name":"Apple Inc"}\n{"__export_complete":true}\n'


# ===========================================================================
# Section 1: ExportStream / AsyncExportStream — stream error handling
# ===========================================================================


class TestExportStreamErrors:
    """Sync ExportStream: graceful handling of mid-stream httpx errors."""

    def test_jsonl_read_error_sets_incomplete(self) -> None:
        """ReadError mid-stream: iteration stops, complete=False, error is set."""
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(JSONL_ROWS[:2], error)

        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    def test_jsonl_read_timeout_sets_incomplete(self) -> None:
        """ReadTimeout mid-stream: same graceful behavior."""
        error = httpx.ReadTimeout("read timed out")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(JSONL_ROWS[:2], error)

        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    def test_jsonl_remote_protocol_error_sets_incomplete(self) -> None:
        """RemoteProtocolError mid-stream (the 'Chunk 3' failure mode)."""
        error = httpx.RemoteProtocolError("peer closed connection")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(JSONL_ROWS[:2], error)

        stream = ExportStream(response, "jsonl")
        rows = list(stream)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    def test_csv_read_error_sets_incomplete(self) -> None:
        """ReadError during CSV iteration: same graceful handling."""
        error = httpx.ReadError("connection reset")
        csv_lines = [CSV_HEADER, CSV_DATA_ROWS[0], CSV_DATA_ROWS[1]]
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(csv_lines, error)

        stream = ExportStream(response, "csv")
        rows = list(stream)

        # Should have yielded the 2 data rows before the error
        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    def test_error_property_none_on_success(self) -> None:
        """Normal successful iteration: error is None."""
        lines = [*JSONL_ROWS, JSONL_SENTINEL]
        response = _make_sync_response(lines)
        stream = ExportStream(response, "jsonl")
        list(stream)

        assert stream.complete is True
        assert stream.error is None

    def test_error_accessible_after_context_manager_exit(self) -> None:
        """Error attribute is still accessible after 'with' block exits."""
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(JSONL_ROWS[:1], error)

        with ExportStream(response, "jsonl") as stream:
            list(stream)

        # After context manager __exit__, error should still be accessible
        assert stream.error is error
        assert stream.complete is False


class TestAsyncExportStreamErrors:
    """Async AsyncExportStream: graceful handling of mid-stream httpx errors."""

    async def test_async_jsonl_read_error_sets_incomplete(self) -> None:
        """Async ReadError mid-stream."""
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(JSONL_ROWS[:2], error)
        response.aclose = AsyncMock()

        stream = AsyncExportStream(response, "jsonl")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    async def test_async_read_timeout_sets_incomplete(self) -> None:
        """Async ReadTimeout mid-stream."""
        error = httpx.ReadTimeout("read timed out")
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(JSONL_ROWS[:2], error)
        response.aclose = AsyncMock()

        stream = AsyncExportStream(response, "jsonl")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    async def test_async_remote_protocol_error_sets_incomplete(self) -> None:
        """Async RemoteProtocolError mid-stream."""
        error = httpx.RemoteProtocolError("peer closed connection")
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(JSONL_ROWS[:2], error)
        response.aclose = AsyncMock()

        stream = AsyncExportStream(response, "jsonl")
        rows = []
        async for row in stream:
            rows.append(row)

        assert len(rows) == 2
        assert stream.complete is False
        assert stream.error is error

    async def test_async_csv_read_error_sets_incomplete(self) -> None:
        """Async ReadError during CSV iteration.

        Note: AsyncExportStream._iterate_csv collects all lines into a list
        before processing with csv.DictReader. When ReadError occurs during
        collection, no rows have been yielded yet — so rows is empty.
        """
        error = httpx.ReadError("connection reset")
        csv_lines = [CSV_HEADER, CSV_DATA_ROWS[0], CSV_DATA_ROWS[1]]
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(csv_lines, error)
        response.aclose = AsyncMock()

        stream = AsyncExportStream(response, "csv")
        rows = []
        async for row in stream:
            rows.append(row)

        # Async CSV buffers all lines before parsing — error during collection means 0 rows yielded
        assert len(rows) == 0
        assert stream.complete is False
        assert stream.error is error


# ===========================================================================
# Section 2: File write — partial results on stream error
# ===========================================================================


class TestWriteToFilePartial:
    """_write_to_file_sync / _write_to_file_async: partial file on stream error."""

    def test_write_to_file_sync_partial_on_read_error(self, tmp_path: Path) -> None:
        """JSONL file write: 5 rows streamed, ReadError, file has 5 lines, complete=False."""
        jsonl_rows = [json.dumps({"row": i}) for i in range(5)]
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(jsonl_rows, error)

        out = str(tmp_path / "partial.jsonl")
        result = _write_to_file_sync(response, out, "jsonl")

        assert isinstance(result, ExportResult)
        assert result.complete is False
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    def test_write_to_file_sync_partial_csv_on_read_error(self, tmp_path: Path) -> None:
        """CSV file write: header + 3 data rows, then ReadError. File has header + 3 lines."""
        csv_lines = [CSV_HEADER, *CSV_DATA_ROWS[:3]]
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.iter_lines.return_value = _iter_then_raise(csv_lines, error)

        out = str(tmp_path / "partial.csv")
        result = _write_to_file_sync(response, out, "csv")

        assert isinstance(result, ExportResult)
        assert result.complete is False
        assert result.rows == 3  # 3 data rows (header excluded from count)
        content = Path(out).read_text().strip().split("\n")
        # File should contain header + 3 data rows = 4 lines
        assert len(content) == 4

    async def test_write_to_file_async_partial_on_read_error(self, tmp_path: Path) -> None:
        """Async JSONL file write: 5 rows, ReadError, partial result."""
        jsonl_rows = [json.dumps({"row": i}) for i in range(5)]
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(jsonl_rows, error)
        response.aclose = AsyncMock()

        out = str(tmp_path / "partial_async.jsonl")
        result = await _write_to_file_async(response, out, "jsonl")

        assert isinstance(result, ExportResult)
        assert result.complete is False
        assert result.rows == 5
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 5

    async def test_write_to_file_async_partial_csv_on_read_error(self, tmp_path: Path) -> None:
        """Async CSV file write: header + 3 data rows, ReadError, partial result."""
        csv_lines = [CSV_HEADER, *CSV_DATA_ROWS[:3]]
        error = httpx.ReadError("connection reset")
        response = MagicMock()
        response.aiter_lines.return_value = _async_iter_then_raise(csv_lines, error)
        response.aclose = AsyncMock()

        out = str(tmp_path / "partial_async.csv")
        result = await _write_to_file_async(response, out, "csv")

        assert isinstance(result, ExportResult)
        assert result.complete is False
        assert result.rows == 3
        content = Path(out).read_text().strip().split("\n")
        assert len(content) == 4  # header + 3 data rows


# ===========================================================================
# Section 3: ExportInProgressError — error hierarchy
# ===========================================================================


class TestExportInProgressErrorHierarchy:
    """ExportInProgressError subclass of RateLimitError."""

    def test_export_in_progress_error_is_rate_limit_subclass(self) -> None:
        """ExportInProgressError must be a subclass of RateLimitError."""
        assert ExportInProgressError is not None, "ExportInProgressError not yet implemented"
        assert issubclass(ExportInProgressError, RateLimitError)

    def test_raise_for_status_export_in_progress(self) -> None:
        """429 with error_code='export_in_progress' raises ExportInProgressError specifically."""
        assert ExportInProgressError is not None, "ExportInProgressError not yet implemented"
        response = httpx.Response(
            429,
            json={"detail": "Export in progress", "code": "export_in_progress"},
            headers={"Retry-After": "60"},
        )
        with pytest.raises(ExportInProgressError) as exc_info:
            raise_for_status(response)
        assert exc_info.value.retry_after == 60.0
        assert exc_info.value.error_code == "export_in_progress"

    def test_raise_for_status_429_without_export_code(self) -> None:
        """429 without export_in_progress code raises plain RateLimitError, not ExportInProgressError."""
        response = httpx.Response(
            429,
            json={"detail": "Rate limit exceeded"},
        )
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(response)
        # Must be exactly RateLimitError, not a subclass
        assert type(exc_info.value) is RateLimitError


# ===========================================================================
# Section 4: _stream_get export_in_progress retry — sync
# ===========================================================================


class TestStreamGetExportInProgressRetry:
    """_stream_get retries on 429 export_in_progress with backoff."""

    @respx.mock
    @patch("thesma.client.time.sleep")
    def test_stream_get_retries_export_in_progress(self, mock_sleep: MagicMock, api_key: str) -> None:
        """429 export_in_progress twice, then 200: should succeed on 3rd call."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "30"},
                ),
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "30"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = ThesmaClient(api_key=api_key)
        response = client._stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 3
        assert response.status_code == 200
        response.close()
        client.close()

    @respx.mock
    @patch("thesma.client.time.sleep")
    def test_stream_get_export_in_progress_exhausted(self, mock_sleep: MagicMock, api_key: str) -> None:
        """429 export_in_progress 7 times: raises ExportInProgressError after 6 retries (7 total calls)."""
        assert ExportInProgressError is not None, "ExportInProgressError not yet implemented"
        responses = [
            httpx.Response(
                429,
                json={"detail": "Export in progress", "code": "export_in_progress"},
                headers={"Retry-After": "30"},
            )
            for _ in range(7)
        ]
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(side_effect=responses)
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(ExportInProgressError):
            client._stream_get("/v1/us/sec/export/companies")

        # 1 initial attempt + 6 retries = 7 total calls
        assert route.call_count == 7
        client.close()

    @respx.mock
    @patch("thesma.client.time.sleep")
    def test_stream_get_respects_retry_after(self, mock_sleep: MagicMock, api_key: str) -> None:
        """Sleep duration respects Retry-After header plus jitter [0, 0.5)."""
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
        response = client._stream_get("/v1/us/sec/export/companies")

        mock_sleep.assert_called_once()
        sleep_val = mock_sleep.call_args[0][0]
        assert sleep_val >= 10.0
        assert sleep_val <= 10.5
        response.close()
        client.close()

    @respx.mock
    @patch("thesma.client.time.sleep")
    def test_stream_get_export_in_progress_missing_retry_after(self, mock_sleep: MagicMock, api_key: str) -> None:
        """No Retry-After header: default to 30s + jitter."""
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
        response = client._stream_get("/v1/us/sec/export/companies")

        mock_sleep.assert_called_once()
        sleep_val = mock_sleep.call_args[0][0]
        assert sleep_val >= 30.0
        assert sleep_val <= 30.5
        response.close()
        client.close()

    @respx.mock
    def test_stream_get_regular_429_no_retry(self, api_key: str) -> None:
        """Plain 429 (no export_in_progress code): raises RateLimitError immediately, 1 call."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "30"},
            ),
        )
        client = ThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError):
            client._stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 1
        client.close()


# ===========================================================================
# Section 5: _async_stream_get export_in_progress retry — async
# ===========================================================================


class TestAsyncStreamGetExportInProgressRetry:
    """_async_stream_get retries on 429 export_in_progress with backoff."""

    @respx.mock
    @patch("thesma.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_async_stream_get_retries_export_in_progress(self, mock_sleep: AsyncMock, api_key: str) -> None:
        """Async: 429 twice, then 200."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "30"},
                ),
                httpx.Response(
                    429,
                    json={"detail": "Export in progress", "code": "export_in_progress"},
                    headers={"Retry-After": "30"},
                ),
                httpx.Response(200, content=JSONL_COMPANIES.encode()),
            ],
        )
        client = AsyncThesmaClient(api_key=api_key)
        response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 3
        assert response.status_code == 200
        await response.aclose()
        await client.close()

    @respx.mock
    @patch("thesma.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_async_stream_get_export_in_progress_exhausted(self, mock_sleep: AsyncMock, api_key: str) -> None:
        """Async: 7 x 429 export_in_progress => ExportInProgressError after 6 retries."""
        assert ExportInProgressError is not None, "ExportInProgressError not yet implemented"
        responses = [
            httpx.Response(
                429,
                json={"detail": "Export in progress", "code": "export_in_progress"},
                headers={"Retry-After": "30"},
            )
            for _ in range(7)
        ]
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(side_effect=responses)
        client = AsyncThesmaClient(api_key=api_key)

        with pytest.raises(ExportInProgressError):
            await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 7
        await client.close()

    @respx.mock
    @patch("thesma.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_async_stream_get_respects_retry_after(self, mock_sleep: AsyncMock, api_key: str) -> None:
        """Async: sleep respects Retry-After + jitter."""
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
        response = await client._async_stream_get("/v1/us/sec/export/companies")

        mock_sleep.assert_called_once()
        sleep_val = mock_sleep.call_args[0][0]
        assert sleep_val >= 10.0
        assert sleep_val <= 10.5
        await response.aclose()
        await client.close()

    @respx.mock
    @patch("thesma.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_async_stream_get_missing_retry_after(self, mock_sleep: AsyncMock, api_key: str) -> None:
        """Async: no Retry-After header => default 30s + jitter."""
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
        response = await client._async_stream_get("/v1/us/sec/export/companies")

        mock_sleep.assert_called_once()
        sleep_val = mock_sleep.call_args[0][0]
        assert sleep_val >= 30.0
        assert sleep_val <= 30.5
        await response.aclose()
        await client.close()

    @respx.mock
    async def test_async_stream_get_regular_429_no_retry(self, api_key: str) -> None:
        """Async: plain 429 (not export_in_progress) => RateLimitError immediately, 1 call."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                429,
                json={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "30"},
            ),
        )
        client = AsyncThesmaClient(api_key=api_key)

        with pytest.raises(RateLimitError):
            await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.call_count == 1
        await client.close()


# ===========================================================================
# Section 6: Configurable stream timeout
# ===========================================================================


class TestStreamTimeout:
    """stream_timeout parameter on ThesmaClient / AsyncThesmaClient."""

    def test_stream_timeout_default_300(self, api_key: str) -> None:
        """Default stream_timeout is 300."""
        client = ThesmaClient(api_key=api_key)
        assert client.stream_timeout == 300
        client.close()

    @respx.mock
    def test_stream_timeout_configurable(self, api_key: str) -> None:
        """Custom stream_timeout=600 propagates to the request timeout."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = ThesmaClient(api_key=api_key, stream_timeout=600)
        assert client.stream_timeout == 600

        response = client._stream_get("/v1/us/sec/export/companies")

        assert route.called
        # Verify the timeout on the sent request
        request = route.calls.last.request
        timeout_dict = request.extensions.get("timeout")
        assert timeout_dict is not None
        assert timeout_dict["read"] == 600.0
        response.close()
        client.close()

    @respx.mock
    async def test_async_stream_timeout_configurable(self, api_key: str) -> None:
        """AsyncThesmaClient: custom stream_timeout=600."""
        route = respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        client = AsyncThesmaClient(api_key=api_key, stream_timeout=600)
        assert client.stream_timeout == 600

        response = await client._async_stream_get("/v1/us/sec/export/companies")

        assert route.called
        request = route.calls.last.request
        timeout_dict = request.extensions.get("timeout")
        assert timeout_dict is not None
        assert timeout_dict["read"] == 600.0
        await response.aclose()
        await client.close()
