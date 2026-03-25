"""Export stream and result types for bulk data exports."""

from __future__ import annotations

import asyncio
import csv
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import httpx

_STREAM_ERRORS = (httpx.ReadError, httpx.ReadTimeout, httpx.RemoteProtocolError)


@dataclass
class ExportResult:
    """Metadata returned after a file-download export completes."""

    path: str
    rows: int
    complete: bool
    format: str
    retries: int = field(default=0)


class ExportStream:
    """Synchronous iterator over a streaming export response.

    Yields parsed ``dict`` objects — one per data row. For JSONL exports,
    detects the ``__export_complete`` sentinel and sets :attr:`complete`
    to ``True`` when found. For CSV exports, ``complete`` is always
    ``True`` after iteration finishes.

    Supports context-manager protocol to ensure the HTTP connection is
    closed if iteration is abandoned early::

        with client.export.financials() as stream:
            for row in stream:
                ...
    """

    def __init__(self, response: Any, fmt: str = "jsonl") -> None:
        self._response = response
        self._format = fmt
        self._complete = False
        self._closed = False
        self._error: Exception | None = None

    @property
    def complete(self) -> bool:
        """Whether the export completed successfully (sentinel found for JSONL, always True for CSV)."""
        return self._complete

    @property
    def error(self) -> Exception | None:
        """The exception that terminated the stream, or ``None`` if iteration succeeded."""
        return self._error

    def __enter__(self) -> ExportStream:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP response."""
        if not self._closed:
            self._response.close()
            self._closed = True

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self._iterate()

    def _iterate(self) -> Iterator[dict[str, Any]]:
        try:
            if self._format == "csv":
                yield from self._iterate_csv()
            else:
                yield from self._iterate_jsonl()
        except GeneratorExit:
            return

    def _iterate_jsonl(self) -> Iterator[dict[str, Any]]:
        try:
            for line in self._response.iter_lines():
                if not line:
                    continue
                data = json.loads(line)
                if "__export_complete" in data:
                    self._complete = True
                    continue
                yield data
        except _STREAM_ERRORS as exc:
            self._error = exc
            return

    def _iterate_csv(self) -> Iterator[dict[str, Any]]:
        try:
            reader = csv.DictReader(self._response.iter_lines())
            for row in reader:
                yield dict(row)
            self._complete = True
        except _STREAM_ERRORS as exc:
            self._error = exc
            return


class AsyncExportStream:
    """Asynchronous iterator over a streaming export response.

    Async variant of :class:`ExportStream`. Uses ``aiter_lines()`` for
    streaming and yields parsed ``dict`` objects.
    """

    def __init__(self, response: Any, fmt: str = "jsonl") -> None:
        self._response = response
        self._format = fmt
        self._complete = False
        self._closed = False
        self._error: Exception | None = None

    @property
    def complete(self) -> bool:
        """Whether the export completed successfully."""
        return self._complete

    @property
    def error(self) -> Exception | None:
        """The exception that terminated the stream, or ``None`` if iteration succeeded."""
        return self._error

    async def __aenter__(self) -> AsyncExportStream:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP response."""
        if not self._closed:
            await self._response.aclose()
            self._closed = True

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        if self._format == "csv":
            return self._iterate_csv()
        return self._iterate_jsonl()

    async def _iterate_jsonl(self) -> AsyncIterator[dict[str, Any]]:
        try:
            async for line in self._response.aiter_lines():
                if not line:
                    continue
                data = json.loads(line)
                if "__export_complete" in data:
                    self._complete = True
                    continue
                yield data
        except _STREAM_ERRORS as exc:
            self._error = exc
            return

    async def _iterate_csv(self) -> AsyncIterator[dict[str, Any]]:
        try:
            lines: list[str] = []
            async for line in self._response.aiter_lines():
                lines.append(line)
            reader = csv.DictReader(lines)
            for row in reader:
                yield dict(row)
            self._complete = True
        except _STREAM_ERRORS as exc:
            self._error = exc
            return


def _serialize_since(since: str | datetime | date | None) -> str | None:
    """Serialize the ``since`` parameter to an ISO format string."""
    if since is None:
        return None
    if isinstance(since, datetime):
        return since.isoformat()
    if isinstance(since, date):
        return since.isoformat()
    return since


def _build_export_params(
    fmt: str,
    since: str | datetime | date | None,
    cik: str | None,
    ticker: str | None,
) -> dict[str, Any]:
    """Build query parameters for an export request, stripping None values."""
    params: dict[str, Any] = {
        "format": fmt,
        "since": _serialize_since(since),
        "cik": cik,
        "ticker": ticker,
    }
    return {k: v for k, v in params.items() if v is not None}


def _write_to_file_sync(
    response: Any,
    output: str,
    fmt: str,
    *,
    stream_fn: Callable[[dict[str, Any]], Any] | None = None,
    params: dict[str, Any] | None = None,
    max_resume_retries: int = 3,
) -> ExportResult:
    """Stream a response to a file synchronously, returning export metadata."""
    from pathlib import Path

    rows = 0
    complete = False
    retries = 0
    max_updated_at: str | None = None

    # Initial attempt — write mode (truncate)
    try:
        with open(output, "w", newline="") as f:
            if fmt == "csv":
                try:
                    for line in response.iter_lines():
                        f.write(line + "\n")
                        rows += 1
                except _STREAM_ERRORS:
                    pass
                else:
                    complete = True
                rows = max(rows - 1, 0)
            else:
                try:
                    for line in response.iter_lines():
                        if not line:
                            continue
                        data = json.loads(line)
                        if "__export_complete" in data:
                            complete = True
                            continue
                        f.write(line + "\n")
                        rows += 1
                        updated_at = data.get("updated_at")
                        if updated_at is not None and (max_updated_at is None or updated_at > max_updated_at):
                            max_updated_at = updated_at
                except _STREAM_ERRORS:
                    pass
    finally:
        response.close()

    # Resume loop — only for JSONL with a valid cursor and stream_fn
    while (
        not complete
        and fmt != "csv"
        and max_updated_at is not None
        and stream_fn is not None
        and params is not None
        and retries < max_resume_retries
    ):
        retries += 1
        resume_params = {**params, "since": max_updated_at}
        response = stream_fn(resume_params)
        try:
            with open(output, "a", newline="") as f:
                try:
                    for line in response.iter_lines():
                        if not line:
                            continue
                        data = json.loads(line)
                        if "__export_complete" in data:
                            complete = True
                            continue
                        f.write(line + "\n")
                        rows += 1
                        updated_at = data.get("updated_at")
                        if updated_at is not None and (max_updated_at is None or updated_at > max_updated_at):
                            max_updated_at = updated_at
                except _STREAM_ERRORS:
                    pass
        finally:
            response.close()

    return ExportResult(
        path=str(Path(output).resolve()),
        rows=rows,
        complete=complete,
        format=fmt,
        retries=retries,
    )


async def _write_to_file_async(
    response: Any,
    output: str,
    fmt: str,
    *,
    stream_fn: Callable[[dict[str, Any]], Awaitable[Any]] | None = None,
    params: dict[str, Any] | None = None,
    max_resume_retries: int = 3,
) -> ExportResult:
    """Stream a response to a file asynchronously, returning export metadata."""
    rows = 0
    complete = False
    retries = 0
    max_updated_at: str | None = None
    batch: list[str] = []
    batch_size = 100

    from pathlib import Path

    resolved = str(Path(output).resolve())

    # Create/truncate the file
    def _truncate(p: str) -> None:
        with open(p, "w", newline=""):
            pass

    await asyncio.to_thread(_truncate, resolved)

    # Initial attempt
    try:
        if fmt == "csv":
            try:
                async for line in response.aiter_lines():
                    batch.append(line + "\n")
                    rows += 1
                    if len(batch) >= batch_size:
                        chunk = "".join(batch)
                        batch.clear()
                        await asyncio.to_thread(_append_to_file, resolved, chunk)
            except _STREAM_ERRORS:
                pass
            else:
                complete = True
            if batch:
                chunk = "".join(batch)
                batch.clear()
                await asyncio.to_thread(_append_to_file, resolved, chunk)
            rows = max(rows - 1, 0)  # subtract header
        else:
            try:
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    data = json.loads(line)
                    if "__export_complete" in data:
                        complete = True
                        continue
                    batch.append(line + "\n")
                    rows += 1
                    updated_at = data.get("updated_at")
                    if updated_at is not None and (max_updated_at is None or updated_at > max_updated_at):
                        max_updated_at = updated_at
                    if len(batch) >= batch_size:
                        chunk = "".join(batch)
                        batch.clear()
                        await asyncio.to_thread(_append_to_file, resolved, chunk)
            except _STREAM_ERRORS:
                pass
            if batch:
                chunk = "".join(batch)
                batch.clear()
                await asyncio.to_thread(_append_to_file, resolved, chunk)
    finally:
        await response.aclose()

    # Resume loop — only for JSONL with a valid cursor and stream_fn
    while (
        not complete
        and fmt != "csv"
        and max_updated_at is not None
        and stream_fn is not None
        and params is not None
        and retries < max_resume_retries
    ):
        retries += 1
        resume_params = {**params, "since": max_updated_at}
        response = await stream_fn(resume_params)
        batch.clear()
        try:
            try:
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    data = json.loads(line)
                    if "__export_complete" in data:
                        complete = True
                        continue
                    batch.append(line + "\n")
                    rows += 1
                    updated_at = data.get("updated_at")
                    if updated_at is not None and (max_updated_at is None or updated_at > max_updated_at):
                        max_updated_at = updated_at
                    if len(batch) >= batch_size:
                        chunk = "".join(batch)
                        batch.clear()
                        await asyncio.to_thread(_append_to_file, resolved, chunk)
            except _STREAM_ERRORS:
                pass
            if batch:
                chunk = "".join(batch)
                batch.clear()
                await asyncio.to_thread(_append_to_file, resolved, chunk)
        finally:
            await response.aclose()

    return ExportResult(
        path=resolved,
        rows=rows,
        complete=complete,
        format=fmt,
        retries=retries,
    )


def _append_to_file(path: str, data: str) -> None:
    """Append data to a file (used as a thread target)."""
    with open(path, "a", newline="") as f:
        f.write(data)
