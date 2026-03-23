"""Export stream and result types for bulk data exports."""

from __future__ import annotations

import asyncio
import csv
import json
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass
class ExportResult:
    """Metadata returned after a file-download export completes."""

    path: str
    rows: int
    complete: bool
    format: str


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

    @property
    def complete(self) -> bool:
        """Whether the export completed successfully (sentinel found for JSONL, always True for CSV)."""
        return self._complete

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
        for line in self._response.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            if "__export_complete" in data:
                self._complete = True
                continue
            yield data

    def _iterate_csv(self) -> Iterator[dict[str, Any]]:
        reader = csv.DictReader(self._response.iter_lines())
        for row in reader:
            yield dict(row)
        self._complete = True


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

    @property
    def complete(self) -> bool:
        """Whether the export completed successfully."""
        return self._complete

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
        async for line in self._response.aiter_lines():
            if not line:
                continue
            data = json.loads(line)
            if "__export_complete" in data:
                self._complete = True
                continue
            yield data

    async def _iterate_csv(self) -> AsyncIterator[dict[str, Any]]:
        lines: list[str] = []
        async for line in self._response.aiter_lines():
            lines.append(line)
        reader = csv.DictReader(lines)
        for row in reader:
            yield dict(row)
        self._complete = True


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


def _write_to_file_sync(response: Any, output: str, fmt: str) -> ExportResult:
    """Stream a response to a file synchronously, returning export metadata."""
    rows = 0
    complete = False
    try:
        with open(output, "w", newline="") as f:
            if fmt == "csv":
                for line in response.iter_lines():
                    f.write(line + "\n")
                    rows += 1
                # Subtract 1 for the header row
                rows = max(rows - 1, 0)
                complete = True
            else:
                for line in response.iter_lines():
                    if not line:
                        continue
                    data = json.loads(line)
                    if "__export_complete" in data:
                        complete = True
                        continue
                    f.write(line + "\n")
                    rows += 1
    finally:
        response.close()

    from pathlib import Path

    return ExportResult(
        path=str(Path(output).resolve()),
        rows=rows,
        complete=complete,
        format=fmt,
    )


async def _write_to_file_async(response: Any, output: str, fmt: str) -> ExportResult:
    """Stream a response to a file asynchronously, returning export metadata."""
    rows = 0
    complete = False
    batch: list[str] = []
    batch_size = 100

    from pathlib import Path

    resolved = str(Path(output).resolve())

    # Create/truncate the file
    def _truncate(p: str) -> None:
        with open(p, "w", newline=""):
            pass

    await asyncio.to_thread(_truncate, resolved)

    try:
        if fmt == "csv":
            async for line in response.aiter_lines():
                batch.append(line + "\n")
                rows += 1
                if len(batch) >= batch_size:
                    chunk = "".join(batch)
                    batch.clear()
                    await asyncio.to_thread(_append_to_file, resolved, chunk)
            if batch:
                chunk = "".join(batch)
                batch.clear()
                await asyncio.to_thread(_append_to_file, resolved, chunk)
            rows = max(rows - 1, 0)  # subtract header
            complete = True
        else:
            async for line in response.aiter_lines():
                if not line:
                    continue
                data = json.loads(line)
                if "__export_complete" in data:
                    complete = True
                    continue
                batch.append(line + "\n")
                rows += 1
                if len(batch) >= batch_size:
                    chunk = "".join(batch)
                    batch.clear()
                    await asyncio.to_thread(_append_to_file, resolved, chunk)
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
    )


def _append_to_file(path: str, data: str) -> None:
    """Append data to a file (used as a thread target)."""
    with open(path, "a", newline="") as f:
        f.write(data)
