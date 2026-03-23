"""Export resource — bulk data export endpoints."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, overload

from thesma._export import (
    AsyncExportStream,
    ExportResult,
    ExportStream,
    _build_export_params,
    _write_to_file_async,
    _write_to_file_sync,
)


def _validate_export_args(
    output: str | None,
    cik: str | None,
    ticker: str | None,
) -> None:
    """Validate common export arguments before making an HTTP request."""
    if cik is not None and ticker is not None:
        msg = "Cannot specify both 'cik' and 'ticker' — they are mutually exclusive."
        raise ValueError(msg)
    if output is not None:
        parent = Path(output).resolve().parent
        if not parent.exists():
            msg = f"Output directory does not exist: {parent}"
            raise FileNotFoundError(msg)


class Export:
    """Synchronous resource for ``/v1/us/sec/export/`` endpoints.

    Provides methods for bulk-exporting complete datasets as JSONL or CSV.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def _export(
        self,
        path: str,
        *,
        output: str | None = None,
        fmt: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        _validate_export_args(output, cik, ticker)
        params = _build_export_params(fmt, since, cik, ticker)
        response = self._client._stream_get(path, params=params)
        if output is not None:
            return _write_to_file_sync(response, output, fmt)
        return ExportStream(response, fmt)

    @overload
    def companies(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def companies(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def companies(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all companies.

        ``GET /v1/us/sec/export/companies``

        Args:
            output: File path to write to. If provided, streams to file and returns
                :class:`ExportResult`. If omitted, returns an :class:`ExportStream`.
            format: Export format — ``"jsonl"`` (default) or ``"csv"``.
            since: Only return records after this timestamp. Accepts ISO date strings
                or Python ``datetime``/``date`` objects.
            cik: Filter by CIK.
            ticker: Filter by ticker (mutually exclusive with ``cik``).
        """
        return self._export(
            "/v1/us/sec/export/companies", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    def financials(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def financials(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def financials(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all financial data.

        ``GET /v1/us/sec/export/financials``
        """
        return self._export(
            "/v1/us/sec/export/financials", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    def insider_trades(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def insider_trades(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def insider_trades(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all insider trade data.

        ``GET /v1/us/sec/export/insider-trades``
        """
        return self._export(
            "/v1/us/sec/export/insider-trades", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    def events(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def events(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def events(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all corporate events.

        ``GET /v1/us/sec/export/events``
        """
        return self._export("/v1/us/sec/export/events", output=output, fmt=format, since=since, cik=cik, ticker=ticker)

    @overload
    def ratios(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def ratios(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def ratios(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all financial ratios.

        ``GET /v1/us/sec/export/ratios``
        """
        return self._export("/v1/us/sec/export/ratios", output=output, fmt=format, since=since, cik=cik, ticker=ticker)

    @overload
    def holdings(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def holdings(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def holdings(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all institutional holdings.

        ``GET /v1/us/sec/export/holdings``

        Args:
            output: File path to write to. If provided, returns :class:`ExportResult`.
            format: Export format — ``"jsonl"`` (default) or ``"csv"``.
            since: Only return records after this timestamp.
            cik: Filter by fund CIK (the 13F filer).
            ticker: Filter by fund ticker (the 13F filer).
        """
        return self._export(
            "/v1/us/sec/export/holdings", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    def compensation(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def compensation(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def compensation(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all executive compensation data.

        ``GET /v1/us/sec/export/compensation``
        """
        return self._export(
            "/v1/us/sec/export/compensation", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    def beneficial_ownership(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    def beneficial_ownership(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportStream: ...

    def beneficial_ownership(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | ExportStream:
        """Export all beneficial ownership data.

        ``GET /v1/us/sec/export/beneficial-ownership``
        """
        return self._export(
            "/v1/us/sec/export/beneficial-ownership", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )


class AsyncExport:
    """Asynchronous resource for ``/v1/us/sec/export/`` endpoints.

    Async variant of :class:`Export`.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def _export(
        self,
        path: str,
        *,
        output: str | None = None,
        fmt: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        _validate_export_args(output, cik, ticker)
        params = _build_export_params(fmt, since, cik, ticker)
        response = await self._client._async_stream_get(path, params=params)
        if output is not None:
            return await _write_to_file_async(response, output, fmt)
        return AsyncExportStream(response, fmt)

    @overload
    async def companies(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def companies(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def companies(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all companies.

        ``GET /v1/us/sec/export/companies``

        Args:
            output: File path to write to. If provided, streams to file and returns
                :class:`ExportResult`. If omitted, returns an :class:`AsyncExportStream`.
            format: Export format — ``"jsonl"`` (default) or ``"csv"``.
            since: Only return records after this timestamp.
            cik: Filter by CIK.
            ticker: Filter by ticker (mutually exclusive with ``cik``).
        """
        return await self._export(
            "/v1/us/sec/export/companies", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def financials(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def financials(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def financials(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all financial data.

        ``GET /v1/us/sec/export/financials``
        """
        return await self._export(
            "/v1/us/sec/export/financials", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def insider_trades(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def insider_trades(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def insider_trades(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all insider trade data.

        ``GET /v1/us/sec/export/insider-trades``
        """
        return await self._export(
            "/v1/us/sec/export/insider-trades", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def events(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def events(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def events(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all corporate events.

        ``GET /v1/us/sec/export/events``
        """
        return await self._export(
            "/v1/us/sec/export/events", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def ratios(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def ratios(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def ratios(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all financial ratios.

        ``GET /v1/us/sec/export/ratios``
        """
        return await self._export(
            "/v1/us/sec/export/ratios", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def holdings(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def holdings(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def holdings(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all institutional holdings.

        ``GET /v1/us/sec/export/holdings``

        Args:
            output: File path to write to. If provided, returns :class:`ExportResult`.
            format: Export format — ``"jsonl"`` (default) or ``"csv"``.
            since: Only return records after this timestamp.
            cik: Filter by fund CIK (the 13F filer).
            ticker: Filter by fund ticker (the 13F filer).
        """
        return await self._export(
            "/v1/us/sec/export/holdings", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def compensation(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def compensation(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def compensation(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all executive compensation data.

        ``GET /v1/us/sec/export/compensation``
        """
        return await self._export(
            "/v1/us/sec/export/compensation", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )

    @overload
    async def beneficial_ownership(
        self,
        *,
        output: str,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> ExportResult: ...

    @overload
    async def beneficial_ownership(
        self,
        *,
        output: None = ...,
        format: str = ...,
        since: str | datetime | date | None = ...,
        cik: str | None = ...,
        ticker: str | None = ...,
    ) -> AsyncExportStream: ...

    async def beneficial_ownership(
        self,
        *,
        output: str | None = None,
        format: str = "jsonl",
        since: str | datetime | date | None = None,
        cik: str | None = None,
        ticker: str | None = None,
    ) -> ExportResult | AsyncExportStream:
        """Export all beneficial ownership data.

        ``GET /v1/us/sec/export/beneficial-ownership``
        """
        return await self._export(
            "/v1/us/sec/export/beneficial-ownership", output=output, fmt=format, since=since, cik=cik, ticker=ticker
        )
