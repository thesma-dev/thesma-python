"""Filings resource — list, get, and fetch content for SEC filings."""

from __future__ import annotations

import datetime
from typing import Any

from thesma._generated.models import FilingContentResponse, FilingDetailResponse, FilingListItem
from thesma._types import DataResponse, PaginatedResponse


def _to_date_str(value: str | datetime.date | None) -> str | None:
    """Convert a date value to an ISO-format string.

    Raises :class:`TypeError` if a :class:`datetime.datetime` is passed.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        raise TypeError("Expected str or datetime.date, got datetime.datetime. Use a date object or ISO string.")
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


class Filings:
    """Resource for filing endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        identifier: str,
        *,
        filing_type: str | None = None,
        start_date: str | datetime.date | None = None,
        end_date: str | datetime.date | None = None,
        include_superseded: bool | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FilingListItem]:
        """List filings for a company.

        ``GET /v1/us/sec/companies/{identifier}/filings``

        ``identifier`` accepts a CIK or ticker (case-insensitive, with
        ``TickerAlias`` fallback for stale tickers).
        """
        params: dict[str, Any] = {
            "type": filing_type,
            "from": _to_date_str(start_date),
            "to": _to_date_str(end_date),
            "include_superseded": include_superseded,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/filings",
            params=params,
            response_model=PaginatedResponse[FilingListItem],
        )

    def list_all(
        self,
        *,
        identifier: str | None = None,
        filing_type: str | None = None,
        start_date: str | datetime.date | None = None,
        end_date: str | datetime.date | None = None,
        include_superseded: bool | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FilingListItem]:
        """List filings across all companies.

        ``GET /v1/us/sec/filings``

        :param identifier: Filter results to a single company by CIK
            (zero-padded or stripped 1-10 digits) or current ticker symbol
            (case-insensitive). Stale tickers fall back to ``TickerAlias``
            (e.g. ``"FB"`` → META). Unknown identifiers return an empty
            result set, not a 4xx — consistent with every other
            query-filter param on this route.
        """
        params: dict[str, Any] = {
            "identifier": identifier,
            "type": filing_type,
            "from": _to_date_str(start_date),
            "to": _to_date_str(end_date),
            "include_superseded": include_superseded,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/filings",
            params=params,
            response_model=PaginatedResponse[FilingListItem],
        )

    def get(self, accession_number: str) -> DataResponse[FilingDetailResponse]:
        """Get a single filing by accession number.

        ``GET /v1/us/sec/filings/{accession_number}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/filings/{accession_number}",
            response_model=DataResponse[FilingDetailResponse],
        )

    def content(self, accession_number: str) -> DataResponse[FilingContentResponse]:
        """Get the cleaned HTML content of a filing.

        ``GET /v1/us/sec/filings/{accession_number}/content``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/filings/{accession_number}/content",
            response_model=DataResponse[FilingContentResponse],
        )
