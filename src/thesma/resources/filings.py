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
        cik: str,
        *,
        filing_type: str | None = None,
        start_date: str | datetime.date | None = None,
        end_date: str | datetime.date | None = None,
        include_superseded: bool | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FilingListItem]:
        """List filings for a company.

        ``GET /v1/us/sec/companies/{cik}/filings``
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
            f"/v1/us/sec/companies/{cik}/filings",
            params=params,
            response_model=PaginatedResponse[FilingListItem],
        )

    def list_all(
        self,
        *,
        cik: str | None = None,
        filing_type: str | None = None,
        start_date: str | datetime.date | None = None,
        end_date: str | datetime.date | None = None,
        include_superseded: bool | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FilingListItem]:
        """List filings across all companies.

        ``GET /v1/us/sec/filings``
        """
        params: dict[str, Any] = {
            "cik": cik,
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
