"""Institutional holdings resource — 13F holders, funds, and position changes."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    CompanyPositionChange,
    FundHoldingListItem,
    FundListItem,
    FundPositionChange,
    HolderListItem,
)
from thesma._types import PaginatedResponse


class Holdings:
    """Resource for institutional holdings endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def holders(
        self,
        cik: str,
        *,
        quarter: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[HolderListItem]:
        """List institutional holders for a company.

        ``GET /v1/us/sec/companies/{cik}/institutional-holders``
        """
        params: dict[str, Any] = {
            "quarter": quarter,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/institutional-holders",
            params=params,
            response_model=PaginatedResponse[HolderListItem],
        )

    def holder_changes(
        self,
        cik: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[CompanyPositionChange]:
        """List institutional position changes for a company.

        ``GET /v1/us/sec/companies/{cik}/institutional-changes``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/institutional-changes",
            params=params,
            response_model=PaginatedResponse[CompanyPositionChange],
        )

    def funds(
        self,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FundListItem]:
        """List institutional funds.

        ``GET /v1/us/sec/funds``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/funds",
            params=params,
            response_model=PaginatedResponse[FundListItem],
        )

    def fund_holdings(
        self,
        cik: str,
        *,
        quarter: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FundHoldingListItem]:
        """List holdings for a specific fund.

        ``GET /v1/us/sec/funds/{cik}/holdings``
        """
        params: dict[str, Any] = {
            "quarter": quarter,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/funds/{cik}/holdings",
            params=params,
            response_model=PaginatedResponse[FundHoldingListItem],
        )

    def fund_changes(
        self,
        cik: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[FundPositionChange]:
        """List position changes for a specific fund.

        ``GET /v1/us/sec/funds/{cik}/holding-changes``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/funds/{cik}/holding-changes",
            params=params,
            response_model=PaginatedResponse[FundPositionChange],
        )
