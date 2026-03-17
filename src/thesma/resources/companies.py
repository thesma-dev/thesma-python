"""Companies resource — list and get SEC-registered companies."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import CompanyListItem, CompanyResponse
from thesma._types import DataResponse, PaginatedResponse


class Companies:
    """Resource for ``/v1/us/sec/companies`` endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        *,
        ticker: str | None = None,
        search: str | None = None,
        sic: str | list[str] | None = None,
        tier: str | None = None,
        state_fips: str | None = None,
        county_fips: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[CompanyListItem]:
        """List companies with optional filters.

        ``GET /v1/us/sec/companies``
        """
        params: dict[str, Any] = {
            "ticker": ticker,
            "search": search,
            "sic": sic,
            "tier": tier,
            "state_fips": state_fips,
            "county_fips": county_fips,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/companies",
            params=params,
            response_model=PaginatedResponse[CompanyListItem],
        )

    def get(self, cik: str) -> DataResponse[CompanyResponse]:
        """Get a single company by CIK.

        ``GET /v1/us/sec/companies/{cik}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}",
            response_model=DataResponse[CompanyResponse],
        )
