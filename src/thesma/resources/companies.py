"""Companies resource — list and get SEC-registered companies."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import CompanyListItem, EnrichedCompanyData
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
        exchange: str | list[str] | None = None,
        domicile: str | None = None,
        state_fips: str | None = None,
        county_fips: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[CompanyListItem]:
        """List companies with optional filters.

        ``GET /v1/us/sec/companies``

        ``exchange`` accepts a single string (``"nyse"``) or a list
        (``["nyse", "nasdaq"]``) mirroring the ``sic`` parameter shape.
        ``domicile`` is a single value (``"us"`` or ``"adr"``). Both
        filters are case-insensitive — the API normalises case before
        querying, so callers may pass any case.
        """
        if isinstance(exchange, list) and not exchange:
            exchange = None
        params: dict[str, Any] = {
            "ticker": ticker,
            "search": search,
            "sic": sic,
            "tier": tier,
            "exchange": exchange,
            "domicile": domicile,
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

    def get(self, cik: str, *, include: str | None = None) -> DataResponse[EnrichedCompanyData]:
        """Get a single company by CIK.

        ``GET /v1/us/sec/companies/{cik}``
        """
        params: dict[str, Any] = {"include": include}
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}",
            params=params,
            response_model=DataResponse[EnrichedCompanyData],
        )
