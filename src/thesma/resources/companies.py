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
        taxonomy: str | None = None,
        currency: str | None = None,
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

        ``taxonomy`` filters by the filing taxonomy of the company's
        most-recent parsed statement — accepted values are ``"us-gaap"``
        and ``"ifrs-full"``; other inputs return 400 as
        ``BadRequestError``. ``currency`` filters by the presentation
        currency of that same statement — case-insensitive 3-letter
        ISO-4217 code (``"USD"``, ``"EUR"``, ``"JPY"``…); unknown codes
        return 400. Companies with no parsed financials are excluded
        from filtered results on either filter.

        ``search`` filters by company name substring OR ticker prefix,
        case-insensitive. The value is passed through to the API
        verbatim — the server trims whitespace, escapes SQL LIKE
        wildcards, and silently skips the ticker branch for companies
        with a null ticker. Known v1 limitations inherited from the
        API: separators differ between the filing record and common
        market conventions (``BRK-B`` on EDGAR vs ``BRK.B`` on Yahoo
        Finance) so matches are separator-sensitive, and there is no
        ticker alias resolution.
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
            "taxonomy": taxonomy,
            "currency": currency,
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

        ``include`` is a comma-separated list of enrichment surfaces. The
        supported values are ``"labor_context"`` and ``"lending_context"``
        (e.g. ``include="lending_context"`` or
        ``include="labor_context,lending_context"``). Unknown values
        return 400 as ``BadRequestError``.

        When ``include="lending_context"`` is requested, the response's
        ``data.lending_context`` field carries a ``LendingContext`` object
        with ``local_market`` and ``industry_lending`` sub-objects. Three
        states are possible: the key is omitted entirely when the company
        has no ``county_fips`` mapping, both sub-objects are ``None`` when
        FIPS exists but no SBA data is available, or one or both are
        populated. Consumers can distinguish omitted vs null-children via
        ``result.data.model_dump(exclude_unset=True)``.
        """
        params: dict[str, Any] = {"include": include}
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}",
            params=params,
            response_model=DataResponse[EnrichedCompanyData],
        )
