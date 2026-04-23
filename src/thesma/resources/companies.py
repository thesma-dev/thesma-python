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
        include: str | None = None,
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
            "include": include,
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

        ``include`` is a comma-separated list of enrichment / expansion
        surfaces. Post-S1 the accepted set is 9 values (up from 2):
        ``"labor_context"``, ``"lending_context"``, ``"financials"``,
        ``"ratios"``, ``"events"``, ``"insider_trades"``, ``"holders"``,
        ``"compensation"``, ``"board"``. Unknown values return 400 as
        ``BadRequestError``; there is no ``"all"`` shortcut — compose the
        explicit list the caller wants.

        .. note::

           ``include="events"`` is disabled in this SDK release (the API
           returns 400 at dispatch pending the B5 latency fix). Both
           ``include="events"`` and combinations that include ``events``
           (e.g. ``include="financials,events"``) surface as
           ``BadRequestError``. A follow-up SDK release will enable the
           expander once the API flips its ``enabled`` flag.

        Each requested expander returns one of three shapes in its
        response slot (accessed via ``model_extra`` because the regenerated
        ``EnrichedCompanyData`` is an ``extra="allow"`` passthrough):

        * **Inline payload** (dict / list) on success — e.g.
          ``result.data.model_extra["financials"]["line_items"]["revenue"]``.
        * **Partial-failure error slot** on expander timeout / upstream
          error — the slot is a dict ``{"error": {"code", "message"}}``
          while the top-level response remains 200. Check for the
          ``"error"`` key to distinguish from success.
        * **HATEOAS link string** on slots NOT requested — e.g.
          ``result.data.events_url`` carries an absolute URL for the
          caller to follow instead (from the S4 HATEOAS fields).

        Expanders run concurrently with a 2-3s per-expander timeout; total
        response latency approximates ``max(expander)``. Partial failures
        do NOT fail the whole request.

        When ``include="labor_context"`` is requested, the response's
        ``data.labor_context`` is the unified nested ``LaborContext``
        object with the ``industry`` / ``local_market`` / ``turnover`` /
        ``compensation_benchmark`` sub-objects plus new ``summary``
        (4-field derived classification — ``industry_hiring_trend``,
        ``local_unemployment_trend``, ``comp_to_market_ratio``,
        ``labour_market_tightness``) and ``data_freshness`` (6 period
        anchors including ``oews_period`` and
        ``sec_exec_comp_snapshot_date``). Post-S3 the endpoint populates
        ``compensation_benchmark`` — pre-S3 it was always ``None``.

        When ``include="lending_context"`` is requested, the response's
        ``data.lending_context`` field carries a ``LendingContext`` object
        with ``local_market`` and ``industry_lending`` sub-objects. Three
        states are possible: the key is omitted entirely when the company
        has no ``county_fips`` mapping, both sub-objects are ``None`` when
        FIPS exists but no SBA data is available, or one or both are
        populated. Consumers can distinguish omitted vs null-children via
        ``result.data.model_dump(exclude_unset=True)``.

        When an enrichment builder (``labor_context`` / ``lending_context``)
        times out or errors, the envelope's ``_enrichment_warnings`` list
        carries a typed ``EnrichmentWarning`` (``field``, ``reason``,
        ``message``) and the context field is ``None``. Access via
        ``result.model_extra.get("_enrichment_warnings")``.
        """
        params: dict[str, Any] = {"include": include}
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}",
            params=params,
            response_model=DataResponse[EnrichedCompanyData],
        )
