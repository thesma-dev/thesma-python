"""Financials resource — statements, time series, and field reference."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    EnrichedFinancialDataResponse,
    EnrichedMultiStatementPaginatedResponse,
    EnrichedMultiStatementResponse,
    FieldsResponse,
    FinancialStatementListItem,
    TimeSeriesResponse,
)
from thesma._types import DataResponse, PaginatedResponse


class Financials:
    """Resource for ``/v1/us/sec/companies/{cik}/financials`` endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(
        self,
        cik: str,
        *,
        statement: str | None = None,
        period: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        per_page: int | None = None,
        page: int | None = None,
        include: str | None = None,
    ) -> (
        EnrichedFinancialDataResponse
        | PaginatedResponse[FinancialStatementListItem]
        | EnrichedMultiStatementResponse
        | EnrichedMultiStatementPaginatedResponse
    ):
        """Get a financial statement for a company.

        ``GET /v1/us/sec/companies/{cik}/financials``

        ``include`` is a comma-separated list of enrichment surfaces —
        ``"labor_context"`` and/or ``"lending_context"`` (e.g.
        ``include="lending_context"`` or
        ``include="labor_context,lending_context"``). The enrichment
        fields land on the returned envelope as typed attributes
        (``result.labor_context``, ``result.lending_context``) on all
        four response shapes; pre-SDK-33 they were silently dropped on
        the single-statement path.

        The returned ``data`` model also exposes typed ``taxonomy``,
        ``currency``, and ``reporting_notes`` attributes —
        ``result.data.taxonomy`` (``"us-gaap"`` / ``"ifrs-full"`` / other),
        ``result.data.currency`` (ISO 4217 code), and
        ``result.data.reporting_notes.presentation_format`` (``"by_function"``
        / ``"by_nature"`` / ``"unknown"``). The ``reporting_notes`` Python
        attribute serialises to the wire-level JSON key ``_reporting_notes``
        (leading underscore) via a Pydantic alias;
        ``model_config=ConfigDict(populate_by_name=True)`` means both
        ``FinancialStatementResponse(reporting_notes=...)`` and
        ``FinancialStatementResponse(_reporting_notes=...)`` work for
        construction. ``result.data.model_dump()`` uses the Python
        attribute name by default (Pydantic v2 ``by_alias=False``); pass
        ``by_alias=True`` to round-trip the wire-level ``_reporting_notes``
        key back to the API.

        Response-shape discriminator on ``(statement, per_page)``:

        * ``statement`` is ``None`` / ``"income"`` / ``"balance-sheet"`` /
          ``"cash-flow"`` and ``per_page`` absent → single-statement
          ``EnrichedFinancialDataResponse`` (the statement itself is in
          ``result.data``; ``labor_context`` / ``lending_context`` sit
          as envelope-root siblings).
        * Same ``statement`` values with ``per_page=N`` → paginated
          ``PaginatedResponse[FinancialStatementListItem]`` (IFRS-09),
          where ``labor_context`` / ``lending_context`` land per-element.
        * ``statement="all"`` with ``per_page`` absent →
          ``EnrichedMultiStatementResponse`` (S2 single-period).
        * ``statement="all"`` with ``per_page=N`` →
          ``EnrichedMultiStatementPaginatedResponse`` (S2 multi-period),
          where ``labor_context`` / ``lending_context`` sit at envelope
          root, NOT per-element — one snapshot for the whole response.

        ``per_page`` is mutually exclusive with ``year`` / ``quarter``;
        passing both combinations returns HTTP 400 as ``BadRequestError``.

        Post-S3, requested ``labor_context`` / ``lending_context`` surfaces
        use the unified nested ``LaborContext`` / ``LendingContext`` shapes
        (same across ``/companies``, ``/financials``, ``/screener``).
        When an enrichment builder times out (2s cap) or errors, the
        envelope's ``enrichment_warnings`` list carries typed
        ``EnrichmentWarning`` entries (``field``, ``reason``, ``message``)
        and the context field itself is ``None``. A ``None`` without a
        matching warning means the builder had no data (e.g., company has
        no NAICS mapping) — not an error:

        .. code-block:: python

           resp = client.financials.get("0000320193", statement="income",
                                        include="labor_context")
           if resp.labor_context is not None:
               print(resp.labor_context.local_market.county_fips)
           for w in resp.enrichment_warnings or []:
               print(f"{w.field}: {w.reason}")
        """
        params: dict[str, Any] = {
            "statement": statement,
            "period": period,
            "year": year,
            "quarter": quarter,
            "per_page": per_page,
            "page": page,
            "include": include,
        }
        response_model: type[Any]
        if statement == "all" and per_page is not None:
            response_model = EnrichedMultiStatementPaginatedResponse
        elif statement == "all":
            response_model = EnrichedMultiStatementResponse
        elif per_page is not None:
            response_model = PaginatedResponse[FinancialStatementListItem]
        else:
            response_model = EnrichedFinancialDataResponse
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/financials",
            params=params,
            response_model=response_model,
        )

    def time_series(
        self,
        cik: str,
        metric: str,
        *,
        period: str | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
    ) -> DataResponse[TimeSeriesResponse]:
        """Get a time series for a single financial metric.

        ``GET /v1/us/sec/companies/{cik}/financials/{metric}``
        """
        params: dict[str, Any] = {
            "period": period,
            "from": from_year,
            "to": to_year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/financials/{metric}",
            params=params,
            response_model=DataResponse[TimeSeriesResponse],
        )

    def fields(self) -> DataResponse[FieldsResponse]:
        """Get the canonical field reference grouped by statement type.

        ``GET /v1/us/sec/financials/fields``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/financials/fields",
            response_model=DataResponse[FieldsResponse],
        )
