"""Financials resource — statements, time series, and field reference."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import FieldsResponse, FinancialStatementResponse, TimeSeriesResponse
from thesma._types import DataResponse


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
        include: str | None = None,
    ) -> DataResponse[FinancialStatementResponse]:
        """Get a financial statement for a company.

        ``GET /v1/us/sec/companies/{cik}/financials``

        ``include`` is a comma-separated list of enrichment surfaces —
        ``"labor_context"`` and/or ``"lending_context"`` (e.g.
        ``include="lending_context"`` or
        ``include="labor_context,lending_context"``). The API returns the
        enrichment fields at the envelope root, but
        ``FinancialStatementResponse`` uses Pydantic ``extra="ignore"`` so
        the values are silently dropped from the parsed result today;
        hoisting them to consumer-visible attributes is tracked as
        follow-up work. The query parameter is still forwarded to the
        API.

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
        """
        params: dict[str, Any] = {
            "statement": statement,
            "period": period,
            "year": year,
            "quarter": quarter,
            "include": include,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/financials",
            params=params,
            response_model=DataResponse[FinancialStatementResponse],
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
