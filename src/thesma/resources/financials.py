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
    ) -> DataResponse[FinancialStatementResponse]:
        """Get a financial statement for a company.

        ``GET /v1/us/sec/companies/{cik}/financials``
        """
        params: dict[str, Any] = {
            "statement": statement,
            "period": period,
            "year": year,
            "quarter": quarter,
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
