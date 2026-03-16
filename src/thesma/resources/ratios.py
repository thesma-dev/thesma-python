"""Ratios resource — financial ratios and ratio time series."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import RatioResponse, RatioTimeSeriesResponse
from thesma._types import DataResponse


class Ratios:
    """Resource for ``/v1/us/sec/companies/{cik}/ratios`` endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(
        self,
        cik: str,
        *,
        period: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
    ) -> DataResponse[RatioResponse]:
        """Get financial ratios for a company.

        ``GET /v1/us/sec/companies/{cik}/ratios``
        """
        params: dict[str, Any] = {
            "period": period,
            "year": year,
            "quarter": quarter,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/ratios",
            params=params,
            response_model=DataResponse[RatioResponse],
        )

    def time_series(
        self,
        cik: str,
        ratio: str,
        *,
        period: str | None = None,
        from_year: int | None = None,
        to_year: int | None = None,
    ) -> DataResponse[RatioTimeSeriesResponse]:
        """Get a time series for a single financial ratio.

        ``GET /v1/us/sec/companies/{cik}/ratios/{ratio}``
        """
        params: dict[str, Any] = {
            "period": period,
            "from": from_year,
            "to": to_year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/ratios/{ratio}",
            params=params,
            response_model=DataResponse[RatioTimeSeriesResponse],
        )
