"""SBA resource — US Small Business Administration 7(a) loan program data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    CharacteristicsDistribution,
    CountyLendingPoint,
    IndustryLendingPoint,
    LenderDetail,
    LenderSummary,
    SbaMetricDetail,
    SbaMetricSummary,
    StateLendingPoint,
    VintageOutcomePoint,
)
from thesma._types import DataResponse, PaginatedResponse


class Sba:
    """Resource for ``/v1/us/sba`` endpoints.

    Quarter-based filters across this namespace use ``YYYY-Qq`` period
    strings (e.g. ``"2024-Q3"``). The SDK exposes them as
    ``from_period`` / ``to_period`` Python kwargs that translate to the
    API's literal ``from`` / ``to`` query params.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def county_lending(
        self,
        fips: str,
        *,
        industry: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        from_period: str | None = None,
        to_period: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[CountyLendingPoint]:
        """Get quarterly SBA 7(a) lending aggregates for a county.

        ``GET /v1/us/sba/counties/{fips}/lending``
        """
        params: dict[str, Any] = {
            "industry": industry,
            "year": year,
            "quarter": quarter,
            "from": from_period,
            "to": to_period,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sba/counties/{fips}/lending",
            params=params,
            response_model=PaginatedResponse[CountyLendingPoint],
        )

    def state_lending(
        self,
        fips: str,
        *,
        industry: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        from_period: str | None = None,
        to_period: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[StateLendingPoint]:
        """Get quarterly SBA 7(a) lending aggregates for a state.

        ``GET /v1/us/sba/states/{fips}/lending``
        """
        params: dict[str, Any] = {
            "industry": industry,
            "year": year,
            "quarter": quarter,
            "from": from_period,
            "to": to_period,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sba/states/{fips}/lending",
            params=params,
            response_model=PaginatedResponse[StateLendingPoint],
        )

    def industry_lending(
        self,
        naics: str,
        *,
        geo: str | None = None,
        state: str | None = None,
        county: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        from_period: str | None = None,
        to_period: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[IndustryLendingPoint]:
        """Get quarterly SBA 7(a) lending aggregates for a NAICS industry.

        ``GET /v1/us/sba/industries/{naics}/lending``

        ``geo`` is one of ``"national"``, ``"state"``, or ``"county"``.
        When omitted, the API defaults to ``"national"``. Direct Python
        callers must pass lowercase strings; unknown values propagate as
        ``BadRequestError`` from the API. ``state`` and ``county`` are
        required when ``geo`` is ``"state"`` or ``"county"`` respectively.
        """
        params: dict[str, Any] = {
            "geo": geo,
            "state": state,
            "county": county,
            "year": year,
            "quarter": quarter,
            "from": from_period,
            "to": to_period,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sba/industries/{naics}/lending",
            params=params,
            response_model=PaginatedResponse[IndustryLendingPoint],
        )

    def lenders(
        self,
        *,
        state: str | None = None,
        county: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: int | None = None,
        from_period: str | None = None,
        to_period: str | None = None,
        sort: str = "loan_count",
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[LenderSummary]:
        """List SBA 7(a) lenders ranked by activity.

        ``GET /v1/us/sba/lenders``

        ``sort`` is one of ``"loan_count"`` (default), ``"total_amount"``,
        or ``"avg_amount"``. Unknown values propagate as ``BadRequestError``.
        """
        params: dict[str, Any] = {
            "state": state,
            "county": county,
            "industry": industry,
            "year": year,
            "quarter": quarter,
            "from": from_period,
            "to": to_period,
            "sort": sort,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sba/lenders",
            params=params,
            response_model=PaginatedResponse[LenderSummary],
        )

    def lender(
        self,
        lender_id: int,
        *,
        from_period: str | None = None,
        to_period: str | None = None,
    ) -> DataResponse[LenderDetail]:
        """Get detail and quarterly history for a single SBA lender.

        ``GET /v1/us/sba/lenders/{lender_id}``
        """
        params: dict[str, Any] = {
            "from": from_period,
            "to": to_period,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sba/lenders/{lender_id}",
            params=params,
            response_model=DataResponse[LenderDetail],
        )

    def lending_characteristics(
        self,
        *,
        year: int | None = None,
        quarter: int | None = None,
        state: str | None = None,
        county: str | None = None,
        industry: str | None = None,
    ) -> DataResponse[CharacteristicsDistribution]:
        """Get distributional breakdown of SBA 7(a) loans for one quarter.

        ``GET /v1/us/sba/lending/characteristics``

        ``year`` and ``quarter`` are required by the API; omitting either
        returns 400 as ``BadRequestError``.
        """
        params: dict[str, Any] = {
            "year": year,
            "quarter": quarter,
            "state": state,
            "county": county,
            "industry": industry,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sba/lending/characteristics",
            params=params,
            response_model=DataResponse[CharacteristicsDistribution],
        )

    def lending_outcomes(
        self,
        *,
        vintage_from: int | None = None,
        vintage_to: int | None = None,
        state: str | None = None,
        county: str | None = None,
        industry: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[VintageOutcomePoint]:
        """Get vintage-level charge-off outcomes for SBA 7(a) loans.

        ``GET /v1/us/sba/lending/outcomes``

        ``vintage_from`` is required by the API; omitting it returns 400
        as ``BadRequestError``. The API caps the vintage range at 10 years.
        """
        params: dict[str, Any] = {
            "vintage_from": vintage_from,
            "vintage_to": vintage_to,
            "state": state,
            "county": county,
            "industry": industry,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sba/lending/outcomes",
            params=params,
            response_model=PaginatedResponse[VintageOutcomePoint],
        )

    def metrics(
        self,
        *,
        category: str | None = None,
        search: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[SbaMetricSummary]:
        """List SBA metric definitions.

        ``GET /v1/us/sba/metrics``

        ``category`` is one of ``"volume"``, ``"outcomes"``, or
        ``"characteristics"``. ``search`` requires a minimum of 2 chars;
        shorter values propagate as ``BadRequestError``.
        """
        params: dict[str, Any] = {
            "category": category,
            "search": search,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sba/metrics",
            params=params,
            response_model=PaginatedResponse[SbaMetricSummary],
        )

    def metric(self, metric: str) -> DataResponse[SbaMetricDetail]:
        """Get detail for a single SBA metric by canonical name.

        ``GET /v1/us/sba/metrics/{metric}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sba/metrics/{metric}",
            response_model=DataResponse[SbaMetricDetail],
        )
