"""BLS resource — US Bureau of Labor Statistics labor market data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    BlsIndustryDetail,
    BlsIndustrySummary,
    BlsMetricDetail,
    BlsMetricSummary,
    CesEmploymentLatest,
    CesEmploymentPoint,
    OccupationDetail,
    OccupationSummary,
    OccupationWages,
    QcewEmployment,
    QcewWages,
)
from thesma._types import DataResponse, PaginatedResponse


class Bls:
    """Resource for ``/v1/us/bls`` endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def industries(
        self,
        *,
        level: int | None = None,
        search: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[BlsIndustrySummary]:
        """List or search NAICS industries.

        ``GET /v1/us/bls/industries``
        """
        params = {"level": level, "search": search, "page": page, "per_page": per_page}
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/industries",
            params=params,
            response_model=PaginatedResponse[BlsIndustrySummary],
        )

    def industry(self, naics: str) -> DataResponse[BlsIndustryDetail]:
        """Get industry detail by NAICS code.

        ``GET /v1/us/bls/industries/{naics}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/industries/{naics}",
            response_model=DataResponse[BlsIndustryDetail],
        )

    def employment(
        self,
        naics: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        geo: str = "national",
        state: str | None = None,
        metro: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[CesEmploymentPoint]:
        """Get CES employment time series for an industry.

        ``GET /v1/us/bls/industries/{naics}/employment``
        """
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "geo": geo,
            "state": state,
            "metro": metro,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/industries/{naics}/employment",
            params=params,
            response_model=PaginatedResponse[CesEmploymentPoint],
        )

    def employment_latest(
        self,
        naics: str,
        *,
        adjustment: str = "sa",
        geo: str = "national",
        state: str | None = None,
        metro: str | None = None,
    ) -> DataResponse[CesEmploymentLatest]:
        """Get latest CES employment data for an industry.

        ``GET /v1/us/bls/industries/{naics}/employment/latest``
        """
        params = {
            "adjustment": adjustment,
            "geo": geo,
            "state": state,
            "metro": metro,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/industries/{naics}/employment/latest",
            params=params,
            response_model=DataResponse[CesEmploymentLatest],
        )

    # --- County data (QCEW) ---

    def county_employment(
        self,
        fips: str,
        *,
        industry: str = "10",
        ownership: str = "private",
        year: int | None = None,
        quarter: int | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[QcewEmployment]:
        """Get quarterly county employment data.

        ``GET /v1/us/bls/counties/{fips}/employment``
        """
        params = {
            "industry": industry,
            "ownership": ownership,
            "year": year,
            "quarter": quarter,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/counties/{fips}/employment",
            params=params,
            response_model=PaginatedResponse[QcewEmployment],
        )

    def county_wages(
        self,
        fips: str,
        *,
        industry: str = "10",
        ownership: str = "private",
        year: int | None = None,
        quarter: int | None = None,
    ) -> DataResponse[QcewWages]:
        """Get latest-quarter county wage snapshot.

        ``GET /v1/us/bls/counties/{fips}/wages``
        """
        params = {
            "industry": industry,
            "ownership": ownership,
            "year": year,
            "quarter": quarter,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/counties/{fips}/wages",
            params=params,
            response_model=DataResponse[QcewWages],
        )

    # --- Occupation data (OEWS) ---

    def occupations(
        self,
        *,
        search: str | None = None,
        group: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[OccupationSummary]:
        """List or search SOC occupations.

        ``GET /v1/us/bls/occupations``
        """
        params = {"search": search, "group": group, "page": page, "per_page": per_page}
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/occupations",
            params=params,
            response_model=PaginatedResponse[OccupationSummary],
        )

    def occupation(self, soc: str) -> DataResponse[OccupationDetail]:
        """Get occupation detail by SOC code.

        ``GET /v1/us/bls/occupations/{soc}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/occupations/{soc}",
            response_model=DataResponse[OccupationDetail],
        )

    def occupation_wages(
        self,
        soc: str,
        *,
        industry: str | None = None,
        geo: str = "national",
        state: str | None = None,
        metro: str | None = None,
        year: int | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[OccupationWages]:
        """Get OEWS wage data for an occupation.

        ``GET /v1/us/bls/occupations/{soc}/wages``
        """
        params = {
            "industry": industry,
            "geo": geo,
            "state": state,
            "metro": metro,
            "year": year,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/occupations/{soc}/wages",
            params=params,
            response_model=PaginatedResponse[OccupationWages],
        )

    # --- Metrics (reference data) ---

    def metrics(
        self,
        *,
        category: str | None = None,
        source: str | None = None,
        search: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[BlsMetricSummary]:
        """List available BLS metrics.

        ``GET /v1/us/bls/metrics``
        """
        params = {
            "category": category,
            "source": source,
            "search": search,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/metrics",
            params=params,
            response_model=PaginatedResponse[BlsMetricSummary],
        )

    def metric(self, metric: str) -> DataResponse[BlsMetricDetail]:
        """Get metric detail by composite key.

        ``GET /v1/us/bls/metrics/{metric}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/metrics/{metric}",
            response_model=DataResponse[BlsMetricDetail],
        )
