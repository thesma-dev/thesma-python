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
    JoltsRegionTurnoverPoint,
    JoltsSizeClassPoint,
    JoltsStateTurnoverPoint,
    JoltsTurnoverLatest,
    JoltsTurnoverPoint,
    LausCountyComparisonResponse,
    LausCountyObservation,
    LausStateComparisonResponse,
    LausStateObservation,
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

    # --- Turnover data (JOLTS) ---

    def _validate_date_range(self, from_date: str | None, to_date: str | None) -> None:
        if (from_date is None) != (to_date is None):
            raise ValueError("Both from_date and to_date are required for a time series")

    def turnover(
        self,
        naics: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        measures: str | None = None,
        rate_or_level: str = "both",
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[JoltsTurnoverPoint]:
        """Get JOLTS industry turnover time series.

        ``GET /v1/us/bls/industries/{naics}/turnover``
        """
        self._validate_date_range(from_date, to_date)
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "measures": measures,
            "rate_or_level": rate_or_level,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/industries/{naics}/turnover",
            params=params,
            response_model=PaginatedResponse[JoltsTurnoverPoint],
        )

    def turnover_latest(
        self,
        naics: str,
        *,
        adjustment: str = "sa",
        measures: str | None = None,
        rate_or_level: str = "both",
    ) -> DataResponse[JoltsTurnoverLatest]:
        """Get latest JOLTS turnover observation for an industry.

        ``GET /v1/us/bls/industries/{naics}/turnover/latest``
        """
        params = {
            "adjustment": adjustment,
            "measures": measures,
            "rate_or_level": rate_or_level,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/industries/{naics}/turnover/latest",
            params=params,
            response_model=DataResponse[JoltsTurnoverLatest],
        )

    def state_turnover(
        self,
        fips: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        measures: str | None = None,
        rate_or_level: str = "both",
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[JoltsStateTurnoverPoint]:
        """Get state-level JOLTS turnover data.

        ``GET /v1/us/bls/states/{fips}/turnover``
        """
        self._validate_date_range(from_date, to_date)
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "measures": measures,
            "rate_or_level": rate_or_level,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/states/{fips}/turnover",
            params=params,
            response_model=PaginatedResponse[JoltsStateTurnoverPoint],
        )

    def regional_turnover(
        self,
        region: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        measures: str | None = None,
        rate_or_level: str = "both",
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[JoltsRegionTurnoverPoint]:
        """Get regional JOLTS turnover data.

        ``GET /v1/us/bls/regions/{region}/turnover``
        """
        self._validate_date_range(from_date, to_date)
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "measures": measures,
            "rate_or_level": rate_or_level,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/regions/{region}/turnover",
            params=params,
            response_model=PaginatedResponse[JoltsRegionTurnoverPoint],
        )

    def turnover_by_size(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        measures: str | None = None,
        rate_or_level: str = "both",
        size: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[JoltsSizeClassPoint]:
        """Get national JOLTS turnover by establishment size class.

        ``GET /v1/us/bls/turnover/by-size``
        """
        self._validate_date_range(from_date, to_date)
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "measures": measures,
            "rate_or_level": rate_or_level,
            "size": size,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/turnover/by-size",
            params=params,
            response_model=PaginatedResponse[JoltsSizeClassPoint],
        )

    # --- Unemployment data (LAUS) ---

    def county_unemployment(
        self,
        fips: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        annual_only: bool = False,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[LausCountyObservation]:
        """Get monthly LAUS unemployment time series for a single county.

        ``GET /v1/us/bls/counties/{fips}/unemployment``

        County LAUS data is never seasonally adjusted — there is no
        ``adjustment`` parameter. When ``annual_only`` is ``True`` the API
        returns only M13 annual averages and the month component of the
        ``from_date``/``to_date`` filters is ignored. ``annual_only`` is
        always sent on the wire (``false`` by default) because ``False``
        is a user-meaningful explicit value.
        """
        params = {
            "from": from_date,
            "to": to_date,
            "annual_only": annual_only,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/counties/{fips}/unemployment",
            params=params,
            response_model=PaginatedResponse[LausCountyObservation],
        )

    def county_unemployment_compare(
        self,
        fips: list[str],
        *,
        year: int | None = None,
        month: int | None = None,
    ) -> LausCountyComparisonResponse:
        """Compare unemployment metrics across up to 10 counties.

        ``GET /v1/us/bls/counties/compare``

        The SDK joins ``fips`` with commas before calling the API. When
        ``year`` and ``month`` are both omitted the API resolves the
        latest period for which data is available; supplying exactly one
        of the two returns 400.

        Raises:
            ValueError: if ``fips`` is an empty list.
        """
        if not fips:
            raise ValueError("fips list must not be empty")
        params = {
            "fips": ",".join(f.strip() for f in fips),
            "year": year,
            "month": month,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/counties/compare",
            params=params,
            response_model=LausCountyComparisonResponse,
        )

    def state_unemployment(
        self,
        fips: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        adjustment: str = "sa",
        annual_only: bool = False,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[LausStateObservation]:
        """Get monthly LAUS unemployment time series for a single state.

        ``GET /v1/us/bls/states/{fips}/unemployment``

        ``adjustment`` is a pass-through string — valid values are
        ``"sa"`` (default) or ``"nsa"``; the API rejects anything else
        with a 400. When ``annual_only`` is ``True`` the API returns only
        M13 annual averages and the month component of the date filters
        is ignored. ``annual_only`` is always sent on the wire.
        """
        params = {
            "from": from_date,
            "to": to_date,
            "adjustment": adjustment,
            "annual_only": annual_only,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/bls/states/{fips}/unemployment",
            params=params,
            response_model=PaginatedResponse[LausStateObservation],
        )

    def state_unemployment_compare(
        self,
        fips: list[str],
        *,
        year: int | None = None,
        month: int | None = None,
        adjustment: str = "sa",
    ) -> LausStateComparisonResponse:
        """Compare unemployment metrics across up to 10 states.

        ``GET /v1/us/bls/states/compare``

        The SDK joins ``fips`` with commas before calling the API. When
        ``year`` and ``month`` are both omitted the API resolves the
        latest period for which data is available; supplying exactly one
        of the two returns 400. ``adjustment`` selects the SA or NSA
        national benchmark to match the requested series.

        Raises:
            ValueError: if ``fips`` is an empty list.
        """
        if not fips:
            raise ValueError("fips list must not be empty")
        params = {
            "fips": ",".join(f.strip() for f in fips),
            "year": year,
            "month": month,
            "adjustment": adjustment,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/bls/states/compare",
            params=params,
            response_model=LausStateComparisonResponse,
        )
