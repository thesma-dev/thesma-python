"""Census resource — US Census demographic and economic data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    BreakdownResponse,
    ComparisonResponse,
    GeographyLevel,
    MetricDetail,
    MetricSummary,
    PlaceDetail,
    PlaceMetrics,
    PlaceSummary,
    TimeSeries,
)
from thesma._types import DataResponse, PaginatedResponse


class Census:
    """Resource for ``/v1/us/census`` endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def geographies(self) -> DataResponse[list[GeographyLevel]]:
        """List available geographic levels with counts.

        ``GET /v1/us/census/geographies``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/census/geographies",
            response_model=DataResponse[list[GeographyLevel]],
        )

    def geography(self, level: str) -> PaginatedResponse[PlaceSummary]:
        """List places at a geographic level.

        ``GET /v1/us/census/geographies/{level}``

        Args:
            level: Geographic level (e.g. ``"state"``, ``"county"``).
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/geographies/{level}",
            response_model=PaginatedResponse[PlaceSummary],
        )

    def geography_places(self, level: str, fips: str) -> DataResponse[PlaceDetail]:
        """Get details for a single place at a geographic level.

        ``GET /v1/us/census/geographies/{level}/{fips}``

        Args:
            level: Geographic level (e.g. ``"state"``, ``"county"``).
            fips: FIPS code as a string, including leading zeros (e.g. ``"06037"``).
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/geographies/{level}/{fips}",
            response_model=DataResponse[PlaceDetail],
        )

    def metrics(self) -> DataResponse[list[MetricSummary]]:
        """List all curated Census metrics.

        ``GET /v1/us/census/metrics``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/census/metrics",
            response_model=DataResponse[list[MetricSummary]],
        )

    def metric(self, metric: str) -> DataResponse[MetricDetail]:
        """Get detailed information for a single metric.

        ``GET /v1/us/census/metrics/{metric}``

        Args:
            metric: Metric canonical name (e.g. ``"total_population"``).
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/metrics/{metric}",
            response_model=DataResponse[MetricDetail],
        )

    def compare(
        self,
        metric: str,
        *,
        fips: list[str],
        dataset: str | None = None,
        year: int | None = None,
    ) -> ComparisonResponse:
        """Compare a metric across multiple places.

        ``GET /v1/us/census/metrics/{metric}/compare``

        Args:
            metric: Metric canonical name.
            fips: List of FIPS codes as strings, including leading zeros
                  (e.g. ``["35620", "31080"]``). Must contain at least one code.
            dataset: Dataset — ``"acs1"`` or ``"acs5"`` (server default: ``"acs5"``).
            year: Data year (defaults to latest available).

        Raises:
            ValueError: If *fips* is empty.
        """
        if len(fips) < 1:
            raise ValueError("fips must contain at least one FIPS code")
        params: dict[str, Any] = {
            "fips": fips,
            "dataset": dataset,
            "year": year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/metrics/{metric}/compare",
            params=params,
            response_model=ComparisonResponse,
        )

    def place(self, fips: str) -> DataResponse[PlaceMetrics]:
        """Get all metrics for a place.

        ``GET /v1/us/census/places/{fips}``

        Args:
            fips: FIPS code as a string, including leading zeros (e.g. ``"06037"``).
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/places/{fips}",
            response_model=DataResponse[PlaceMetrics],
        )

    def place_metric(
        self,
        fips: str,
        metric: str,
        *,
        dataset: str | None = None,
        year: int | None = None,
    ) -> DataResponse[TimeSeries]:
        """Get time series for one metric at a place.

        ``GET /v1/us/census/places/{fips}/metrics/{metric}``

        Args:
            fips: FIPS code as a string, including leading zeros (e.g. ``"06037"``).
            metric: Metric canonical name.
            dataset: Dataset — ``"acs1"`` or ``"acs5"`` (server default: ``"acs5"``).
            year: Data year (defaults to latest available).
        """
        params: dict[str, Any] = {
            "dataset": dataset,
            "year": year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/places/{fips}/metrics/{metric}",
            params=params,
            response_model=DataResponse[TimeSeries],
        )

    def breakdown(
        self,
        fips: str,
        metric: str,
        *,
        dataset: str | None = None,
        year: int | None = None,
    ) -> BreakdownResponse:
        """Get children of a place ranked by a metric.

        ``GET /v1/us/census/places/{fips}/metrics/{metric}/breakdown``

        Args:
            fips: FIPS code as a string, including leading zeros (e.g. ``"06037"``).
            metric: Metric canonical name.
            dataset: Dataset — ``"acs1"`` or ``"acs5"`` (server default: ``"acs5"``).
            year: Data year (defaults to latest available).
        """
        params: dict[str, Any] = {
            "dataset": dataset,
            "year": year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/census/places/{fips}/metrics/{metric}/breakdown",
            params=params,
            response_model=BreakdownResponse,
        )
