"""Tests for the Census resource."""

from __future__ import annotations

import httpx
import pytest
import respx

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
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

GEOGRAPHIES_JSON = {
    "data": [
        {"level": "state", "count": 52},
        {"level": "county", "count": 3221},
    ],
}

GEOGRAPHY_PLACES_JSON = {
    "data": [
        {"fips": "06", "name": "California", "level": "state", "parent_fips": None, "population": 39538223},
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 52, "total_pages": 3},
}

GEOGRAPHY_PLACE_DETAIL_JSON = {
    "data": {
        "fips": "06",
        "name": "California",
        "level": "state",
        "parent_fips": None,
        "parent_name": None,
        "population": 39538223,
        "area_sq_mi": 163695.0,
        "lat": 36.778,
        "lon": -119.418,
        "children_levels": ["county", "metro"],
    },
}

METRICS_JSON = {
    "data": [
        {
            "canonical_name": "total_population",
            "display_name": "Total Population",
            "category": "demographics",
            "unit": "people",
            "is_computed": False,
            "notes": None,
            "latest_year": {"acs1": 2023, "acs5": 2022},
        },
    ],
}

METRIC_DETAIL_JSON = {
    "data": {
        "canonical_name": "total_population",
        "display_name": "Total Population",
        "category": "demographics",
        "unit": "people",
        "is_computed": False,
        "moe_formula_type": None,
        "notes": None,
        "latest_year": {"acs1": 2023, "acs5": 2022},
        "source_variables": [],
    },
}

COMPARE_JSON = {
    "data": {
        "metric": {
            "canonical_name": "median_income",
            "display_name": "Median Household Income",
            "category": "income",
            "unit": "dollars",
        },
        "year": 2022,
        "dataset": "acs5",
        "survey_years": None,
        "places": [
            {"fips": "35620", "name": "New York Metro", "value": 80000.0, "moe": 500.0, "suppressed": False},
            {"fips": "31080", "name": "Los Angeles Metro", "value": 75000.0, "moe": 600.0, "suppressed": False},
        ],
    },
    "pagination": {"page": 1, "per_page": 25, "total": 2, "total_pages": 1},
}

PLACE_JSON = {
    "data": {
        "fips": "06037",
        "name": "Los Angeles County",
        "level": "county",
        "year": 2022,
        "dataset": "acs5",
        "survey_years": None,
        "metrics": [
            {
                "canonical_name": "total_population",
                "display_name": "Total Population",
                "category": "demographics",
                "value": 10014009.0,
                "moe": None,
                "unit": "people",
                "suppressed": False,
            },
        ],
    },
}

PLACE_METRIC_JSON = {
    "data": {
        "fips": "06037",
        "name": "Los Angeles County",
        "metric": {
            "canonical_name": "total_population",
            "display_name": "Total Population",
            "category": "demographics",
            "unit": "people",
        },
        "dataset": "acs5",
        "series": [
            {"year": 2020, "value": 10014009.0, "moe": None, "suppressed": False, "survey_years": None},
            {"year": 2021, "value": 9829544.0, "moe": None, "suppressed": False, "survey_years": None},
        ],
    },
}

BREAKDOWN_JSON = {
    "data": {
        "parent": {"fips": "06", "name": "California", "level": "state"},
        "metric": {
            "canonical_name": "total_population",
            "display_name": "Total Population",
            "category": "demographics",
            "unit": "people",
        },
        "child_level": "county",
        "year": 2022,
        "dataset": "acs5",
        "survey_years": None,
        "places": [
            {"fips": "06037", "name": "Los Angeles County", "value": 10014009.0, "moe": None, "suppressed": False},
        ],
    },
    "pagination": {"page": 1, "per_page": 25, "total": 58, "total_pages": 3},
}


class TestCensusGeographies:
    @respx.mock
    def test_geographies_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/geographies").mock(
            return_value=httpx.Response(200, json=GEOGRAPHIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.geographies()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 2
        assert isinstance(result.data[0], GeographyLevel)
        assert result.data[0].level == "state"
        client.close()

    @respx.mock
    def test_geography_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/geographies/state").mock(
            return_value=httpx.Response(200, json=GEOGRAPHY_PLACES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.geography("state")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert isinstance(result.data[0], PlaceSummary)
        assert result.data[0].fips == "06"
        client.close()

    @respx.mock
    def test_geography_places_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/geographies/state/06").mock(
            return_value=httpx.Response(200, json=GEOGRAPHY_PLACE_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.geography_places("state", "06")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, PlaceDetail)
        assert result.data.fips == "06"
        assert result.data.children_levels == ["county", "metro"]
        client.close()


class TestCensusMetrics:
    @respx.mock
    def test_metrics_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/metrics").mock(
            return_value=httpx.Response(200, json=METRICS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.metrics()

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data) == 1
        assert isinstance(result.data[0], MetricSummary)
        assert result.data[0].canonical_name == "total_population"
        client.close()

    @respx.mock
    def test_metric_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/metrics/total_population").mock(
            return_value=httpx.Response(200, json=METRIC_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.metric("total_population")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, MetricDetail)
        assert result.data.canonical_name == "total_population"
        assert result.data.source_variables == []
        client.close()


class TestCensusCompare:
    @respx.mock
    def test_compare_sends_repeated_fips_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/metrics/median_income/compare").mock(
            return_value=httpx.Response(200, json=COMPARE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.compare("median_income", fips=["35620", "31080"])

        assert route.called
        request = route.calls.last.request
        url_str = str(request.url)
        assert "fips=35620" in url_str
        assert "fips=31080" in url_str
        assert isinstance(result, ComparisonResponse)
        assert len(result.data.places) == 2
        client.close()

    def test_compare_empty_fips_raises_value_error(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(ValueError, match="fips must contain at least one FIPS code"):
            client.census.compare("median_income", fips=[])
        client.close()

    @respx.mock
    def test_compare_with_optional_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/metrics/median_income/compare").mock(
            return_value=httpx.Response(200, json=COMPARE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.compare("median_income", fips=["35620"], dataset="acs1", year=2021)

        assert route.called
        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset=acs1" in url_str
        assert "year=2021" in url_str
        client.close()

    @respx.mock
    def test_compare_omits_none_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/metrics/median_income/compare").mock(
            return_value=httpx.Response(200, json=COMPARE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.compare("median_income", fips=["35620"])

        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset" not in url_str
        assert "year" not in url_str
        client.close()


class TestCensusPlace:
    @respx.mock
    def test_place_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037").mock(
            return_value=httpx.Response(200, json=PLACE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.place("06037")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, PlaceMetrics)
        assert result.data.fips == "06037"
        assert result.data.name == "Los Angeles County"
        client.close()


class TestCensusPlaceMetric:
    @respx.mock
    def test_place_metric_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population").mock(
            return_value=httpx.Response(200, json=PLACE_METRIC_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.place_metric("06037", "total_population")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, TimeSeries)
        assert result.data.fips == "06037"
        assert len(result.data.series) == 2
        client.close()

    @respx.mock
    def test_place_metric_with_optional_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population").mock(
            return_value=httpx.Response(200, json=PLACE_METRIC_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.place_metric("06037", "total_population", dataset="acs1", year=2020)

        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset=acs1" in url_str
        assert "year=2020" in url_str
        client.close()

    @respx.mock
    def test_place_metric_omits_none_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population").mock(
            return_value=httpx.Response(200, json=PLACE_METRIC_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.place_metric("06037", "total_population")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset" not in url_str
        assert "year" not in url_str
        client.close()


class TestCensusBreakdown:
    @respx.mock
    def test_breakdown_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population/breakdown").mock(
            return_value=httpx.Response(200, json=BREAKDOWN_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.census.breakdown("06037", "total_population")

        assert route.called
        assert isinstance(result, BreakdownResponse)
        assert result.data.child_level == "county"
        assert result.data.year == 2022
        assert result.pagination.total == 58
        client.close()

    @respx.mock
    def test_breakdown_with_optional_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population/breakdown").mock(
            return_value=httpx.Response(200, json=BREAKDOWN_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.breakdown("06037", "total_population", dataset="acs1", year=2021)

        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset=acs1" in url_str
        assert "year=2021" in url_str
        client.close()

    @respx.mock
    def test_breakdown_omits_none_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/census/places/06037/metrics/total_population/breakdown").mock(
            return_value=httpx.Response(200, json=BREAKDOWN_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.census.breakdown("06037", "total_population")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "dataset" not in url_str
        assert "year" not in url_str
        client.close()
