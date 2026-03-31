"""Tests for the BLS resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient
from thesma.resources.bls import Bls

BASE = "https://api.thesma.dev"

INDUSTRIES_RESPONSE = {
    "data": [
        {
            "naics_code": "52",
            "title": "Finance and Insurance",
            "level": 2,
            "parent_naics": None,
            "has_ces_data": True,
            "has_qcew_data": True,
            "has_oews_data": True,
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

INDUSTRY_DETAIL_RESPONSE = {
    "data": {
        "naics_code": "522110",
        "title": "Commercial Banking",
        "level": 6,
        "parent_naics": "52211",
        "has_ces_data": True,
        "has_qcew_data": True,
        "has_oews_data": True,
        "children": [{"naics_code": "522111", "title": "Sub-sector", "level": 7}],
        "data_availability": {
            "ces_years": {"min": 2010, "max": 2026},
            "qcew_years": {"min": 2010, "max": 2025},
            "oews_years": {"min": 2015, "max": 2024},
        },
    }
}

EMPLOYMENT_RESPONSE = {
    "data": [
        {
            "year": 2024,
            "month": 1,
            "period": "2024-01",
            "all_employees_thousands": 2100.5,
            "avg_hourly_earnings": 35.20,
            "avg_weekly_earnings": 1408.0,
            "adjustment": "sa",
            "footnote_code": None,
            "naics_code": "522110",
            "match_precision": "exact",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

EMPLOYMENT_LATEST_RESPONSE = {
    "data": {
        "year": 2026,
        "month": 2,
        "period": "2026-02",
        "all_employees_thousands": 2150.3,
        "avg_hourly_earnings": 36.50,
        "avg_weekly_earnings": 1460.0,
        "adjustment": "sa",
        "footnote_code": None,
        "naics_code": "522110",
        "match_precision": "exact",
        "employment_yoy_pct": 2.3,
        "earnings_yoy_pct": 3.7,
    }
}


class TestIndustries:
    @respx.mock
    def test_industries_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries").mock(
            return_value=httpx.Response(200, json=INDUSTRIES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.industries()

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        assert result.data[0].naics_code == "52"
        assert result.data[0].title == "Finance and Insurance"
        client.close()

    @respx.mock
    def test_industries_with_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries").mock(
            return_value=httpx.Response(200, json=INDUSTRIES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.bls.industries(level=2, search="finance")

        assert route.called
        request = route.calls.last.request
        assert "level=2" in str(request.url)
        assert "search=finance" in str(request.url)
        client.close()


class TestIndustryDetail:
    @respx.mock
    def test_industry_detail(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries/522110").mock(
            return_value=httpx.Response(200, json=INDUSTRY_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.industry("522110")

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data.children) == 1
        assert result.data.data_availability.ces_years.min == 2010
        assert result.data.data_availability.ces_years.max == 2026
        client.close()


class TestEmployment:
    @respx.mock
    def test_employment_time_series(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries/522110/employment").mock(
            return_value=httpx.Response(200, json=EMPLOYMENT_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.employment("522110", from_date="2024-01", to_date="2024-12")

        assert route.called
        request = route.calls.last.request
        assert "from=2024-01" in str(request.url)
        assert "to=2024-12" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        assert result.data[0].all_employees_thousands == 2100.5
        client.close()

    @respx.mock
    def test_employment_with_geo(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries/522110/employment").mock(
            return_value=httpx.Response(200, json=EMPLOYMENT_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.bls.employment("522110", geo="state", state="06")

        assert route.called
        request = route.calls.last.request
        assert "geo=state" in str(request.url)
        assert "state=06" in str(request.url)
        client.close()


class TestEmploymentLatest:
    @respx.mock
    def test_employment_latest(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/industries/522110/employment/latest").mock(
            return_value=httpx.Response(200, json=EMPLOYMENT_LATEST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.employment_latest("522110")

        assert route.called
        assert isinstance(result, DataResponse)
        assert result.data.employment_yoy_pct == 2.3
        assert result.data.earnings_yoy_pct == 3.7
        client.close()


# --- SDK-14 fixtures ---

COUNTY_EMPLOYMENT_RESPONSE = {
    "data": [
        {
            "area_fips": "12086",
            "year": 2024,
            "quarter": 3,
            "industry_code": "10",
            "requested_industry": "10",
            "match_precision": "exact",
            "ownership": "private",
            "disclosure_code": None,
            "establishment_count": 85000,
            "month1_employment": 1200000,
            "month2_employment": 1210000,
            "month3_employment": 1205000,
            "employment_yoy_change": 15000,
            "employment_yoy_pct": 1.26,
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

COUNTY_WAGES_RESPONSE = {
    "data": {
        "area_fips": "12086",
        "year": 2024,
        "quarter": 3,
        "industry_code": "10",
        "requested_industry": "10",
        "match_precision": "exact",
        "ownership": "private",
        "total_quarterly_wages": 25000000000,
        "avg_weekly_wage": 1450,
        "wage_yoy_change": 50,
        "wage_yoy_pct": 3.57,
        "total_wages_yoy_change": 800000000,
        "total_wages_yoy_pct": 3.31,
        "location_quotient_employment": 1.05,
        "location_quotient_wages": 0.98,
        "location_quotient_establishments": 1.02,
    }
}

OCCUPATIONS_RESPONSE = {
    "data": [{"soc_code": "15-1252", "title": "Software Developers", "major_group": "15-0000", "is_detailed": True}],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

OCCUPATION_DETAIL_RESPONSE = {
    "data": {
        "soc_code": "15-1252",
        "title": "Software Developers",
        "major_group": "15-0000",
        "is_detailed": True,
        "data_availability": {
            "years": {"min": 2022, "max": 2023},
            "geographies": ["national", "state", "metro"],
            "industry_count": 45,
        },
        "related_occupations": [{"soc_code": "15-1253", "title": "Software Quality Assurance Analysts and Testers"}],
    }
}

OCCUPATION_WAGES_RESPONSE = {
    "data": [
        {
            "soc_code": "11-1011",
            "soc_title": "Chief Executives",
            "naics_code": "522110",
            "naics_title": "Commercial Banking",
            "area_code": "0000000",
            "area_name": "U.S.",
            "area_type": "national",
            "reference_year": 2023,
            "employment": 200540,
            "mean_hourly_wage": 115.22,
            "mean_annual_wage": 239660,
            "median_hourly_wage": 99.50,
            "median_annual_wage": 206980,
            "pct10_hourly": 45.50,
            "pct10_annual": 94640,
            "pct25_hourly": 68.20,
            "pct25_annual": 141860,
            "pct75_hourly": None,
            "pct75_annual": None,
            "pct90_hourly": None,
            "pct90_annual": None,
            "suppressed": False,
            "wage_censored_high": True,
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

METRICS_RESPONSE = {
    "data": [
        {
            "canonical_name": "total_employment",
            "display_name": "Total Employment",
            "description": "Total nonfarm employment",
            "category": "employment",
            "unit": "persons",
            "source_dataset": "ces",
            "update_cadence": "monthly",
            "typical_lag_months": 1,
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

METRIC_DETAIL_RESPONSE = {
    "data": {
        "canonical_name": "total_employment",
        "display_name": "Total Employment",
        "description": "Total nonfarm employment",
        "category": "employment",
        "unit": "persons",
        "source_dataset": "ces",
        "update_cadence": "monthly",
        "typical_lag_months": 1,
        "data_availability": {"min": 2020, "max": 2025},
        "related_endpoints": ["/v1/us/bls/industries/{naics}/employment"],
    }
}


# --- SDK-14 tests ---


class TestCountyEmployment:
    @respx.mock
    def test_county_employment(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/counties/12086/employment").mock(
            return_value=httpx.Response(200, json=COUNTY_EMPLOYMENT_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.county_employment("12086")

        assert route.called
        assert "12086" in str(route.calls.last.request.url)
        assert result.data[0].area_fips == "12086"
        assert result.pagination.total == 1
        client.close()

    @respx.mock
    def test_county_employment_with_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/counties/12086/employment").mock(
            return_value=httpx.Response(200, json=COUNTY_EMPLOYMENT_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.bls.county_employment("12086", industry="31-33", ownership="private", year=2024, quarter=3)

        request = route.calls.last.request
        url_str = str(request.url)
        assert "industry=31-33" in url_str
        assert "ownership=private" in url_str
        assert "year=2024" in url_str
        assert "quarter=3" in url_str
        client.close()


class TestCountyWages:
    @respx.mock
    def test_county_wages(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/counties/12086/wages").mock(
            return_value=httpx.Response(200, json=COUNTY_WAGES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.county_wages("12086")

        assert route.called
        assert isinstance(result, DataResponse)
        assert result.data.avg_weekly_wage == 1450
        client.close()


class TestOccupations:
    @respx.mock
    def test_occupations_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/occupations").mock(
            return_value=httpx.Response(200, json=OCCUPATIONS_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.occupations(search="software", group="detailed")

        assert route.called
        request = route.calls.last.request
        url_str = str(request.url)
        assert "search=software" in url_str
        assert "group=detailed" in url_str
        assert result.data[0].soc_code == "15-1252"
        client.close()

    @respx.mock
    def test_occupation_detail(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/occupations/15-1252").mock(
            return_value=httpx.Response(200, json=OCCUPATION_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.occupation("15-1252")

        assert route.called
        assert "15-1252" in str(route.calls.last.request.url)
        assert len(result.data.related_occupations) == 1
        client.close()


class TestOccupationWages:
    @respx.mock
    def test_occupation_wages(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/occupations/11-1011/wages").mock(
            return_value=httpx.Response(200, json=OCCUPATION_WAGES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.occupation_wages("11-1011", geo="national")

        assert route.called
        assert "11-1011" in str(route.calls.last.request.url)
        assert "geo=national" in str(route.calls.last.request.url)
        assert result.data[0].median_annual_wage == 206980
        client.close()

    @respx.mock
    def test_occupation_wages_with_industry(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/occupations/11-1011/wages").mock(
            return_value=httpx.Response(200, json=OCCUPATION_WAGES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.bls.occupation_wages("11-1011", industry="522110", state="36")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "industry=522110" in url_str
        assert "state=36" in url_str
        client.close()


class TestMetrics:
    @respx.mock
    def test_metrics_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/metrics").mock(
            return_value=httpx.Response(200, json=METRICS_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.metrics(category="employment", source="ces")

        assert route.called
        request = route.calls.last.request
        url_str = str(request.url)
        assert "category=employment" in url_str
        assert "source=ces" in url_str
        assert result.data[0].canonical_name == "total_employment"
        client.close()

    @respx.mock
    def test_metric_detail(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/bls/metrics/total_employment:ces").mock(
            return_value=httpx.Response(200, json=METRIC_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.bls.metric("total_employment:ces")

        assert route.called
        assert "total_employment:ces" in str(route.calls.last.request.url)
        assert result.data.data_availability is not None
        assert result.data.data_availability.min == 2020
        client.close()


class TestBlsRegistered:
    def test_bls_registered_on_client(self, api_key: str) -> None:
        client = ThesmaClient(api_key=api_key)
        assert hasattr(client, "bls")
        assert isinstance(client.bls, Bls)
        client.close()
