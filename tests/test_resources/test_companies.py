"""Tests for the Companies resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import EnrichedCompanyData
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_COMPANIES_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sic_code": "3571",
            "company_tier": "sp500",
            "state_fips": "06",
            "county_fips": "06073",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

COMPANY_DETAIL_JSON = {
    "data": {
        "cik": "0000320193",
        "name": "Apple Inc.",
        "ticker": "AAPL",
        "sic_code": "3571",
        "sic_description": "Electronic Computers",
        "company_tier": "sp500",
        "state_fips": "06",
        "county_fips": "06073",
        "filings_url": "/v1/us/sec/companies/0000320193/filings",
        "financials_url": "/v1/us/sec/companies/0000320193/financials",
    },
}


def _build_company_with_local_market(local_market: dict | None) -> dict:
    return {
        "data": {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sic_code": "3571",
            "sic_description": "Electronic Computers",
            "company_tier": "sp500",
            "state_fips": "06",
            "county_fips": "06085",
            "filings_url": "/v1/us/sec/companies/0000320193/filings",
            "financials_url": "/v1/us/sec/companies/0000320193/financials",
            "labor_context": {
                "industry": {
                    "naics_code": "334111",
                    "employment": 142000,
                    "employment_yoy_pct": 3.8,
                },
                "local_market": local_market,
                "compensation_benchmark": None,
            },
        },
    }


_LAUS_LOCAL_MARKET = {
    "county_fips": "06085",
    "county_name": "Santa Clara County, CA",
    "county_fips_confidence": "high",
    "industry_employment": 142000,
    "industry_employment_yoy_pct": 3.8,
    "industry_avg_weekly_wage": 3200,
    "industry_wage_yoy_pct": 4.2,
    "total_employment": 1050000,
    "total_avg_weekly_wage": 2800,
    "data_period": "2025-Q2",
    "data_lag_months": 6,
    "match_precision": "6-digit",
    "unemployment_rate": 2.8,
    "unemployment_rate_yoy_change": -0.4,
    "labor_force": 1050450,
    "labor_force_yoy_change_pct": 1.2,
    "laus_data_period": "2025-11",
    "laus_data_lag_weeks": 7,
    "match_level": "county",
    "seasonal_adjustment": "not_seasonally_adjusted",
    "source": "LAUS+QCEW",
}

COMPANY_WITH_LAUS_LOCAL_MARKET_RESPONSE = _build_company_with_local_market(_LAUS_LOCAL_MARKET)

COMPANY_WITH_QCEW_ONLY_RESPONSE = _build_company_with_local_market(
    {
        "county_fips": "06085",
        "county_name": "Santa Clara County, CA",
        "county_fips_confidence": "high",
        "industry_employment": 142000,
        "industry_employment_yoy_pct": 3.8,
        "industry_avg_weekly_wage": 3200,
        "industry_wage_yoy_pct": 4.2,
        "total_employment": 1050000,
        "total_avg_weekly_wage": 2800,
        "data_period": "2025-Q2",
        "data_lag_months": 6,
        "match_precision": "6-digit",
        "unemployment_rate": None,
        "unemployment_rate_yoy_change": None,
        "labor_force": None,
        "labor_force_yoy_change_pct": None,
        "laus_data_period": None,
        "laus_data_lag_weeks": None,
        "match_level": None,
        "seasonal_adjustment": None,
        "source": "QCEW",
    }
)

COMPANY_WITH_LAUS_ONLY_RESPONSE = _build_company_with_local_market(
    {
        "county_fips": "06085",
        "county_name": "Santa Clara County, CA",
        "county_fips_confidence": "high",
        "industry_employment": None,
        "industry_employment_yoy_pct": None,
        "industry_avg_weekly_wage": None,
        "industry_wage_yoy_pct": None,
        "total_employment": None,
        "total_avg_weekly_wage": None,
        "data_period": None,
        "data_lag_months": None,
        "match_precision": None,
        "unemployment_rate": 2.8,
        "unemployment_rate_yoy_change": -0.4,
        "labor_force": 1050450,
        "labor_force_yoy_change_pct": 1.2,
        "laus_data_period": "2025-11",
        "laus_data_lag_weeks": 7,
        "match_level": "county",
        "seasonal_adjustment": "not_seasonally_adjusted",
        "source": "LAUS",
    }
)

COMPANY_WITH_STATE_FALLBACK_RESPONSE = _build_company_with_local_market(
    {
        "county_fips": None,
        "county_name": "California",
        "county_fips_confidence": "low",
        "industry_employment": None,
        "industry_employment_yoy_pct": None,
        "industry_avg_weekly_wage": None,
        "industry_wage_yoy_pct": None,
        "total_employment": None,
        "total_avg_weekly_wage": None,
        "data_period": None,
        "data_lag_months": None,
        "match_precision": None,
        "unemployment_rate": 4.3,
        "unemployment_rate_yoy_change": 0.1,
        "labor_force": 19500000,
        "labor_force_yoy_change_pct": 0.8,
        "laus_data_period": "2025-11",
        "laus_data_lag_weeks": 7,
        "match_level": "state",
        "seasonal_adjustment": "seasonally_adjusted",
        "source": "LAUS",
    }
)

COMPANY_WITH_NO_LOCAL_MARKET_RESPONSE = _build_company_with_local_market(None)

COMPANY_WITH_NULL_YOY_RESPONSE = _build_company_with_local_market(
    {
        "county_fips": "06085",
        "county_name": "Santa Clara County, CA",
        "county_fips_confidence": "high",
        "industry_employment": 142000,
        "industry_employment_yoy_pct": 3.8,
        "industry_avg_weekly_wage": 3200,
        "industry_wage_yoy_pct": 4.2,
        "total_employment": 1050000,
        "total_avg_weekly_wage": 2800,
        "data_period": "2025-Q2",
        "data_lag_months": 6,
        "match_precision": "6-digit",
        "unemployment_rate": 2.8,
        "unemployment_rate_yoy_change": None,
        "labor_force": 1050450,
        "labor_force_yoy_change_pct": None,
        "laus_data_period": "2025-11",
        "laus_data_lag_weeks": 7,
        "match_level": "county",
        "seasonal_adjustment": "not_seasonally_adjusted",
        "source": "LAUS+QCEW",
    }
)


class TestCompaniesList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.list()

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_with_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.list(tier="sp500", page=2)

        assert route.called
        request = route.calls.last.request
        assert "tier=sp500" in str(request.url)
        assert "page=2" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.list()

        assert result.data[0].cik == "0000320193"
        assert result.data[0].ticker == "AAPL"
        assert result.pagination.total == 1
        client.close()

    @respx.mock
    def test_list_with_sic_single_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(sic="3571")

        request = route.calls.last.request
        assert "sic=3571" in str(request.url)
        client.close()

    @respx.mock
    def test_list_with_sic_multi_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(sic=["7372", "3674"])

        request = route.calls.last.request
        url_str = str(request.url)
        assert "sic=7372" in url_str
        assert "sic=3674" in url_str
        client.close()


class TestCompaniesGet:
    @respx.mock
    def test_get_sends_correct_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193")

        assert route.called
        assert isinstance(result, DataResponse)
        assert result.data.cik == "0000320193"
        assert result.data.ticker == "AAPL"
        client.close()

    @respx.mock
    def test_get_response_model_type(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193")

        assert isinstance(result.data, EnrichedCompanyData)
        client.close()


class TestCompaniesGetInclude:
    @respx.mock
    def test_get_with_include(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.get("0000320193", include="labor_context")

        request = route.calls.last.request
        assert "include=labor_context" in str(request.url)
        client.close()

    @respx.mock
    def test_get_without_include(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.get("0000320193")

        request = route.calls.last.request
        assert "include" not in str(request.url)
        client.close()


class TestCompaniesGetLausLocalMarket:
    @respx.mock
    def test_companies_get_laus_local_market_fields_present(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_LAUS_LOCAL_MARKET_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        local_market = result.data.labor_context["local_market"]  # type: ignore[attr-defined]
        assert local_market["unemployment_rate"] == 2.8
        assert local_market["match_level"] == "county"
        assert local_market["seasonal_adjustment"] == "not_seasonally_adjusted"
        assert local_market["source"] == "LAUS+QCEW"
        client.close()

    @respx.mock
    def test_companies_get_local_market_source_qcew_only(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_QCEW_ONLY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        local_market = result.data.labor_context["local_market"]  # type: ignore[attr-defined]
        assert local_market["source"] == "QCEW"
        assert local_market["unemployment_rate"] is None
        assert local_market["match_level"] is None
        client.close()

    @respx.mock
    def test_companies_get_local_market_source_laus_only(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_LAUS_ONLY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        local_market = result.data.labor_context["local_market"]  # type: ignore[attr-defined]
        assert local_market["source"] == "LAUS"
        assert local_market["industry_employment"] is None
        assert local_market["data_period"] is None
        assert local_market["data_lag_months"] is None
        assert local_market["match_precision"] is None
        assert local_market["unemployment_rate"] == 2.8
        client.close()

    @respx.mock
    def test_companies_get_state_fallback_match_level(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_STATE_FALLBACK_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        local_market = result.data.labor_context["local_market"]  # type: ignore[attr-defined]
        assert local_market["match_level"] == "state"
        assert local_market["seasonal_adjustment"] == "seasonally_adjusted"
        assert local_market["county_fips"] is None
        client.close()

    @respx.mock
    def test_companies_get_no_local_market_when_neither_source(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_NO_LOCAL_MARKET_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context["local_market"] is None  # type: ignore[attr-defined]
        client.close()

    @respx.mock
    def test_companies_get_yoy_can_be_null(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_NULL_YOY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        local_market = result.data.labor_context["local_market"]  # type: ignore[attr-defined]
        assert local_market["unemployment_rate_yoy_change"] is None
        assert local_market["labor_force_yoy_change_pct"] is None
        client.close()
