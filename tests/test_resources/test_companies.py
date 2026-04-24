"""Tests for the Companies resource."""

from __future__ import annotations

import httpx
import pytest
import respx

from thesma._generated.models import EnrichedCompanyData, EnrichedCompanyDataResponse
from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient
from thesma.errors import BadRequestError

BASE = "https://api.thesma.dev"

PAGINATED_COMPANIES_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sic_code": "3571",
            "company_tier": "sp500",
            "exchange": "NASDAQ",
            "domicile": "us",
            "state_fips": "06",
            "county_fips": "06073",
            "detail_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

PAGINATED_COMPANIES_JSON_NULL_EXCHANGE_DOMICILE = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sic_code": "3571",
            "company_tier": "sp500",
            "exchange": None,
            "domicile": None,
            "state_fips": "06",
            "county_fips": "06073",
            "detail_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193",
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
        "exchange": "NASDAQ",
        "domicile": "us",
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
                    "naics_description": "Electronic Computer Manufacturing",
                    "naics_match_level": "6-digit",
                    "data_period": "2025-Q2",
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
        assert isinstance(result, EnrichedCompanyDataResponse)
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

        assert result.data.labor_context is not None
        local_market = result.data.labor_context.local_market
        assert local_market is not None
        assert local_market.unemployment_rate == 2.8
        assert local_market.match_level == "county"
        assert local_market.seasonal_adjustment == "not_seasonally_adjusted"
        assert local_market.source == "LAUS+QCEW"
        client.close()

    @respx.mock
    def test_companies_get_local_market_source_qcew_only(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_QCEW_ONLY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context is not None
        local_market = result.data.labor_context.local_market
        assert local_market is not None
        assert local_market.source == "QCEW"
        assert local_market.unemployment_rate is None
        assert local_market.match_level is None
        client.close()

    @respx.mock
    def test_companies_get_local_market_source_laus_only(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_LAUS_ONLY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context is not None
        local_market = result.data.labor_context.local_market
        assert local_market is not None
        assert local_market.source == "LAUS"
        assert local_market.industry_employment is None
        assert local_market.data_period is None
        assert local_market.data_lag_months is None
        assert local_market.match_precision is None
        assert local_market.unemployment_rate == 2.8
        client.close()

    @respx.mock
    def test_companies_get_state_fallback_match_level(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_STATE_FALLBACK_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context is not None
        local_market = result.data.labor_context.local_market
        assert local_market is not None
        assert local_market.match_level == "state"
        assert local_market.seasonal_adjustment == "seasonally_adjusted"
        assert local_market.county_fips is None
        client.close()

    @respx.mock
    def test_companies_get_no_local_market_when_neither_source(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_NO_LOCAL_MARKET_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context is not None
        assert result.data.labor_context.local_market is None
        client.close()

    @respx.mock
    def test_companies_get_yoy_can_be_null(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_WITH_NULL_YOY_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")

        assert result.data.labor_context is not None
        local_market = result.data.labor_context.local_market
        assert local_market is not None
        assert local_market.unemployment_rate_yoy_change is None
        assert local_market.labor_force_yoy_change_pct is None
        client.close()


class TestCompaniesListExchangeDomicile:
    @respx.mock
    def test_list_with_exchange_single(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(exchange="nyse")

        request = route.calls.last.request
        assert "exchange=nyse" in str(request.url)
        client.close()

    @respx.mock
    def test_list_with_exchange_multiple(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(exchange=["nyse", "nasdaq"])

        url_str = str(route.calls.last.request.url)
        assert "exchange=nyse" in url_str
        assert "exchange=nasdaq" in url_str
        client.close()

    @respx.mock
    def test_list_with_domicile(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(domicile="us")

        request = route.calls.last.request
        assert "domicile=us" in str(request.url)
        client.close()

    @respx.mock
    def test_list_with_exchange_and_domicile_and_tier_combined(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(exchange=["nyse", "nasdaq"], domicile="us", tier="russell1000")

        url_str = str(route.calls.last.request.url)
        assert "exchange=nyse" in url_str
        assert "exchange=nasdaq" in url_str
        assert "domicile=us" in url_str
        assert "tier=russell1000" in url_str
        client.close()

    @respx.mock
    def test_list_exchange_domicile_omitted_when_none(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list()

        url_str = str(route.calls.last.request.url)
        assert "exchange=" not in url_str
        assert "domicile=" not in url_str
        client.close()

    @respx.mock
    def test_list_exchange_empty_list_omitted(self, api_key: str) -> None:
        """Empty list must be normalised to None so httpx does not attempt to serialise it."""
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(exchange=[])

        url_str = str(route.calls.last.request.url)
        assert "exchange=" not in url_str
        client.close()

    @respx.mock
    def test_list_response_parses_exchange_and_domicile(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.list()

        # CompanyListItem is typed post-regeneration; .exchange and .domicile
        # resolve to Enum members whose .value matches the API string.
        assert result.data[0].exchange is not None
        assert result.data[0].exchange.value == "NASDAQ"
        assert result.data[0].domicile is not None
        assert result.data[0].domicile.value == "us"
        client.close()

    @respx.mock
    def test_list_response_parses_null_exchange_and_domicile(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON_NULL_EXCHANGE_DOMICILE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.list()

        assert result.data[0].exchange is None
        assert result.data[0].domicile is None
        client.close()

    @respx.mock
    def test_list_invalid_exchange_raises_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(
                400,
                json={"error": {"code": "invalid_parameter", "message": "Invalid exchange 'amex'..."}},
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.companies.list(exchange="amex")
        client.close()

    @respx.mock
    def test_list_invalid_domicile_raises_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(
                400,
                json={"error": {"code": "invalid_parameter", "message": "Invalid domicile 'uk'..."}},
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.companies.list(domicile="uk")
        client.close()


class TestCompaniesListTaxonomyCurrency:
    @respx.mock
    def test_list_with_taxonomy(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(taxonomy="ifrs-full")

        assert "taxonomy=ifrs-full" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_list_with_currency(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(currency="EUR")

        assert "currency=EUR" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_list_with_taxonomy_and_currency_combined(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list(taxonomy="us-gaap", currency="USD")

        url_str = str(route.calls.last.request.url)
        assert "taxonomy=us-gaap" in url_str
        assert "currency=USD" in url_str
        client.close()

    @respx.mock
    def test_list_taxonomy_currency_omitted_when_none(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(200, json=PAGINATED_COMPANIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.companies.list()

        url_str = str(route.calls.last.request.url)
        assert "taxonomy=" not in url_str
        assert "currency=" not in url_str
        client.close()


class TestCompaniesGetExchangeDomicile:
    @respx.mock
    def test_get_response_carries_exchange_and_domicile(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=COMPANY_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193")

        # EnrichedCompanyData is still an extra="allow" stub post-regeneration;
        # access via the stub-attr pattern.
        assert result.data.exchange == "NASDAQ"  # type: ignore[attr-defined]
        assert result.data.domicile == "us"  # type: ignore[attr-defined]
        client.close()


_LENDING_CONTEXT_FULL = {
    "local_market": {
        "county_fips": "06037",
        "county_name": "Los Angeles County, CA",
        "county_fips_confidence": "high",
        "quarterly_loan_count": 142,
        "quarterly_total_amount": 38_500_000,
        "avg_loan_size": 271_127,
        "quarterly_yoy_change_pct": 8.4,
        "charge_off_rate_trailing_4q": 2.1,
        "top_industry_naics": "722511",
        "top_industry_name": "Full-Service Restaurants",
        "data_period": "2025-Q3",
        "source": "SBA",
    },
    "industry_lending": {
        "naics_code": "511210",
        "naics_description": "Software Publishers",
        "naics_match_level": "6-digit",
        "national_quarterly_loan_count": 920,
        "national_quarterly_total_amount": 210_000_000,
        "national_avg_loan_size": 228_260,
        "national_yoy_change_pct": 6.1,
        "national_charge_off_rate_trailing_4q": 1.3,
        "data_period": "2025-Q3",
        "source": "SBA",
    },
}


def _build_company_with_lending_context(lending_context: dict | None, *, omit_key: bool = False) -> dict:
    data: dict = {
        "cik": "0000320193",
        "name": "Apple Inc.",
        "ticker": "AAPL",
        "sic_code": "3571",
        "company_tier": "sp500",
        "state_fips": "06",
        "county_fips": "06085",
        "filings_url": "/v1/us/sec/companies/0000320193/filings",
        "financials_url": "/v1/us/sec/companies/0000320193/financials",
    }
    if not omit_key:
        data["lending_context"] = lending_context
    return {"data": data}


class TestCompaniesGetLendingContext:
    @respx.mock
    def test_get_with_include_lending_context_populated(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=_build_company_with_lending_context(_LENDING_CONTEXT_FULL)),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="lending_context")

        lending_context = result.data.lending_context
        assert lending_context is not None
        assert lending_context.local_market is not None
        assert lending_context.local_market.county_fips == "06037"
        assert lending_context.industry_lending is not None
        assert lending_context.industry_lending.naics_code == "511210"
        client.close()

    @respx.mock
    def test_get_with_include_lending_context_omitted_key(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=_build_company_with_lending_context(None, omit_key=True)),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="lending_context")

        assert result.data.lending_context is None
        client.close()

    @respx.mock
    def test_get_with_include_lending_context_null_children(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(
                200,
                json=_build_company_with_lending_context({"local_market": None, "industry_lending": None}),
            ),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="lending_context")

        lending_context = result.data.lending_context
        assert lending_context is not None
        assert lending_context.local_market is None
        assert lending_context.industry_lending is None
        client.close()

    @respx.mock
    def test_get_with_include_lending_context_partial_local_only(self, api_key: str) -> None:
        partial = {"local_market": _LENDING_CONTEXT_FULL["local_market"], "industry_lending": None}
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=_build_company_with_lending_context(partial)),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="lending_context")

        lending_context = result.data.lending_context
        assert lending_context is not None
        assert lending_context.local_market is not None
        assert lending_context.local_market.county_fips == "06037"
        assert lending_context.industry_lending is None
        client.close()

    @respx.mock
    def test_get_with_include_labor_and_lending_combined(self, api_key: str) -> None:
        combined_data = _build_company_with_lending_context(_LENDING_CONTEXT_FULL)
        combined_data["data"]["labor_context"] = {
            "industry": {
                "naics_code": "334111",
                "naics_description": "Electronic Computer Manufacturing",
                "naics_match_level": "6-digit",
                "data_period": "2025-Q2",
            },
        }
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=combined_data),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context,lending_context")

        assert "include=labor_context%2Clending_context" in str(route.calls.last.request.url)
        assert result.data.labor_context is not None
        assert result.data.lending_context is not None
        client.close()

    @respx.mock
    def test_get_with_unknown_include_value_raises_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(
                400, json={"error": {"code": "bad_request", "message": "unknown include value"}}
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.companies.get("0000320193", include="bogus")
        client.close()

    @respx.mock
    def test_get_county_fips_confidence_unknown_parses(self, api_key: str) -> None:
        payload = _build_company_with_lending_context(
            {
                "local_market": {
                    **_LENDING_CONTEXT_FULL["local_market"],
                    "county_fips_confidence": "unknown",
                },
                "industry_lending": _LENDING_CONTEXT_FULL["industry_lending"],
            }
        )
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="lending_context")

        lending_context = result.data.lending_context
        assert lending_context is not None
        assert lending_context.local_market is not None
        # county_fips_confidence is a codegen enum; value access works for equality.
        assert lending_context.local_market.county_fips_confidence.value == "unknown"
        client.close()


# --- SDK-32: include= composition primitive (9-value expander set) ---------

_HATEOAS_URL_FIELDS = {
    "filings_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/filings",
    "financials_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/financials",
    "ratios_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/ratios",
    "events_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/events",
    "insider_trades_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/insider-trades",
    "insider_holdings_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/insider-holdings",
    "holders_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/holders",
    "compensation_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/compensation",
    "board_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/board",
    "proxy_votes_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/proxy-votes",
    "beneficial_ownership_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/beneficial-ownership",
}

_COMPANY_BASE = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "ticker": "AAPL",
    "sic_code": "3571",
    "company_tier": "sp500",
    "state_fips": "06",
    "county_fips": "06073",
    **_HATEOAS_URL_FIELDS,
}


class TestCompaniesGetIncludeComposition:
    @respx.mock
    def test_get_with_include_financials_inline(self, api_key: str) -> None:
        """Requested expander returns inline payload in its slot."""
        payload = {
            "data": {
                **_COMPANY_BASE,
                "financials": {"line_items": {"revenue": 391_035_000_000}, "fiscal_year": 2024},
            }
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="financials")
        # SDK-33: S1 expander slots land as typed attributes (Any | None).
        assert result.data.financials is not None
        assert result.data.financials["line_items"]["revenue"] == 391_035_000_000
        client.close()

    @respx.mock
    def test_get_with_include_eight_expanders(self, api_key: str) -> None:
        """8-expander combination — no events requested.

        Separate coverage for include=events lives in
        test_get_with_include_events_returns_populated_slot.
        """
        payload = {
            "data": {
                **_COMPANY_BASE,
                "financials": {"line_items": {"revenue": 391_035_000_000}},
                "ratios": {"return_on_equity": 1.65},
                "insider_trades": [{"transaction_date": "2025-12-01"}],
                "holders": [{"fund_cik": "0001234567", "shares": 1000000}],
                "compensation": {"pay_ratio": 1447},
                "board": {"members": []},
                "labor_context": {
                    "industry": {
                        "naics_code": "334111",
                        "naics_description": "Electronic Computer Manufacturing",
                        "naics_match_level": "6-digit",
                        "data_period": "2025-Q2",
                    }
                },
                "lending_context": {"local_market": None, "industry_lending": None},
            }
        }
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=payload),
        )
        include_value = "financials,ratios,insider_trades,holders,compensation,board,labor_context,lending_context"
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include=include_value)
        # SDK-33: S1 expander slots + labor/lending land as typed attributes on EnrichedCompanyData.
        for slot in (
            "financials",
            "ratios",
            "insider_trades",
            "holders",
            "compensation",
            "board",
            "labor_context",
            "lending_context",
        ):
            assert getattr(result.data, slot) is not None, f"slot '{slot}' missing from response"
        # events was NOT requested — events_url HATEOAS link still passes through via extra="allow".
        extra = result.data.model_extra or {}
        assert extra["events_url"].startswith("https://")
        # Query param forwarded verbatim (comma URL-encoded by httpx).
        assert "include=financials%2Cratios" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_with_include_events_returns_populated_slot(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        **_COMPANY_BASE,
                        "events": [
                            {
                                "filing_accession": "0000320193-25-000012",
                                "filed_at": "2025-12-01T16:00:00+00:00",
                                "category": "earnings",
                                "items": [
                                    {"code": "2.02", "description": "Results of Operations and Financial Condition"},
                                ],
                            },
                            {
                                "filing_accession": "0000320193-25-000010",
                                "filed_at": "2025-11-15T14:00:00+00:00",
                                "category": "leadership",
                                "items": [
                                    {"code": "5.02", "description": "Departure of Directors or Certain Officers"}
                                ],
                            },
                        ],
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="events")
        assert result.data.events is not None
        assert len(result.data.events) == 2
        assert result.data.events[0]["filing_accession"] == "0000320193-25-000012"
        assert result.data.events[0]["category"] == "earnings"
        client.close()

    @respx.mock
    def test_get_with_include_events_combination_returns_all_slots(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        **_COMPANY_BASE,
                        "financials": {"line_items": {"revenue": 391_035_000_000}},
                        "events": [
                            {
                                "filing_accession": "0000320193-25-000012",
                                "filed_at": "2025-12-01T16:00:00+00:00",
                                "category": "earnings",
                                "items": [
                                    {"code": "2.02", "description": "Results of Operations and Financial Condition"},
                                ],
                            },
                        ],
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="financials,events")
        assert result.data.financials is not None
        assert result.data.financials["line_items"]["revenue"] == 391_035_000_000
        assert result.data.events is not None
        assert len(result.data.events) == 1
        assert result.data.events[0]["filing_accession"] == "0000320193-25-000012"
        client.close()

    @respx.mock
    def test_get_with_include_partial_failure_error_slot(self, api_key: str) -> None:
        """Upstream timeout on a single expander returns typed error slot; top-level 200."""
        payload = {
            "data": {
                **_COMPANY_BASE,
                "financials": {"line_items": {"revenue": 391_035_000_000}},
                "holders": {"error": {"code": "upstream_timeout", "message": "holders did not complete within 3.0s"}},
            }
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="financials,holders")
        # financials succeeded — inline dict in the typed slot.
        assert result.data.financials is not None
        assert result.data.financials["line_items"]["revenue"] == 391_035_000_000
        # holders failed — error slot dict with typed "error" sub-object.
        assert result.data.holders is not None
        assert isinstance(result.data.holders, dict) and "error" in result.data.holders
        assert result.data.holders["error"]["code"] == "upstream_timeout"
        client.close()

    @respx.mock
    def test_get_with_include_all_returns_400(self, api_key: str) -> None:
        """No ``all`` shortcut — callers must enumerate the expanders."""
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(
                400,
                json={
                    "error": {
                        "code": "validation_error",
                        "message": "Unknown include values: all.",
                        "status": 400,
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.companies.get("0000320193", include="all")
        client.close()

    @respx.mock
    def test_get_backwards_compat_labor_context_only(self, api_key: str) -> None:
        """Pre-S1 2-value set still works — no regression."""
        payload = {
            "data": {
                **_COMPANY_BASE,
                "labor_context": {
                    "industry": {
                        "naics_code": "334111",
                        "naics_description": "Electronic Computer Manufacturing",
                        "naics_match_level": "6-digit",
                        "data_period": "2025-Q2",
                    }
                },
            }
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.companies.get("0000320193", include="labor_context")
        # SDK-33: labor_context is typed on EnrichedCompanyData.
        assert result.data.labor_context is not None
        client.close()

    @respx.mock
    def test_companies_list_rejects_s1_expanders(self, api_key: str) -> None:
        """Scope lock: ``companies.list()`` still accepts only ``labor_context`` /
        ``lending_context``. The 7 new S1 expanders (``financials``, ``ratios``, etc.)
        are single-resource-only; passing them to the list endpoint returns 400.
        """
        respx.get(f"{BASE}/v1/us/sec/companies").mock(
            return_value=httpx.Response(
                400,
                json={
                    "error": {
                        "code": "validation_error",
                        "message": (
                            "Unknown include values: financials. Supported values: labor_context, lending_context"
                        ),
                        "status": 400,
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.companies.list(include="financials")
        client.close()
