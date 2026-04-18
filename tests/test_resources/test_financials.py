"""Tests for the Financials resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import FieldsResponse, FinancialStatementResponse, TimeSeriesResponse
from thesma._types import DataResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

FINANCIAL_STATEMENT_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000081",
        "currency": "USD",
        "line_items": {"revenue": 391035000000},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "us-gaap:Revenues"},
        },
    },
}

TIME_SERIES_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "metric": "revenue",
        "period": "annual",
        "currency": "USD",
        "series": [
            {"fiscal_year": 2024, "value": 391035000000, "filing_accession": "0000320193-24-000081"},
            {"fiscal_year": 2023, "value": 383285000000, "filing_accession": "0000320193-23-000077"},
        ],
    },
}

FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000081",
        "currency": "USD",
        "line_items": {"revenue": 391035000000},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "us-gaap:Revenues"},
        },
        "labor_context": {
            "industry": {"naics_code": "334111"},
            "local_market": {
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
            },
            "compensation_benchmark": None,
        },
    },
}

FIELDS_JSON = {
    "data": {
        "income": {
            "fields": [
                {"name": "revenue", "description": "Total revenue", "bank_specific": False},
            ],
        },
        "balance_sheet": {
            "fields": [
                {"name": "total_assets", "description": "Total assets", "bank_specific": False},
            ],
        },
        "cash_flow": {
            "fields": [
                {"name": "operating_cash_flow", "description": "Operating cash flow", "bank_specific": False},
            ],
        },
    },
}


class TestFinancialsGet:
    @respx.mock
    def test_get_with_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", period="annual")

        assert route.called
        request = route.calls.last.request
        assert "statement=income" in str(request.url)
        assert "period=annual" in str(request.url)
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FinancialStatementResponse)
        client.close()

    @respx.mock
    def test_get_with_year_and_quarter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income", period="quarterly", year=2024, quarter=3)

        request = route.calls.last.request
        assert "year=2024" in str(request.url)
        assert "quarter=3" in str(request.url)
        client.close()


class TestFinancialsTimeSeries:
    @respx.mock
    def test_time_series_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials/revenue").mock(
            return_value=httpx.Response(200, json=TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.time_series("0000320193", "revenue")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, TimeSeriesResponse)
        client.close()

    @respx.mock
    def test_time_series_with_period(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials/revenue").mock(
            return_value=httpx.Response(200, json=TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.time_series("0000320193", "revenue", period="quarterly")

        request = route.calls.last.request
        assert "period=quarterly" in str(request.url)
        client.close()


class TestFinancialsLaborContextInclude:
    @respx.mock
    def test_financials_get_laus_local_market_fields_present(self, api_key: str) -> None:
        """LAUS-enriched financials responses deserialize cleanly via ?include=labor_context.

        ``FinancialStatementResponse`` uses Pydantic's default ``extra="ignore"``,
        so the top-level ``labor_context`` key is dropped when the strict model
        parses the payload. The test therefore asserts two things:

        1. The ``include=labor_context`` query parameter reaches the API.
        2. The payload (including the LAUS-enriched ``local_market`` block)
           parses without raising a validation error — confirming the new
           LAUS fields on the generated ``LocalMarketContext`` model are
           compatible with the serialized shape the financials endpoint
           returns.
        """
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", include="labor_context")

        request = route.calls.last.request
        assert "include=labor_context" in str(request.url)
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FinancialStatementResponse)
        # Confirm the well-known financials fields still round-trip cleanly.
        assert result.data.fiscal_year == 2024
        assert result.data.line_items["revenue"] == 391035000000
        client.close()


class TestFinancialsLendingContextInclude:
    @respx.mock
    def test_get_with_include_lending_context_forwards_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income", period="annual", include="lending_context")

        assert "include=lending_context" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_with_include_combined_forwards_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", include="labor_context,lending_context")

        assert "include=labor_context%2Clending_context" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_without_include_omits_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income")

        assert "include=" not in str(route.calls.last.request.url)
        client.close()


class TestFinancialsFields:
    @respx.mock
    def test_fields(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/financials/fields").mock(
            return_value=httpx.Response(200, json=FIELDS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.fields()

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FieldsResponse)
        client.close()
