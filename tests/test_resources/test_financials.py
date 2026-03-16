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
