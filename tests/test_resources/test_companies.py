"""Tests for the Companies resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import CompanyResponse
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

        assert isinstance(result.data, CompanyResponse)
        client.close()
