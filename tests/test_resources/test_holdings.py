"""Tests for the Holdings resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_HOLDERS_JSON = {
    "data": [
        {
            "fund_cik": "0001234567",
            "fund_name": "Vanguard Group Inc",
            "shares": 1200000000.0,
            "market_value": 180000000000.0,
            "filing_accession": "0001234567-24-000001",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

PAGINATED_CHANGES_JSON = {
    "data": [
        {
            "fund_cik": "0001234567",
            "fund_name": "Vanguard Group Inc",
            "change_type": "increased",
            "current_shares": 1200000000.0,
            "previous_shares": 1150000000.0,
            "share_delta": 50000000.0,
            "pct_change": 4.35,
            "quarter": "2024-Q3",
            "previous_quarter": "2024-Q2",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

PAGINATED_FUNDS_JSON = {
    "data": [
        {
            "cik": "0001234567",
            "name": "Vanguard Group Inc",
            "holdings_url": "/v1/us/sec/funds/0001234567/holdings",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

PAGINATED_FUND_HOLDINGS_JSON = {
    "data": [
        {
            "held_company_name": "Apple Inc.",
            "cusip": "037833100",
            "held_company_cik": "0000320193",
            "held_company_ticker": "AAPL",
            "shares": 1200000000.0,
            "market_value": 180000000000.0,
            "position_type": "equity",
            "filing_accession": "0001234567-24-000001",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

PAGINATED_FUND_CHANGES_JSON = {
    "data": [
        {
            "held_company_name": "Apple Inc.",
            "cusip": "037833100",
            "held_company_cik": "0000320193",
            "change_type": "increased",
            "current_shares": 1200000000.0,
            "previous_shares": 1150000000.0,
            "share_delta": 50000000.0,
            "pct_change": 4.35,
            "quarter": "2024-Q3",
            "previous_quarter": "2024-Q2",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestHolders:
    @respx.mock
    def test_holders_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/institutional-holders").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDERS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.holdings.holders("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_holders_with_quarter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/institutional-holders").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDERS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.holdings.holders("0000320193", quarter="2024-Q3")

        request = route.calls.last.request
        assert "quarter=2024-Q3" in str(request.url)
        client.close()

    @respx.mock
    def test_holders_none_quarter_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/institutional-holders").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDERS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.holdings.holders("0000320193")

        request = route.calls.last.request
        assert "quarter=" not in str(request.url)
        client.close()


class TestHolderChanges:
    @respx.mock
    def test_holder_changes(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/institutional-changes").mock(
            return_value=httpx.Response(200, json=PAGINATED_CHANGES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.holdings.holder_changes("0000320193")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        client.close()


class TestFunds:
    @respx.mock
    def test_funds_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/funds").mock(
            return_value=httpx.Response(200, json=PAGINATED_FUNDS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.holdings.funds()

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert result.data[0].name == "Vanguard Group Inc"
        client.close()


class TestFundHoldings:
    @respx.mock
    def test_fund_holdings(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/funds/0001234567/holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FUND_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.holdings.fund_holdings("0001234567")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert result.data[0].held_company_name == "Apple Inc."
        client.close()

    @respx.mock
    def test_fund_holdings_with_quarter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/funds/0001234567/holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FUND_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.holdings.fund_holdings("0001234567", quarter="2024-Q3")

        request = route.calls.last.request
        assert "quarter=2024-Q3" in str(request.url)
        client.close()


class TestFundChanges:
    @respx.mock
    def test_fund_changes(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/funds/0001234567/holding-changes").mock(
            return_value=httpx.Response(200, json=PAGINATED_FUND_CHANGES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.holdings.fund_changes("0001234567")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        client.close()
