"""Tests for the InsiderTrades resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_TRADES_JSON = {
    "data": [
        {
            "person": {"name": "John Doe", "title": "CFO", "relationship": "cfo"},
            "cik": "0000320193",
            "transaction_date": "2024-06-15",
            "type": "purchase",
            "security": "Common Stock",
            "shares": 1000.0,
            "price_per_share": 150.0,
            "total_value": 150000.0,
            "ownership": "direct",
            "filing_accession": "0001234567-24-000001",
            "filing_url": "/v1/us/sec/filings/0001234567-24-000001",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestInsiderTradesList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.insider_trades.list("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_with_trade_type(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.insider_trades.list("0000320193", trade_type="purchase")

        request = route.calls.last.request
        assert "type=purchase" in str(request.url)
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.insider_trades.list("0000320193")

        assert result.data[0].cik == "0000320193"
        assert result.pagination.total == 1
        client.close()

    @respx.mock
    def test_list_none_trade_type_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.insider_trades.list("0000320193")

        request = route.calls.last.request
        assert "type=" not in str(request.url)
        client.close()


class TestInsiderTradesListAll:
    @respx.mock
    def test_list_all_default(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.insider_trades.list_all()

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()

    @respx.mock
    def test_list_all_url_is_global(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/insider-trades").mock(
            return_value=httpx.Response(200, json=PAGINATED_TRADES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.insider_trades.list_all()

        assert route.called
        assert "/companies/" not in str(route.calls.last.request.url)
        client.close()
