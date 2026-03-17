"""Tests for the Screener resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

SCREENER_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": 383285000000,
                "net_income": 96995000000,
                "eps_diluted": 6.08,
                "common_shares_outstanding": 15550061000,
                "total_equity": 62146000000,
                "dividends_paid": -15025000000,
                "institutional_ownership_pct": 62.5,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


SCREENER_JSON_NULL_FINANCIALS = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": None,
                "net_income": None,
                "eps_diluted": None,
                "common_shares_outstanding": None,
                "total_equity": None,
                "dividends_paid": None,
                "institutional_ownership_pct": None,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestScreenerScreen:
    @respx.mock
    def test_screen_with_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(tier="sp500", min_gross_margin=40)

        assert route.called
        request = route.calls.last.request
        assert "tier=sp500" in str(request.url)
        assert "min_gross_margin=40" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()

    @respx.mock
    def test_screen_response_type(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        assert result.data[0].cik == "0000320193"
        client.close()

    @respx.mock
    def test_screen_with_sic_single_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sic="7372")

        request = route.calls.last.request
        assert "sic=7372" in str(request.url)
        client.close()

    @respx.mock
    def test_screen_with_sic_multi_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sic=["7372", "3674"])

        request = route.calls.last.request
        url_str = str(request.url)
        assert "sic=7372" in url_str
        assert "sic=3674" in url_str
        client.close()

    @respx.mock
    def test_screen_sort_maps_to_api_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sort_by="gross_margin", order="desc")

        request = route.calls.last.request
        assert "sort=gross_margin" in str(request.url)
        assert "order=desc" in str(request.url)
        client.close()


class TestScreenerEnhancements:
    @respx.mock
    def test_new_params_passed_to_api(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(
            max_net_income=50000,
            min_institutional_ownership_pct=30.0,
            insider_buying_days="90",
        )

        request = route.calls.last.request
        assert "max_net_income=50000" in str(request.url)
        assert "min_institutional_ownership_pct=30.0" in str(request.url)
        assert "insider_buying_days=90" in str(request.url)
        client.close()

    @respx.mock
    def test_max_net_income_zero_not_stripped(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_net_income=0)

        request = route.calls.last.request
        assert "max_net_income=0" in str(request.url)
        client.close()

    @respx.mock
    def test_insider_buying_days_without_has_insider_buying(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(insider_buying_days="30")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "insider_buying_days=30" in url_str
        assert "has_insider_buying" not in url_str
        client.close()

    @respx.mock
    def test_financials_deserialized(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        item = result.data[0]
        assert item.financials.revenue == 383285000000
        assert item.financials.eps_diluted == 6.08
        assert item.financials.institutional_ownership_pct == 62.5
        client.close()

    @respx.mock
    def test_financials_all_null(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON_NULL_FINANCIALS),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        item = result.data[0]
        assert item.financials.revenue is None
        assert item.financials.net_income is None
        assert item.financials.eps_diluted is None
        assert item.financials.common_shares_outstanding is None
        assert item.financials.total_equity is None
        assert item.financials.dividends_paid is None
        assert item.financials.institutional_ownership_pct is None
        client.close()

    @respx.mock
    def test_negative_dividends_paid(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert result.data[0].financials.dividends_paid == -15025000000
        client.close()

    @respx.mock
    def test_existing_ratios_still_accessible(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert result.data[0].ratios.gross_margin == 46.2
        client.close()
