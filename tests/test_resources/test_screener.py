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
