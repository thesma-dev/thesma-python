"""Tests for the Ratios resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import RatioResponse, RatioTimeSeriesResponse
from thesma._types import DataResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

RATIO_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "period": "annual",
        "fiscal_year": 2024,
        "ratios": {
            "gross_margin": 46.2,
            "operating_margin": 31.5,
            "net_margin": 26.4,
        },
        "metadata": {"filing_accession": "0000320193-24-000081"},
    },
}

RATIO_TIME_SERIES_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "ratio": "gross_margin",
        "period": "annual",
        "series": [
            {"fiscal_year": 2024, "value": 46.2},
            {"fiscal_year": 2023, "value": 44.1},
        ],
    },
}


class TestRatiosGet:
    @respx.mock
    def test_get(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/ratios").mock(
            return_value=httpx.Response(200, json=RATIO_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.ratios.get("0000320193")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, RatioResponse)
        client.close()

    @respx.mock
    def test_get_with_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/ratios").mock(
            return_value=httpx.Response(200, json=RATIO_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.ratios.get("0000320193", period="quarterly", year=2024, quarter=3)

        request = route.calls.last.request
        assert "period=quarterly" in str(request.url)
        assert "year=2024" in str(request.url)
        assert "quarter=3" in str(request.url)
        client.close()


class TestRatiosTimeSeries:
    @respx.mock
    def test_time_series(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/ratios/gross_margin").mock(
            return_value=httpx.Response(200, json=RATIO_TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.ratios.time_series("0000320193", "gross_margin")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, RatioTimeSeriesResponse)
        client.close()

    @respx.mock
    def test_time_series_with_period(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/ratios/gross_margin").mock(
            return_value=httpx.Response(200, json=RATIO_TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.ratios.time_series("0000320193", "gross_margin", period="quarterly")

        request = route.calls.last.request
        assert "period=quarterly" in str(request.url)
        client.close()
