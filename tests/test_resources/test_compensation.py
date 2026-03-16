"""Tests for the Compensation resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import BoardResponse, CompensationResponse
from thesma._types import DataResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

COMPENSATION_JSON = {
    "data": {
        "company": {"cik": "0000320193", "name": "Apple Inc.", "ticker": "AAPL"},
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000001",
        "executives": [
            {
                "name": "Tim Cook",
                "title": "CEO",
                "compensation": {
                    "salary": 3000000.0,
                    "bonus": 0.0,
                    "stock_awards": 40000000.0,
                    "option_awards": 0.0,
                    "non_equity_incentive": 12000000.0,
                    "other": 1200000.0,
                    "total": 63200000.0,
                },
                "extraction_confidence": "high",
            },
        ],
    },
}

BOARD_JSON = {
    "data": {
        "company": {"cik": "0000320193", "name": "Apple Inc.", "ticker": "AAPL"},
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000002",
        "members": [
            {
                "name": "Arthur Levinson",
                "is_independent": True,
                "committees": ["Compensation Committee"],
                "age": 74,
            },
        ],
    },
}


class TestCompensationGet:
    @respx.mock
    def test_get_default(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/executive-compensation").mock(
            return_value=httpx.Response(200, json=COMPENSATION_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.compensation.get("0000320193")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, CompensationResponse)
        assert result.data.fiscal_year == 2024
        client.close()

    @respx.mock
    def test_get_with_year(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/executive-compensation").mock(
            return_value=httpx.Response(200, json=COMPENSATION_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.compensation.get("0000320193", year=2024)

        request = route.calls.last.request
        assert "year=2024" in str(request.url)
        client.close()

    @respx.mock
    def test_get_none_year_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/executive-compensation").mock(
            return_value=httpx.Response(200, json=COMPENSATION_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.compensation.get("0000320193")

        request = route.calls.last.request
        assert "year=" not in str(request.url)
        client.close()


class TestCompensationBoard:
    @respx.mock
    def test_board(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/board").mock(
            return_value=httpx.Response(200, json=BOARD_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.compensation.board("0000320193")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, BoardResponse)
        assert len(result.data.members) == 1
        client.close()
