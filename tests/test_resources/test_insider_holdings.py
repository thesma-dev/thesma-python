"""Tests for the InsiderHoldings resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_HOLDINGS_JSON = {
    "data": [
        {
            "person": {
                "name": "Jane Smith",
                "title": "CEO",
                "relationship": "ceo",
                "holdings": [
                    {
                        "security_type": "Common Stock",
                        "shares": 50000.0,
                        "ownership": "direct",
                        "is_derivative": False,
                    },
                ],
            },
            "filing_date": "2024-06-17",
            "filing_accession": "0001234567-24-000001",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestInsiderHoldingsList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.insider_holdings.list("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_url_interpolation(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000789019/insider-holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.insider_holdings.list("0000789019")

        assert route.called
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.insider_holdings.list("0000320193")

        assert result.data[0].person.name == "Jane Smith"
        assert result.pagination.total == 1
        client.close()

    @respx.mock
    def test_list_custom_pagination(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/insider-holdings").mock(
            return_value=httpx.Response(200, json=PAGINATED_HOLDINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.insider_holdings.list("0000320193", page=3, per_page=10)

        request = route.calls.last.request
        assert "page=3" in str(request.url)
        assert "per_page=10" in str(request.url)
        client.close()
