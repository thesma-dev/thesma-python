"""Tests for the BeneficialOwnership resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_OWNERSHIP_JSON = {
    "data": [
        {
            "accession_number": "0001234567-24-000001",
            "cik": "0000320193",
            "filer_name": "Vanguard Group Inc",
            "schedule_type": "SC 13G/A",
            "filing_date": "2024-02-14",
            "shares_held": 1300000000.0,
            "percent_of_class": 7.8,
            "is_amendment": True,
            "is_group_filing": False,
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

EMPTY_OWNERSHIP_JSON = {
    "data": [],
    "pagination": {"page": 1, "per_page": 25, "total": 0, "total_pages": 0},
}


class TestBeneficialOwnershipList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=PAGINATED_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.beneficial_ownership.list("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=PAGINATED_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.beneficial_ownership.list("0000320193")

        assert result.data[0].filer_name == "Vanguard Group Inc"
        assert result.data[0].percent_of_class == 7.8
        client.close()

    @respx.mock
    def test_list_empty(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/9999999999/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=EMPTY_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.beneficial_ownership.list("9999999999")

        assert result.data == []
        assert result.pagination.total == 0
        client.close()

    @respx.mock
    def test_list_url_interpolation(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000789019/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=PAGINATED_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.beneficial_ownership.list("0000789019")

        assert route.called
        client.close()


class TestBeneficialOwnershipListAll:
    @respx.mock
    def test_list_all(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=PAGINATED_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.beneficial_ownership.list_all()

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()

    @respx.mock
    def test_list_all_url_is_global(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/beneficial-ownership").mock(
            return_value=httpx.Response(200, json=PAGINATED_OWNERSHIP_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.beneficial_ownership.list_all()

        assert route.called
        assert "/companies/" not in str(route.calls.last.request.url)
        client.close()
