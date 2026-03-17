"""Tests for the Events resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import EventCategory
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_EVENTS_JSON = {
    "data": [
        {
            "filing_accession": "0000320193-24-000001",
            "cik": "0000320193",
            "company_name": "Apple Inc.",
            "filed_at": "2024-10-31T00:00:00Z",
            "items": [{"item_code": "2.02"}],
            "category": "earnings",
            "filing_url": "/v1/us/sec/filings/0000320193-24-000001",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

CATEGORIES_JSON = {
    "data": [
        {"name": "earnings", "description": "Earnings announcements", "filing_count": 5000},
        {"name": "leadership", "description": "Leadership changes", "filing_count": 2000},
    ],
}


class TestEventsList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.events.list("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_with_category(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list("0000320193", category="earnings")

        request = route.calls.last.request
        assert "category=earnings" in str(request.url)
        client.close()

    @respx.mock
    def test_list_none_category_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list("0000320193")

        request = route.calls.last.request
        assert "category=" not in str(request.url)
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.events.list("0000320193")

        assert result.data[0].category == "earnings"
        assert result.data[0].cik == "0000320193"
        client.close()

    @respx.mock
    def test_list_with_from_date(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list("0000320193", from_date="2024-01-01")

        request = route.calls.last.request
        assert "from=2024-01-01" in str(request.url)
        client.close()

    @respx.mock
    def test_list_none_from_date_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list("0000320193")

        request = route.calls.last.request
        assert "from=" not in str(request.url)
        client.close()


class TestEventsListAll:
    @respx.mock
    def test_list_all(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.events.list_all(category="earnings")

        assert route.called
        request = route.calls.last.request
        assert "category=earnings" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()


    @respx.mock
    def test_list_all_with_from_date(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list_all(from_date="2024-01-01")

        request = route.calls.last.request
        assert "from=2024-01-01" in str(request.url)
        client.close()

    @respx.mock
    def test_list_all_none_from_date_omitted(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/events").mock(
            return_value=httpx.Response(200, json=PAGINATED_EVENTS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.events.list_all()

        request = route.calls.last.request
        assert "from=" not in str(request.url)
        client.close()


class TestEventsCategories:
    @respx.mock
    def test_categories_returns_list(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/events/categories").mock(
            return_value=httpx.Response(200, json=CATEGORIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.events.categories()

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, list)
        assert len(result.data) == 2
        assert isinstance(result.data[0], EventCategory)
        assert result.data[0].name == "earnings"
        client.close()
