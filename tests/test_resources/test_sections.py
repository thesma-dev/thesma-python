"""Tests for the Sections resource."""

from __future__ import annotations

import httpx
import respx

from thesma._generated.models import SearchPaginatedResponse, SectionChangeResponse, SectionDetail
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_SECTIONS_JSON = {
    "data": [
        {
            "accession_number": "0000320193-24-000081",
            "cik": "0000320193",
            "filing_type": "10-K",
            "filed_at": "2024-11-01T00:00:00Z",
            "period_of_report": "2024-09-28",
            "section_type": "item_1a",
            "section_title": "Risk Factors",
            "word_count": 15000,
            "fiscal_year": 2024,
            "content_url": "/v1/us/sec/filings/0000320193-24-000081/sections/item_1a",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

SECTION_LIST_JSON = {
    "data": {
        "accession_number": "0000320193-24-000081",
        "cik": "0000320193",
        "filing_type": "10-K",
        "sections": [
            {
                "section_type": "item_1a",
                "section_title": "Risk Factors",
                "word_count": 15000,
                "content_url": "/v1/us/sec/filings/0000320193-24-000081/sections/item_1a",
            },
        ],
    },
}

SECTION_DETAIL_JSON = {
    "data": {
        "accession_number": "0000320193-24-000081",
        "cik": "0000320193",
        "filing_type": "10-K",
        "section_type": "item_1a",
        "section_title": "Risk Factors",
        "word_count": 15000,
        "content": "Risk factor content here...",
        "content_url": "/v1/us/sec/filings/0000320193-24-000081/sections/item_1a",
    },
}

SECTION_CHANGES_JSON = {
    "data": {
        "current_accession_number": "0000320193-24-000081",
        "previous_accession_number": "0000320193-23-000077",
        "current_filed_at": "2024-11-01T00:00:00Z",
        "previous_filed_at": "2023-11-03T00:00:00Z",
        "similarity_score": 0.85,
        "paragraphs_added": 3,
        "paragraphs_removed": 1,
        "paragraphs_modified": 5,
        "paragraphs_unchanged": 40,
        "change_summary": {},
    },
}

ENTITIES_JSON = {
    "data": [
        {
            "accession_number": "0000320193-24-000081",
            "filing_type": "10-K",
            "filed_at": "2024-11-01T00:00:00Z",
            "entity_text": "Apple Inc.",
            "entity_type": "ORG",
            "start_char": 100,
            "end_char": 110,
        },
    ],
    "pagination": {"page": 1, "per_page": 50, "total": 1, "total_pages": 1},
}

SEARCH_JSON = {
    "data": [
        {
            "chunk_text": "The company faces risks related to...",
            "similarity_score": 0.92,
            "word_count": 150,
            "accession_number": "0000320193-24-000081",
            "cik": "0000320193",
            "company_name": "Apple Inc.",
            "filing_type": "10-K",
            "filed_at": "2024-11-01T00:00:00Z",
            "section_type": "item_1a",
        },
    ],
    "pagination": {"page": 1, "per_page": 20, "has_more": False},
}


class TestSectionsListByCompany:
    @respx.mock
    def test_list_by_company(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/sections").mock(
            return_value=httpx.Response(200, json=PAGINATED_SECTIONS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.list_by_company("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()


class TestSectionsListByFiling:
    @respx.mock
    def test_list_by_filing(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings/0000320193-24-000081/sections").mock(
            return_value=httpx.Response(200, json=SECTION_LIST_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.list_by_filing("0000320193-24-000081")

        assert route.called
        assert isinstance(result, DataResponse)
        assert len(result.data.sections) == 1
        client.close()


class TestSectionsGet:
    @respx.mock
    def test_get_section(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings/0000320193-24-000081/sections/item_1a").mock(
            return_value=httpx.Response(200, json=SECTION_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.get("0000320193-24-000081", "item_1a")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, SectionDetail)
        client.close()


class TestSectionsChanges:
    @respx.mock
    def test_changes(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings/0000320193-24-000081/sections/item_1a/changes").mock(
            return_value=httpx.Response(200, json=SECTION_CHANGES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.changes("0000320193-24-000081", "item_1a")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, SectionChangeResponse)
        assert result.data.similarity_score == 0.85
        client.close()


class TestSectionsEntities:
    @respx.mock
    def test_entities(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/sections/item_1a/entities").mock(
            return_value=httpx.Response(200, json=ENTITIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.entities("0000320193", "item_1a")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert result.data[0].entity_text == "Apple Inc."
        client.close()


class TestSectionsSearch:
    @respx.mock
    def test_search_sends_query(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/sections/search").mock(
            return_value=httpx.Response(200, json=SEARCH_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sections.search(query="risk factors supply chain")

        assert route.called
        request = route.calls.last.request
        assert "q=risk" in str(request.url)
        assert isinstance(result, SearchPaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_search_default_pagination(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/sections/search").mock(
            return_value=httpx.Response(200, json=SEARCH_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.sections.search(query="test")

        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=20" in str(request.url)
        client.close()
