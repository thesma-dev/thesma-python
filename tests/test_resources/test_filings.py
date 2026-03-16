"""Tests for the Filings resource."""

from __future__ import annotations

import datetime

import httpx
import pytest
import respx

from thesma._generated.models import FilingContentResponse, FilingDetailResponse
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient
from thesma.resources.filings import _to_date_str

BASE = "https://api.thesma.dev"

PAGINATED_FILINGS_JSON = {
    "data": [
        {
            "accession_number": "0000320193-24-000081",
            "cik": "0000320193",
            "filing_type": "10-K",
            "filed_at": "2024-11-01T00:00:00Z",
            "period_of_report": "2024-09-28",
            "is_amendment": False,
            "is_superseded": False,
            "is_latest_for_period": True,
            "detail_url": "/v1/us/sec/filings/0000320193-24-000081",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

FILING_DETAIL_JSON = {
    "data": {
        "accession_number": "0000320193-24-000081",
        "cik": "0000320193",
        "company_name": "Apple Inc.",
        "filing_type": "10-K",
        "filed_at": "2024-11-01T00:00:00Z",
        "period_of_report": "2024-09-28",
        "is_amendment": False,
        "is_superseded": False,
        "is_latest_for_period": True,
        "parse_status": "parsed",
        "financials_available": True,
        "content_url": "/v1/us/sec/filings/0000320193-24-000081/content",
    },
}

FILING_CONTENT_JSON = {
    "data": {
        "accession_number": "0000320193-24-000081",
        "filing_type": "10-K",
        "content_type": "text/plain",
        "content": "Filing content text here.",
        "source_url": "https://www.sec.gov/Archives/edgar/data/320193/...",
    },
}


class TestFilingsList:
    @respx.mock
    def test_list_company_filings(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/filings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FILINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.filings.list("0000320193")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert result.data[0].accession_number == "0000320193-24-000081"
        client.close()

    @respx.mock
    def test_list_with_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/filings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FILINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.filings.list("0000320193", filing_type="10-K", start_date="2024-01-01")

        request = route.calls.last.request
        assert "type=10-K" in str(request.url)
        assert "from=2024-01-01" in str(request.url)
        client.close()

    @respx.mock
    def test_list_with_date_object(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/filings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FILINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.filings.list("0000320193", start_date=datetime.date(2024, 1, 1))

        request = route.calls.last.request
        assert "from=2024-01-01" in str(request.url)
        client.close()


class TestFilingsListAll:
    @respx.mock
    def test_list_all_filings(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings").mock(
            return_value=httpx.Response(200, json=PAGINATED_FILINGS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.filings.list_all(filing_type="10-K")

        assert route.called
        request = route.calls.last.request
        assert "type=10-K" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()


class TestFilingsGet:
    @respx.mock
    def test_get_filing(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings/0000320193-24-000081").mock(
            return_value=httpx.Response(200, json=FILING_DETAIL_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.filings.get("0000320193-24-000081")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FilingDetailResponse)
        client.close()


class TestFilingsContent:
    @respx.mock
    def test_content(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/filings/0000320193-24-000081/content").mock(
            return_value=httpx.Response(200, json=FILING_CONTENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.filings.content("0000320193-24-000081")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FilingContentResponse)
        client.close()


class TestDateConversion:
    def test_none_returns_none(self) -> None:
        assert _to_date_str(None) is None

    def test_string_passthrough(self) -> None:
        assert _to_date_str("2024-01-01") == "2024-01-01"

    def test_date_converted(self) -> None:
        assert _to_date_str(datetime.date(2024, 1, 1)) == "2024-01-01"

    def test_datetime_raises(self) -> None:
        with pytest.raises(TypeError, match=r"datetime\.datetime"):
            _to_date_str(datetime.datetime(2024, 1, 1, 12, 0))  # type: ignore[arg-type]
