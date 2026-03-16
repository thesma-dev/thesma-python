"""Tests for the ProxyVotes resource."""

from __future__ import annotations

import httpx
import respx

from thesma._types import PaginatedResponse
from thesma.client import ThesmaClient

BASE = "https://api.thesma.dev"

PAGINATED_VOTES_JSON = {
    "data": [
        {
            "proposal_number": "1",
            "proposal_type": "director_election",
            "description": "Election of Director - Tim Cook",
            "votes_for": 12000000000.0,
            "votes_against": 500000000.0,
            "votes_abstain": 100000000.0,
            "outcome": "passed",
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestProxyVotesList:
    @respx.mock
    def test_list_default_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/proxy-votes").mock(
            return_value=httpx.Response(200, json=PAGINATED_VOTES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.proxy_votes.list("0000320193")

        assert route.called
        request = route.calls.last.request
        assert "page=1" in str(request.url)
        assert "per_page=25" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        client.close()

    @respx.mock
    def test_list_response_parsed(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/proxy-votes").mock(
            return_value=httpx.Response(200, json=PAGINATED_VOTES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.proxy_votes.list("0000320193")

        assert result.data[0].proposal_type == "director_election"
        assert result.data[0].outcome == "passed"
        client.close()

    @respx.mock
    def test_list_custom_pagination(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/proxy-votes").mock(
            return_value=httpx.Response(200, json=PAGINATED_VOTES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.proxy_votes.list("0000320193", page=2, per_page=50)

        request = route.calls.last.request
        assert "page=2" in str(request.url)
        assert "per_page=50" in str(request.url)
        client.close()

    @respx.mock
    def test_list_url_interpolation(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000789019/proxy-votes").mock(
            return_value=httpx.Response(200, json=PAGINATED_VOTES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.proxy_votes.list("0000789019")

        assert route.called
        client.close()
