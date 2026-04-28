"""Proxy votes resource — shareholder voting data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import ProxyVoteItem
from thesma._types import PaginatedResponse


class ProxyVotes:
    """Resource for proxy vote endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        identifier: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[ProxyVoteItem]:
        """List proxy votes for a company.

        ``GET /v1/us/sec/companies/{identifier}/proxy-votes``

        ``identifier`` accepts a CIK or ticker (case-insensitive, with
        ``TickerAlias`` fallback).
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/proxy-votes",
            params=params,
            response_model=PaginatedResponse[ProxyVoteItem],
        )
