"""Insider trades resource — SEC Form 4 transaction data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import InsiderTradeListItem
from thesma._types import PaginatedResponse


class InsiderTrades:
    """Resource for insider trade endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        cik: str,
        *,
        trade_type: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[InsiderTradeListItem]:
        """List insider trades for a company.

        ``GET /v1/us/sec/companies/{cik}/insider-trades``
        """
        params: dict[str, Any] = {
            "type": trade_type,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/insider-trades",
            params=params,
            response_model=PaginatedResponse[InsiderTradeListItem],
        )

    def list_all(
        self,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[InsiderTradeListItem]:
        """List insider trades across all companies.

        ``GET /v1/us/sec/insider-trades``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/insider-trades",
            params=params,
            response_model=PaginatedResponse[InsiderTradeListItem],
        )
