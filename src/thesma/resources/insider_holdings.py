"""Insider holdings resource — SEC insider ownership positions."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import InsiderHoldingListItem
from thesma._types import PaginatedResponse


class InsiderHoldings:
    """Resource for insider holding endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        cik: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[InsiderHoldingListItem]:
        """List insider holdings for a company.

        ``GET /v1/us/sec/companies/{cik}/insider-holdings``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/insider-holdings",
            params=params,
            response_model=PaginatedResponse[InsiderHoldingListItem],
        )
