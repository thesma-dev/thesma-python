"""Events resource — SEC 8-K corporate events."""

from __future__ import annotations

import builtins
from typing import Any

from thesma._generated.models import EventCategory, EventListItem
from thesma._types import DataResponse, PaginatedResponse


class Events:
    """Resource for corporate event endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        cik: str,
        *,
        category: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[EventListItem]:
        """List events for a company.

        ``GET /v1/us/sec/companies/{cik}/events``
        """
        params: dict[str, Any] = {
            "category": category,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/events",
            params=params,
            response_model=PaginatedResponse[EventListItem],
        )

    def list_all(
        self,
        *,
        category: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[EventListItem]:
        """List events across all companies.

        ``GET /v1/us/sec/events``
        """
        params: dict[str, Any] = {
            "category": category,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/events",
            params=params,
            response_model=PaginatedResponse[EventListItem],
        )

    def categories(self) -> DataResponse[builtins.list[EventCategory]]:
        """List all event categories.

        ``GET /v1/us/sec/events/categories``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/events/categories",
            response_model=DataResponse[builtins.list[EventCategory]],
        )
