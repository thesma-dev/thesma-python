"""Generic response wrapper types for the Thesma API."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, PrivateAttr, model_validator

if TYPE_CHECKING:
    from collections.abc import Callable

    from thesma._pagination import AsyncPaginator, SyncPaginator

T = TypeVar("T")


class PaginationMeta(BaseModel):
    """Pagination metadata returned by list endpoints."""

    page: int
    per_page: int
    total: int
    total_pages: int = 0

    @model_validator(mode="before")
    @classmethod
    def _compute_total_pages(cls, values: dict) -> dict:  # type: ignore[type-arg]
        """Compute total_pages from total/per_page if not provided by the API."""
        if isinstance(values, dict) and not values.get("total_pages"):
            total = values.get("total", 0)
            per_page = values.get("per_page", 1)
            values["total_pages"] = math.ceil(total / per_page) if per_page > 0 else 0
        return values


class DataResponse(BaseModel, Generic[T]):
    """Wrapper for single-object API responses: ``{"data": {...}}``."""

    data: T


class PaginatedResponse(BaseModel, Generic[T]):
    """Wrapper for paginated list API responses: ``{"data": [...], "pagination": {...}}``."""

    data: list[T]
    pagination: PaginationMeta

    _fetch_page: Callable[[int], Any] | None = PrivateAttr(default=None)
    _is_async: bool = PrivateAttr(default=False)

    def auto_paging_iter(self) -> SyncPaginator[T] | AsyncPaginator[T]:
        """Return a lazy iterator over all items across all pages.

        Each call returns a **new** iterator instance with its own page counter.
        For sync clients the returned object supports ``for item in ...``;
        for async clients it supports ``async for item in ...``.
        """
        if self._fetch_page is None:
            raise ValueError("Cannot paginate: response was not created by a client request")
        if self._is_async:
            from thesma._pagination import AsyncPaginator

            return AsyncPaginator(self, self._fetch_page)

        from thesma._pagination import SyncPaginator

        return SyncPaginator(self, self._fetch_page)

    def next_page(self) -> PaginatedResponse[T] | None:
        """Fetch and return the next page synchronously, or ``None`` if on the last page."""
        if self.pagination.page >= self.pagination.total_pages:
            return None
        if self._fetch_page is None:
            raise ValueError("Cannot paginate: response was not created by a client request")
        return self._fetch_page(self.pagination.page + 1)  # type: ignore[no-any-return]

    async def anext_page(self) -> PaginatedResponse[T] | None:
        """Fetch and return the next page asynchronously, or ``None`` if on the last page."""
        if self.pagination.page >= self.pagination.total_pages:
            return None
        if self._fetch_page is None:
            raise ValueError("Cannot paginate: response was not created by a client request")
        return await self._fetch_page(self.pagination.page + 1)  # type: ignore[no-any-return]
