"""Auto-pagination iterators for paginated API responses."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from thesma._types import PaginatedResponse

T = TypeVar("T")


class SyncPaginator(Generic[T]):
    """Synchronous lazy iterator over all pages of a paginated response.

    Each call to :meth:`PaginatedResponse.auto_paging_iter` returns a **new**
    instance with its own page counter.
    """

    def __init__(
        self,
        first_page: PaginatedResponse[T],
        fetch_page: Callable[[int], PaginatedResponse[T]],
    ) -> None:
        self._current = first_page
        self._fetch_page = fetch_page
        self._index = 0

    def __iter__(self) -> SyncPaginator[T]:
        return self

    def __next__(self) -> T:
        while True:
            if self._index < len(self._current.data):
                item = self._current.data[self._index]
                self._index += 1
                return item
            if self._current.pagination.page >= self._current.pagination.total_pages:
                raise StopIteration
            self._current = self._fetch_page(self._current.pagination.page + 1)
            self._index = 0


class AsyncPaginator(Generic[T]):
    """Asynchronous lazy iterator over all pages of a paginated response.

    Each call to :meth:`PaginatedResponse.auto_paging_iter` returns a **new**
    instance with its own page counter.
    """

    def __init__(
        self,
        first_page: PaginatedResponse[T],
        fetch_page: Callable[[int], Any],
    ) -> None:
        self._current = first_page
        self._fetch_page = fetch_page
        self._index = 0

    def __aiter__(self) -> AsyncPaginator[T]:
        return self

    async def __anext__(self) -> T:
        while True:
            if self._index < len(self._current.data):
                item = self._current.data[self._index]
                self._index += 1
                return item
            if self._current.pagination.page >= self._current.pagination.total_pages:
                raise StopAsyncIteration
            self._current = await self._fetch_page(self._current.pagination.page + 1)
            self._index = 0


__all__ = ["AsyncPaginator", "SyncPaginator"]
