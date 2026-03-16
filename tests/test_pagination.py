"""Tests for auto-pagination iterators."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from pydantic import BaseModel

from thesma._pagination import AsyncPaginator, SyncPaginator
from thesma._types import PaginatedResponse, PaginationMeta


class _Item(BaseModel):
    id: int
    name: str


def _make_page(
    page: int,
    total_pages: int,
    items: list[_Item],
    per_page: int = 2,
) -> PaginatedResponse:
    """Build a PaginatedResponse with the given page metadata and items."""
    return PaginatedResponse(
        data=[item.model_dump() for item in items],
        pagination=PaginationMeta(
            page=page,
            per_page=per_page,
            total=total_pages * per_page,
            total_pages=total_pages,
        ),
    )


# --- Sync paginator ---


class TestSyncPaginator:
    def test_yields_all_items_across_three_pages(self) -> None:
        items_p1 = [_Item(id=1, name="a"), _Item(id=2, name="b")]
        items_p2 = [_Item(id=3, name="c"), _Item(id=4, name="d")]
        items_p3 = [_Item(id=5, name="e"), _Item(id=6, name="f")]

        page1 = _make_page(1, 3, items_p1)
        page2 = _make_page(2, 3, items_p2)
        page3 = _make_page(3, 3, items_p3)

        fetch_page = MagicMock(side_effect=lambda p: {2: page2, 3: page3}[p])

        paginator = SyncPaginator(first_page=page1, fetch_page=fetch_page)
        result = list(paginator)

        assert len(result) == 6
        assert [r["id"] for r in result] == [1, 2, 3, 4, 5, 6]

    def test_single_page_stops_without_fetching(self) -> None:
        items = [_Item(id=1, name="a"), _Item(id=2, name="b")]
        page1 = _make_page(1, 1, items)

        fetch_page = MagicMock()

        paginator = SyncPaginator(first_page=page1, fetch_page=fetch_page)
        result = list(paginator)

        assert len(result) == 2
        fetch_page.assert_not_called()

    def test_empty_first_page_yields_nothing(self) -> None:
        page1 = PaginatedResponse(
            data=[],
            pagination=PaginationMeta(page=1, per_page=10, total=0, total_pages=1),
        )
        fetch_page = MagicMock()

        paginator = SyncPaginator(first_page=page1, fetch_page=fetch_page)
        result = list(paginator)

        assert result == []
        fetch_page.assert_not_called()

    def test_list_converts_paginator_to_list(self) -> None:
        items_p1 = [_Item(id=1, name="a")]
        items_p2 = [_Item(id=2, name="b")]

        page1 = _make_page(1, 2, items_p1, per_page=1)
        page2 = _make_page(2, 2, items_p2, per_page=1)

        fetch_page = MagicMock(return_value=page2)

        paginator = SyncPaginator(first_page=page1, fetch_page=fetch_page)
        result = list(paginator)

        assert isinstance(result, list)
        assert len(result) == 2

    def test_makes_exactly_n_requests_for_n_pages(self) -> None:
        items_p1 = [_Item(id=1, name="a")]
        items_p2 = [_Item(id=2, name="b")]
        items_p3 = [_Item(id=3, name="c")]

        page1 = _make_page(1, 3, items_p1, per_page=1)
        page2 = _make_page(2, 3, items_p2, per_page=1)
        page3 = _make_page(3, 3, items_p3, per_page=1)

        fetch_page = MagicMock(side_effect=lambda p: {2: page2, 3: page3}[p])

        paginator = SyncPaginator(first_page=page1, fetch_page=fetch_page)
        list(paginator)

        # First page is already provided, so fetch_page called for pages 2 and 3 only.
        # Total requests = 1 (original) + 2 (fetch_page) = 3 for 3 pages.
        assert fetch_page.call_count == 2
        fetch_page.assert_has_calls([call(2), call(3)])


# --- Async paginator ---


class TestAsyncPaginator:
    async def test_yields_all_items_across_three_pages(self) -> None:
        items_p1 = [_Item(id=1, name="a"), _Item(id=2, name="b")]
        items_p2 = [_Item(id=3, name="c"), _Item(id=4, name="d")]
        items_p3 = [_Item(id=5, name="e"), _Item(id=6, name="f")]

        page1 = _make_page(1, 3, items_p1)
        page2 = _make_page(2, 3, items_p2)
        page3 = _make_page(3, 3, items_p3)

        async def fetch_page(p: int) -> PaginatedResponse:
            return {2: page2, 3: page3}[p]

        paginator = AsyncPaginator(first_page=page1, fetch_page=fetch_page)
        result = [item async for item in paginator]

        assert len(result) == 6
        assert [r["id"] for r in result] == [1, 2, 3, 4, 5, 6]
