"""Tests for generic response wrapper types."""

from __future__ import annotations

from pydantic import BaseModel

from thesma._types import DataResponse, PaginatedResponse, PaginationMeta


class _Item(BaseModel):
    name: str
    value: int


class TestDataResponse:
    def test_deserializes_single_object(self) -> None:
        raw = {"data": {"name": "Widget", "value": 42}}
        resp = DataResponse[_Item].model_validate(raw)
        assert isinstance(resp.data, _Item)
        assert resp.data.name == "Widget"
        assert resp.data.value == 42


class TestPaginatedResponse:
    def test_deserializes_list_with_pagination(self) -> None:
        raw = {
            "data": [
                {"name": "A", "value": 1},
                {"name": "B", "value": 2},
            ],
            "pagination": {
                "page": 1,
                "per_page": 25,
                "total": 2,
                "total_pages": 1,
            },
        }
        resp = PaginatedResponse[_Item].model_validate(raw)
        assert len(resp.data) == 2
        assert resp.data[0].name == "A"
        assert resp.data[1].value == 2
        assert isinstance(resp.pagination, PaginationMeta)
        assert resp.pagination.page == 1
        assert resp.pagination.total == 2
        assert resp.pagination.total_pages == 1
