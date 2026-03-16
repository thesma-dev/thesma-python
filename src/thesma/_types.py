"""Generic response wrapper types for the Thesma API."""

from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginationMeta(BaseModel):
    """Pagination metadata returned by list endpoints."""

    page: int
    per_page: int
    total: int
    total_pages: int


class DataResponse(BaseModel, Generic[T]):
    """Wrapper for single-object API responses: ``{"data": {...}}``."""

    data: T


class PaginatedResponse(BaseModel, Generic[T]):
    """Wrapper for paginated list API responses: ``{"data": [...], "pagination": {...}}``."""

    data: list[T]
    pagination: PaginationMeta
