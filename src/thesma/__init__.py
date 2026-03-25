"""Thesma Python SDK — developer-friendly access to SEC EDGAR financial data."""

from __future__ import annotations

from thesma._types import DataResponse, PaginatedResponse, PaginationMeta
from thesma._version import __version__
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import (
    AuthenticationError,
    BadRequestError,
    ConnectionError,
    ExportInProgressError,
    ForbiddenError,
    NotFoundError,
    RateLimitError,
    ServerError,
    ThesmaError,
    TimeoutError,
)

__all__ = [
    "AsyncThesmaClient",
    "AuthenticationError",
    "BadRequestError",
    "ConnectionError",
    "DataResponse",
    "ExportInProgressError",
    "ForbiddenError",
    "NotFoundError",
    "PaginatedResponse",
    "PaginationMeta",
    "RateLimitError",
    "ServerError",
    "ThesmaClient",
    "ThesmaError",
    "TimeoutError",
    "__version__",
]
