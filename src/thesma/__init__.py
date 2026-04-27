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
    PaymentRequiredError,
    RateLimitError,
    ServerError,
    ThesmaError,
    TierRequiredError,
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
    "PaymentRequiredError",
    "RateLimitError",
    "ServerError",
    "ThesmaClient",
    "ThesmaError",
    "TierRequiredError",
    "TimeoutError",
    "__version__",
]
