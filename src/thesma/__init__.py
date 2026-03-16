"""Thesma Python SDK — developer-friendly access to SEC EDGAR financial data."""

from __future__ import annotations

from thesma._version import __version__


class ThesmaClient:
    """Synchronous client for the Thesma API.

    Placeholder — real implementation in SDK-03.
    """

    def __init__(self, api_key: str, *, base_url: str = "https://api.thesma.dev", timeout: int = 30) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout


class AsyncThesmaClient:
    """Asynchronous client for the Thesma API.

    Placeholder — real implementation in SDK-03.
    """

    def __init__(self, api_key: str, *, base_url: str = "https://api.thesma.dev", timeout: int = 30) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout


__all__ = [
    "AsyncThesmaClient",
    "ThesmaClient",
    "__version__",
]
