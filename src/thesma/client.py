"""Public client classes for the Thesma API."""

from __future__ import annotations

import threading
import warnings
from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from thesma._auth import auth_headers, validate_api_key
from thesma._base_client import AsyncAPIClient, SyncAPIClient

if TYPE_CHECKING:
    from pydantic import BaseModel

T = TypeVar("T", bound="BaseModel")


def _redact_key(api_key: str) -> str:
    """Return a redacted version of the API key for repr."""
    if len(api_key) <= 8:
        return "***"
    return api_key[:8] + "...***"


class ThesmaClient:
    """Synchronous client for the Thesma API.

    Usage::

        client = ThesmaClient(api_key="th_live_...")
        # or as a context manager:
        with ThesmaClient(api_key="th_live_...") as client:
            ...
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.thesma.dev",
        timeout: int = 30,
    ) -> None:
        self.api_key = validate_api_key(api_key)
        self.base_url = base_url
        self.timeout = timeout
        self._client: httpx.Client | None = None
        self._lock = threading.Lock()

    # -- lazy httpx.Client init (thread-safe) --

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = httpx.Client(
                        base_url=self.base_url,
                        timeout=self.timeout,
                        headers=auth_headers(self.api_key),
                    )
        return self._client

    @property
    def _api(self) -> SyncAPIClient:
        return SyncAPIClient(self._ensure_client())

    # -- context manager --

    def __enter__(self) -> ThesmaClient:
        self._ensure_client()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- repr --

    def __repr__(self) -> str:
        return f"ThesmaClient(api_key='{_redact_key(self.api_key)}')"

    # -- public request method (used by resource namespaces) --

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Send an HTTP request via the sync API client."""
        return self._api.request(
            method,
            path,
            params=params,
            json=json,
            response_model=response_model,
        )


class AsyncThesmaClient:
    """Asynchronous client for the Thesma API.

    Usage::

        async with AsyncThesmaClient(api_key="th_live_...") as client:
            ...
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.thesma.dev",
        timeout: int = 30,
    ) -> None:
        self.api_key = validate_api_key(api_key)
        self.base_url = base_url
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._closed = False

    # -- lazy httpx.AsyncClient init --

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers=auth_headers(self.api_key),
            )
        return self._client

    @property
    def _api(self) -> AsyncAPIClient:
        return AsyncAPIClient(self._ensure_client())

    # -- async context manager --

    async def __aenter__(self) -> AsyncThesmaClient:
        self._ensure_client()
        return self

    async def __aexit__(self, *_args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying async HTTP connection pool."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._closed = True

    def __del__(self) -> None:
        if getattr(self, "_client", None) is not None and not getattr(self, "_closed", True):
            warnings.warn(
                "AsyncThesmaClient was not closed. Use 'async with' or call 'await client.close()'.",
                ResourceWarning,
                stacklevel=1,
            )

    # -- repr --

    def __repr__(self) -> str:
        return f"AsyncThesmaClient(api_key='{_redact_key(self.api_key)}')"

    # -- public request method (used by resource namespaces) --

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Send an HTTP request via the async API client."""
        return await self._api.request(
            method,
            path,
            params=params,
            json=json,
            response_model=response_model,
        )
