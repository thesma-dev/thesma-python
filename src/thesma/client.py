"""Public client classes for the Thesma API."""

from __future__ import annotations

import asyncio
import random
import threading
import time
import warnings
from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from thesma._auth import auth_headers, validate_api_key
from thesma._base_client import AsyncAPIClient, SyncAPIClient
from thesma._types import PaginatedResponse
from thesma.errors import ConnectionError as ThesmaConnectionError
from thesma.errors import ExportInProgressError, raise_for_status
from thesma.errors import TimeoutError as ThesmaTimeoutError
from thesma.resources.beneficial_ownership import BeneficialOwnership
from thesma.resources.census import Census
from thesma.resources.companies import Companies
from thesma.resources.compensation import Compensation
from thesma.resources.events import Events
from thesma.resources.export import AsyncExport, Export
from thesma.resources.filings import Filings
from thesma.resources.financials import Financials
from thesma.resources.holdings import Holdings
from thesma.resources.insider_holdings import InsiderHoldings
from thesma.resources.insider_trades import InsiderTrades
from thesma.resources.proxy_votes import ProxyVotes
from thesma.resources.ratios import Ratios
from thesma.resources.screener import Screener
from thesma.resources.sections import Sections
from thesma.resources.webhooks import Webhooks

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
        auto_retry: bool = False,
        max_retries: int = 0,
        stream_timeout: int = 300,
    ) -> None:
        self.api_key = validate_api_key(api_key)
        self.base_url = base_url
        self.timeout = timeout
        self.auto_retry = auto_retry
        self.max_retries = max_retries
        self.stream_timeout = stream_timeout
        self._client: httpx.Client | None = None
        self._lock = threading.Lock()

        self.beneficial_ownership = BeneficialOwnership(self)
        self.census = Census(self)
        self.companies = Companies(self)
        self.compensation = Compensation(self)
        self.events = Events(self)
        self.export = Export(self)
        self.filings = Filings(self)
        self.financials = Financials(self)
        self.holdings = Holdings(self)
        self.insider_holdings = InsiderHoldings(self)
        self.insider_trades = InsiderTrades(self)
        self.proxy_votes = ProxyVotes(self)
        self.ratios = Ratios(self)
        self.screener = Screener(self)
        self.sections = Sections(self)
        self.webhooks = Webhooks(self)

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
        return SyncAPIClient(
            self._ensure_client(),
            auto_retry=self.auto_retry,
            max_retries=self.max_retries,
        )

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
        result = self._api.request(
            method,
            path,
            params=params,
            json=json,
            response_model=response_model,
        )
        if isinstance(result, PaginatedResponse):

            def _fetch_page(page: int) -> Any:
                new_params = {**(params or {}), "page": page}
                return self.request(method, path, params=new_params, json=json, response_model=response_model)

            result._fetch_page = _fetch_page
        return result

    # -- streaming request (used by Export resource) --

    def _stream_get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """Send a streaming GET request, returning an open httpx.Response.

        The caller is responsible for closing the response. Uses an extended
        read timeout (configurable via ``stream_timeout``) for long-running
        export downloads. Retries up to 6 times on ``export_in_progress`` 429s.
        """
        _MAX_EXPORT_RETRIES = 6
        _DEFAULT_RETRY_AFTER = 30.0

        client = self._ensure_client()
        timeout = httpx.Timeout(
            connect=float(self.timeout),
            read=float(self.stream_timeout),
            write=float(self.timeout),
            pool=float(self.timeout),
        )
        stripped = {k: v for k, v in (params or {}).items() if v is not None} or None

        for attempt in range(_MAX_EXPORT_RETRIES + 1):
            request = client.build_request("GET", path, params=stripped)
            request.extensions["timeout"] = timeout.as_dict()
            try:
                response = client.send(request, stream=True)
            except httpx.TimeoutException as exc:
                raise ThesmaTimeoutError(str(exc)) from exc
            except httpx.RequestError as exc:
                raise ThesmaConnectionError(str(exc)) from exc

            if not response.is_success:
                response.read()
                response.close()
                try:
                    raise_for_status(response)
                except ExportInProgressError as exc:
                    if attempt >= _MAX_EXPORT_RETRIES:
                        raise
                    retry_after = exc.retry_after if exc.retry_after is not None else _DEFAULT_RETRY_AFTER
                    time.sleep(retry_after + random.uniform(0, 0.5))
                    continue

            return response

        raise RuntimeError("unreachable")  # pragma: no cover


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
        auto_retry: bool = False,
        max_retries: int = 0,
        stream_timeout: int = 300,
    ) -> None:
        self.api_key = validate_api_key(api_key)
        self.base_url = base_url
        self.timeout = timeout
        self.auto_retry = auto_retry
        self.max_retries = max_retries
        self.stream_timeout = stream_timeout
        self._client: httpx.AsyncClient | None = None
        self._closed = False

        self.beneficial_ownership = BeneficialOwnership(self)
        self.census = Census(self)
        self.companies = Companies(self)
        self.compensation = Compensation(self)
        self.events = Events(self)
        self.export = AsyncExport(self)
        self.filings = Filings(self)
        self.financials = Financials(self)
        self.holdings = Holdings(self)
        self.insider_holdings = InsiderHoldings(self)
        self.insider_trades = InsiderTrades(self)
        self.proxy_votes = ProxyVotes(self)
        self.ratios = Ratios(self)
        self.screener = Screener(self)
        self.sections = Sections(self)
        self.webhooks = Webhooks(self)

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
        return AsyncAPIClient(
            self._ensure_client(),
            auto_retry=self.auto_retry,
            max_retries=self.max_retries,
        )

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
        result = await self._api.request(
            method,
            path,
            params=params,
            json=json,
            response_model=response_model,
        )
        if isinstance(result, PaginatedResponse):

            async def _fetch_page(page: int) -> Any:
                new_params = {**(params or {}), "page": page}
                return await self.request(method, path, params=new_params, json=json, response_model=response_model)

            result._fetch_page = _fetch_page
            result._is_async = True
        return result

    # -- streaming request (used by AsyncExport resource) --

    async def _async_stream_get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """Send an async streaming GET request, returning an open httpx.Response.

        The caller is responsible for closing the response. Retries up to 6
        times on ``export_in_progress`` 429s.
        """
        _MAX_EXPORT_RETRIES = 6
        _DEFAULT_RETRY_AFTER = 30.0

        client = self._ensure_client()
        timeout = httpx.Timeout(
            connect=float(self.timeout),
            read=float(self.stream_timeout),
            write=float(self.timeout),
            pool=float(self.timeout),
        )
        stripped = {k: v for k, v in (params or {}).items() if v is not None} or None

        for attempt in range(_MAX_EXPORT_RETRIES + 1):
            request = client.build_request("GET", path, params=stripped)
            request.extensions["timeout"] = timeout.as_dict()
            try:
                response = await client.send(request, stream=True)
            except httpx.TimeoutException as exc:
                raise ThesmaTimeoutError(str(exc)) from exc
            except httpx.RequestError as exc:
                raise ThesmaConnectionError(str(exc)) from exc

            if not response.is_success:
                await response.aread()
                await response.aclose()
                try:
                    raise_for_status(response)
                except ExportInProgressError as exc:
                    if attempt >= _MAX_EXPORT_RETRIES:
                        raise
                    retry_after = exc.retry_after if exc.retry_after is not None else _DEFAULT_RETRY_AFTER
                    await asyncio.sleep(retry_after + random.uniform(0, 0.5))
                    continue

            return response

        raise RuntimeError("unreachable")  # pragma: no cover
