"""Internal HTTP engine — sync and async base clients."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from thesma._auth import auth_headers
from thesma._retry import async_retry, sync_retry
from thesma.errors import ConnectionError as ThesmaConnectionError
from thesma.errors import TimeoutError as ThesmaTimeoutError
from thesma.errors import raise_for_status

if TYPE_CHECKING:
    from pydantic import BaseModel

T = TypeVar("T", bound="BaseModel")


def _strip_none(params: dict[str, Any] | None) -> dict[str, Any] | None:
    """Remove keys whose values are ``None``."""
    if params is None:
        return None
    stripped = {k: v for k, v in params.items() if v is not None}
    return stripped or None


class SyncAPIClient:
    """Synchronous HTTP client used by :class:`ThesmaClient`."""

    def __init__(
        self,
        httpx_client: httpx.Client,
        *,
        auto_retry: bool = False,
        max_retries: int = 0,
    ) -> None:
        self._client = httpx_client
        self._auto_retry = auto_retry
        self._max_retries = max_retries

    def _do_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Execute a single HTTP request (no retry)."""
        try:
            response = self._client.request(
                method,
                path,
                params=_strip_none(params),
                json=json,
            )
        except httpx.TimeoutException as exc:
            raise ThesmaTimeoutError(str(exc) or "Request timed out") from exc
        except httpx.RequestError as exc:
            raise ThesmaConnectionError(str(exc) or "Connection error") from exc

        raise_for_status(response)

        if response_model is None:
            return None

        return response_model.model_validate(response.json())

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Send an HTTP request and return the parsed response model."""
        if self._auto_retry:
            return sync_retry(
                lambda: self._do_request(method, path, params=params, json=json, response_model=response_model),
                self._max_retries,
            )
        return self._do_request(method, path, params=params, json=json, response_model=response_model)


class AsyncAPIClient:
    """Asynchronous HTTP client used by :class:`AsyncThesmaClient`."""

    def __init__(
        self,
        httpx_client: httpx.AsyncClient,
        *,
        auto_retry: bool = False,
        max_retries: int = 0,
    ) -> None:
        self._client = httpx_client
        self._auto_retry = auto_retry
        self._max_retries = max_retries

    async def _do_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Execute a single HTTP request (no retry)."""
        try:
            response = await self._client.request(
                method,
                path,
                params=_strip_none(params),
                json=json,
            )
        except httpx.TimeoutException as exc:
            raise ThesmaTimeoutError(str(exc) or "Request timed out") from exc
        except httpx.RequestError as exc:
            raise ThesmaConnectionError(str(exc) or "Connection error") from exc

        raise_for_status(response)

        if response_model is None:
            return None

        return response_model.model_validate(response.json())

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        response_model: type[T] | None = None,
    ) -> T | None:
        """Send an HTTP request and return the parsed response model."""
        if self._auto_retry:
            return await async_retry(
                lambda: self._do_request(method, path, params=params, json=json, response_model=response_model),
                self._max_retries,
            )
        return await self._do_request(method, path, params=params, json=json, response_model=response_model)


__all__ = ["AsyncAPIClient", "SyncAPIClient", "auth_headers"]
