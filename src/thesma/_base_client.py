"""Internal HTTP engine — sync and async base clients."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from thesma._auth import auth_headers
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

    def __init__(self, httpx_client: httpx.Client) -> None:
        self._client = httpx_client

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


class AsyncAPIClient:
    """Asynchronous HTTP client used by :class:`AsyncThesmaClient`."""

    def __init__(self, httpx_client: httpx.AsyncClient) -> None:
        self._client = httpx_client

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


__all__ = ["AsyncAPIClient", "SyncAPIClient", "auth_headers"]
