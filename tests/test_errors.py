"""Tests for the error hierarchy and raise_for_status mapping."""

from __future__ import annotations

import httpx
import pytest
import respx
from pydantic import BaseModel

from thesma._base_client import SyncAPIClient
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
    raise_for_status,
)

# --- Inheritance ---


class TestErrorInheritance:
    def test_all_errors_inherit_from_thesma_error(self) -> None:
        for cls in (
            BadRequestError,
            AuthenticationError,
            ForbiddenError,
            NotFoundError,
            RateLimitError,
            ServerError,
            ConnectionError,
            TimeoutError,
        ):
            assert issubclass(cls, ThesmaError)

    def test_thesma_error_has_status_code_and_message(self) -> None:
        err = ThesmaError("something broke", status_code=500, error_code="INTERNAL")
        assert err.status_code == 500
        assert err.message == "something broke"
        assert err.error_code == "INTERNAL"
        assert str(err) == "something broke"


# --- ExportInProgressError ---


class TestExportInProgressError:
    def test_export_in_progress_error_is_rate_limit_subclass(self) -> None:
        assert issubclass(ExportInProgressError, RateLimitError)

    def test_raise_for_status_export_in_progress(self) -> None:
        resp = httpx.Response(
            429,
            json={"detail": "Export in progress", "code": "export_in_progress"},
            headers={"Retry-After": "60"},
        )
        with pytest.raises(ExportInProgressError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.retry_after == 60.0

    def test_raise_for_status_429_without_export_code(self) -> None:
        resp = httpx.Response(
            429,
            json={"detail": "Rate limit exceeded"},
        )
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(resp)
        assert type(exc_info.value) is RateLimitError


# --- raise_for_status mapping ---


class TestRaiseForStatus:
    def _response(
        self,
        status_code: int,
        *,
        json_body: dict[str, str] | None = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        if json_body is not None:
            return httpx.Response(
                status_code,
                json=json_body,
                headers=headers or {},
            )
        return httpx.Response(
            status_code,
            text=text or "",
            headers=headers or {},
        )

    def test_400_raises_bad_request_error(self) -> None:
        resp = self._response(400, json_body={"detail": "Invalid CIK"})
        with pytest.raises(BadRequestError, match="Invalid CIK") as exc_info:
            raise_for_status(resp)
        assert exc_info.value.status_code == 400

    def test_401_raises_authentication_error(self) -> None:
        resp = self._response(401, json_body={"detail": "Invalid API key"})
        with pytest.raises(AuthenticationError):
            raise_for_status(resp)

    def test_403_raises_forbidden_error(self) -> None:
        resp = self._response(403, json_body={"detail": "Forbidden"})
        with pytest.raises(ForbiddenError):
            raise_for_status(resp)

    def test_404_raises_not_found_error(self) -> None:
        resp = self._response(404, json_body={"detail": "Company not found"})
        with pytest.raises(NotFoundError, match="Company not found") as exc_info:
            raise_for_status(resp)
        assert exc_info.value.status_code == 404

    def test_429_raises_rate_limit_error_with_retry_after(self) -> None:
        resp = self._response(
            429,
            json_body={"detail": "Rate limit exceeded"},
            headers={"Retry-After": "30"},
        )
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.retry_after == 30.0
        assert exc_info.value.status_code == 429

    def test_429_without_retry_after_header(self) -> None:
        resp = self._response(429, json_body={"detail": "Rate limit exceeded"})
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.retry_after is None

    def test_500_raises_server_error(self) -> None:
        resp = self._response(500, json_body={"detail": "Internal server error"})
        with pytest.raises(ServerError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.status_code == 500

    def test_502_html_body_falls_back_to_reason_phrase(self) -> None:
        resp = httpx.Response(
            502,
            text="<html><body>Bad Gateway</body></html>",
            headers={"content-type": "text/html"},
        )
        with pytest.raises(ServerError, match="Bad Gateway"):
            raise_for_status(resp)

    def test_503_raises_server_error(self) -> None:
        resp = self._response(503, json_body={"detail": "Service unavailable"})
        with pytest.raises(ServerError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.status_code == 503

    def test_2xx_does_not_raise(self) -> None:
        resp = self._response(200, json_body={"data": "ok"})
        raise_for_status(resp)  # should not raise


# --- Network errors via base client ---


class _DummyModel(BaseModel):
    value: str


class TestNetworkErrors:
    @respx.mock
    def test_timeout_raises_thesma_timeout_error(self) -> None:
        respx.get("https://api.thesma.dev/v1/test").mock(side_effect=httpx.ReadTimeout("timed out"))
        client = httpx.Client(base_url="https://api.thesma.dev", headers={"X-API-Key": "test"})
        api = SyncAPIClient(client)
        with pytest.raises(TimeoutError):
            api.request("GET", "/v1/test", response_model=_DummyModel)
        client.close()

    @respx.mock
    def test_connect_error_raises_thesma_connection_error(self) -> None:
        respx.get("https://api.thesma.dev/v1/test").mock(side_effect=httpx.ConnectError("connection refused"))
        client = httpx.Client(base_url="https://api.thesma.dev", headers={"X-API-Key": "test"})
        api = SyncAPIClient(client)
        with pytest.raises(ConnectionError):
            api.request("GET", "/v1/test", response_model=_DummyModel)
        client.close()


# --- 204 No Content ---


class TestNoContent:
    @respx.mock
    def test_204_no_content_returns_none(self) -> None:
        respx.delete("https://api.thesma.dev/v1/test").mock(return_value=httpx.Response(204))
        client = httpx.Client(base_url="https://api.thesma.dev", headers={"X-API-Key": "test"})
        api = SyncAPIClient(client)
        result = api.request("DELETE", "/v1/test", response_model=None)
        assert result is None
        client.close()
