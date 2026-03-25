"""Thesma SDK exception hierarchy."""

from __future__ import annotations

import contextlib

import httpx


class ThesmaError(Exception):
    """Base exception for all Thesma SDK errors."""

    status_code: int | None
    message: str
    error_code: str | None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


# --- Network errors ---


class ConnectionError(ThesmaError):
    """Network, DNS, or SSL failure."""


class TimeoutError(ThesmaError):
    """Request timed out."""


# --- HTTP status errors ---


class BadRequestError(ThesmaError):
    """400 Bad Request."""


class AuthenticationError(ThesmaError):
    """401 Unauthorized — invalid or missing API key."""


class ForbiddenError(ThesmaError):
    """403 Forbidden."""


class NotFoundError(ThesmaError):
    """404 Not Found."""


class RateLimitError(ThesmaError):
    """429 Too Many Requests."""

    retry_after: float | None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = 429,
        error_code: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, error_code=error_code)
        self.retry_after = retry_after


class ExportInProgressError(RateLimitError):
    """429 with error_code ``export_in_progress`` — a previous export is still active."""


class ServerError(ThesmaError):
    """5xx Server Error."""


_STATUS_MAP: dict[int, type[ThesmaError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    429: RateLimitError,
}


def _parse_error_body(response: httpx.Response) -> tuple[str, str | None]:
    """Extract message and error_code from a JSON error response.

    Falls back to HTTP reason phrase for non-JSON bodies (e.g. HTML proxy errors).
    """
    try:
        body = response.json()
    except Exception:
        return response.reason_phrase or f"HTTP {response.status_code}", None

    if isinstance(body, dict):
        message = body.get("detail") or body.get("message") or body.get("error") or str(body)
        error_code = body.get("code") or body.get("error_code")
        return str(message), str(error_code) if error_code is not None else None

    return str(body), None


def raise_for_status(response: httpx.Response) -> None:
    """Raise an appropriate :class:`ThesmaError` subclass for non-2xx responses."""
    if response.is_success:
        return

    message, error_code = _parse_error_body(response)
    status_code = response.status_code

    # 429 — include Retry-After header
    if status_code == 429:
        retry_after_raw = response.headers.get("Retry-After")
        retry_after: float | None = None
        if retry_after_raw is not None:
            with contextlib.suppress(ValueError, TypeError):
                retry_after = float(retry_after_raw)
        exc_cls_429: type[RateLimitError] = RateLimitError
        if error_code == "export_in_progress":
            exc_cls_429 = ExportInProgressError
        raise exc_cls_429(
            message,
            status_code=status_code,
            error_code=error_code,
            retry_after=retry_after,
        )

    # Known 4xx status codes
    exc_cls = _STATUS_MAP.get(status_code)
    if exc_cls is not None:
        raise exc_cls(message, status_code=status_code, error_code=error_code)

    # 5xx
    if 500 <= status_code < 600:
        raise ServerError(message, status_code=status_code, error_code=error_code)

    # Fallback for unexpected status codes
    raise ThesmaError(message, status_code=status_code, error_code=error_code)
