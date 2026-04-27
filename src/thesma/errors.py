"""Thesma SDK exception hierarchy."""

from __future__ import annotations

import contextlib
from typing import NamedTuple

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


class PaymentRequiredError(ThesmaError):
    """402 Payment Required — base class for any 402 response.

    Catches both ``tier_required`` (subclassed as :class:`TierRequiredError`)
    and ``plan_cap_exceeded`` (the webhook plan-cap path).
    """


class TierRequiredError(PaymentRequiredError):
    """402 with ``error_code == "tier_required"`` — the caller's tier is
    insufficient for the requested capability.

    Carries typed ``current_tier`` and ``required_tier`` attributes pulled
    from the api response body so callers can render upgrade CTAs without
    parsing the message string.
    """

    current_tier: str | None
    required_tier: str | None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = 402,
        error_code: str | None = None,
        current_tier: str | None = None,
        required_tier: str | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, error_code=error_code)
        self.current_tier = current_tier
        self.required_tier = required_tier


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


class _ParsedError(NamedTuple):
    """Structured view of a parsed error response body.

    ``message`` and ``error_code`` are populated for every dispatched error;
    the optional ``current_tier`` / ``required_tier`` fields are populated
    only when the api response carries them (currently only the
    ``tier_required`` 402 surface).
    """

    message: str
    error_code: str | None
    current_tier: str | None = None
    required_tier: str | None = None


def _parse_error_body(response: httpx.Response) -> _ParsedError:
    """Extract message, error_code, and (optionally) tier fields from a JSON error response.

    Falls back to HTTP reason phrase for non-JSON bodies (e.g. HTML proxy errors).

    Resolution priority: try the api's actual nested-error shape first
    (``{"error": {"code": "...", "message": "...", ...}}``), fall back to
    top-level keys (proxy errors / forward-compat / hypothetical api shape
    changes). Guards on ``isinstance(error_field, dict)`` so a string or
    null value under the ``error`` key doesn't raise an unguarded
    ``AttributeError``.
    """
    try:
        body = response.json()
    except Exception:
        return _ParsedError(response.reason_phrase or f"HTTP {response.status_code}", None)

    if not isinstance(body, dict):
        return _ParsedError(str(body), None)

    # Nested shape (api's actual emission for all 4xx + 429 — see api/main.py:275).
    error_field = body.get("error")
    if isinstance(error_field, dict):
        message = error_field.get("message") or error_field.get("detail") or str(error_field)
        error_code = error_field.get("code") or error_field.get("error_code")
        current_tier = error_field.get("current_tier")
        required_tier = error_field.get("required_tier")
        return _ParsedError(
            str(message),
            str(error_code) if error_code is not None else None,
            str(current_tier) if current_tier is not None else None,
            str(required_tier) if required_tier is not None else None,
        )

    # Top-level fallback (proxy errors / forward-compat / pre-nested api shapes).
    message = body.get("detail") or body.get("message") or body.get("error") or str(body)
    error_code = body.get("code") or body.get("error_code")
    current_tier = body.get("current_tier")
    required_tier = body.get("required_tier")
    return _ParsedError(
        str(message),
        str(error_code) if error_code is not None else None,
        str(current_tier) if current_tier is not None else None,
        str(required_tier) if required_tier is not None else None,
    )


def raise_for_status(response: httpx.Response) -> None:
    """Raise an appropriate :class:`ThesmaError` subclass for non-2xx responses."""
    if response.is_success:
        return

    parsed = _parse_error_body(response)
    status_code = response.status_code

    # 429 — include Retry-After header
    if status_code == 429:
        retry_after_raw = response.headers.get("Retry-After")
        retry_after: float | None = None
        if retry_after_raw is not None:
            with contextlib.suppress(ValueError, TypeError):
                retry_after = float(retry_after_raw)
        exc_cls_429: type[RateLimitError] = RateLimitError
        if parsed.error_code == "export_in_progress":
            exc_cls_429 = ExportInProgressError
        raise exc_cls_429(
            parsed.message,
            status_code=status_code,
            error_code=parsed.error_code,
            retry_after=retry_after,
        )

    # 402 — discriminate by error_code (mirrors 429 / ExportInProgressError pattern).
    if status_code == 402:
        if parsed.error_code == "tier_required":
            raise TierRequiredError(
                parsed.message,
                status_code=status_code,
                error_code=parsed.error_code,
                current_tier=parsed.current_tier,
                required_tier=parsed.required_tier,
            )
        raise PaymentRequiredError(
            parsed.message,
            status_code=status_code,
            error_code=parsed.error_code,
        )

    # Known 4xx status codes
    exc_cls = _STATUS_MAP.get(status_code)
    if exc_cls is not None:
        raise exc_cls(parsed.message, status_code=status_code, error_code=parsed.error_code)

    # 5xx
    if 500 <= status_code < 600:
        raise ServerError(parsed.message, status_code=status_code, error_code=parsed.error_code)

    # Fallback for unexpected status codes
    raise ThesmaError(parsed.message, status_code=status_code, error_code=parsed.error_code)
