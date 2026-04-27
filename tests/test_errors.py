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
    PaymentRequiredError,
    RateLimitError,
    ServerError,
    ThesmaError,
    TierRequiredError,
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


# --- SDK-39: 402 dispatch + tier-error attrs ---


def _make_402(code: str | None, current_tier: str | None = None, required_tier: str | None = None) -> httpx.Response:
    """Build a 402 response matching the api's actual body shape."""
    error: dict = {"code": code, "message": "test message", "status": 402}
    if current_tier is not None:
        error["current_tier"] = current_tier
    if required_tier is not None:
        error["required_tier"] = required_tier
    return httpx.Response(402, json={"error": error})


class TestTierRequiredErrorDispatch:
    """tier_required → TierRequiredError with all 4 attrs."""

    def test_tier_required_402_raises_typed(self):
        resp = _make_402("tier_required", current_tier="free", required_tier="pro")
        with pytest.raises(TierRequiredError) as exc_info:
            raise_for_status(resp)
        e = exc_info.value
        assert e.status_code == 402
        assert e.error_code == "tier_required"
        assert e.current_tier == "free"
        assert e.required_tier == "pro"
        assert e.message == "test message"

    def test_tier_required_inherits_payment_required(self):
        """Class hierarchy — TierRequiredError IS-A PaymentRequiredError IS-A ThesmaError."""
        assert issubclass(TierRequiredError, PaymentRequiredError)
        assert issubclass(PaymentRequiredError, ThesmaError)

    def test_tier_required_with_unknown_tier_value(self):
        """API emits 'unknown' for corrupt-state plans — SDK passes it through."""
        resp = _make_402("tier_required", current_tier="unknown", required_tier="pro")
        with pytest.raises(TierRequiredError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.current_tier == "unknown"


class TestPaymentRequiredErrorDispatch:
    """Non-tier 402s → generic PaymentRequiredError, NOT TierRequiredError."""

    def test_plan_cap_exceeded_raises_payment_required_not_tier(self):
        """Webhook plan-cap 402 → PaymentRequiredError. Don't accidentally upcast to TierRequiredError."""
        resp = _make_402("plan_cap_exceeded")
        with pytest.raises(PaymentRequiredError) as exc_info:
            raise_for_status(resp)
        e = exc_info.value
        assert not isinstance(e, TierRequiredError)
        assert e.status_code == 402
        assert e.error_code == "plan_cap_exceeded"

    def test_unknown_402_code_raises_payment_required(self):
        """Future-proof: an unknown error_code on 402 falls back to PaymentRequiredError."""
        resp = _make_402("future_unknown_code")
        with pytest.raises(PaymentRequiredError) as exc_info:
            raise_for_status(resp)
        assert not isinstance(exc_info.value, TierRequiredError)
        assert exc_info.value.error_code == "future_unknown_code"

    def test_402_with_no_error_code_raises_payment_required(self):
        """Defensive: 402 with no `code` field (malformed/proxy) → PaymentRequiredError, code=None."""
        resp = httpx.Response(402, json={"error": {"message": "no code here", "status": 402}})
        with pytest.raises(PaymentRequiredError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.error_code is None


class TestExistingStatusCodesUnchanged:
    """Regression — existing 400/401/403/404/429 paths must not be perturbed."""

    @pytest.mark.parametrize(
        "status,expected_msg_substring",
        [(400, "bad"), (401, "auth"), (403, "forbid"), (404, "not found"), (429, "rate")],
    )
    def test_other_4xx_unchanged(self, status, expected_msg_substring):
        # Just ensure the dispatcher still raises SOMETHING for these — exact class
        # tested in existing tests.
        resp = httpx.Response(status, json={"error": {"code": "x", "message": expected_msg_substring}})
        with pytest.raises(ThesmaError):
            raise_for_status(resp)


class TestErrorBodyParsing:
    """Confirms the body-shape extraction works against the api's actual nested-error response."""

    def test_extracts_current_tier_from_nested_error_object(self):
        resp = _make_402("tier_required", current_tier="starter", required_tier="pro")
        with pytest.raises(TierRequiredError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.current_tier == "starter"

    def test_handles_missing_tier_fields_gracefully(self):
        """tier_required code with no tier fields (defensive) → still raises Tier with None attrs."""
        resp = httpx.Response(402, json={"error": {"code": "tier_required", "message": "x", "status": 402}})
        with pytest.raises(TierRequiredError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.current_tier is None
        assert exc_info.value.required_tier is None

    def test_non_json_402_body_falls_back_to_payment_required(self):
        """A CDN/proxy returning 402 with HTML/text body must surface as PaymentRequiredError
        (not crash inside _parse_error_body). error_code is None; message comes from
        reason_phrase. Guards against future _parse_error_body refactors."""
        resp = httpx.Response(402, content=b"<html>Pay here</html>", headers={"Content-Type": "text/html"})
        with pytest.raises(PaymentRequiredError) as exc_info:
            raise_for_status(resp)
        assert not isinstance(exc_info.value, TierRequiredError)
        assert exc_info.value.error_code is None

    def test_402_with_string_error_field_does_not_raise_attributeerror(self):
        """Defensive: api hypothetically emits {"error": "tier_required"} (string, not dict).
        The nested-error walk MUST guard isinstance(error_field, dict) — without the guard,
        .get("code") on a string raises raw AttributeError that customer except ThesmaError
        clauses don't catch."""
        resp = httpx.Response(402, json={"error": "tier_required"})
        with pytest.raises(PaymentRequiredError) as exc_info:
            raise_for_status(resp)
        # Should NOT discriminate to TierRequiredError because the string value isn't a dict
        # to walk into for code extraction.
        assert not isinstance(exc_info.value, TierRequiredError)

    def test_402_with_null_error_field(self):
        """Defensive: {"error": null} — same isinstance guard requirement as above."""
        resp = httpx.Response(402, json={"error": None})
        with pytest.raises(PaymentRequiredError):
            raise_for_status(resp)

    def test_402_with_top_level_code_key(self):
        """Forward compat: if api ever emits {"code": "...", ...} at top level (older shape
        that _parse_error_body still handles via line 106 today), it must continue to work."""
        resp = httpx.Response(402, json={"code": "tier_required", "message": "x"})
        # The top-level extraction path doesn't have current_tier/required_tier fields,
        # so this should raise TierRequiredError with None tier attrs (same as the
        # missing-tier-fields case but with a different body shape).
        with pytest.raises(TierRequiredError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.current_tier is None


class TestExportInProgressErrorRevival:
    """SDK-39 side-effect regression: with the parser fix, the existing
    `if error_code == "export_in_progress"` discriminator at errors.py:128 can
    actually fire against api responses for the first time. Pre-SDK-39 this branch
    was dead code in production because `_parse_error_body` extracted `error_code=None`
    against the api's nested-error shape. The CHANGELOG `### Fixed` block calls
    out the revival; this test pins it."""

    def test_export_in_progress_429_under_nested_shape_dispatches_correctly(self):
        """429 + nested error.code = 'export_in_progress' → ExportInProgressError
        (NOT plain RateLimitError). Retry-After header survives the dispatch."""
        resp = httpx.Response(
            429,
            json={"error": {"code": "export_in_progress", "message": "Export still running.", "status": 429}},
            headers={"Retry-After": "30"},
        )
        with pytest.raises(ExportInProgressError) as exc_info:
            raise_for_status(resp)
        e = exc_info.value
        assert e.retry_after == 30.0
        assert e.error_code == "export_in_progress"
        assert e.status_code == 429
        # Subclass relationship: ExportInProgressError ⊂ RateLimitError ⊂ ThesmaError.
        assert isinstance(e, RateLimitError)
        assert isinstance(e, ThesmaError)

    def test_plain_rate_limit_429_under_nested_shape_does_not_upcast(self):
        """A 429 with a non-export error.code (or no code) must NOT dispatch to
        ExportInProgressError — only the discriminator code triggers the subclass."""
        resp = httpx.Response(
            429,
            json={"error": {"code": "rate_limit_exceeded", "message": "slow down", "status": 429}},
            headers={"Retry-After": "5"},
        )
        with pytest.raises(RateLimitError) as exc_info:
            raise_for_status(resp)
        assert not isinstance(exc_info.value, ExportInProgressError)
        assert exc_info.value.error_code == "rate_limit_exceeded"
        assert exc_info.value.retry_after == 5.0
