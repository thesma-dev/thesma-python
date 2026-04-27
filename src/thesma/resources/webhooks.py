"""Webhooks resource — manage webhook subscriptions and deliveries."""

from __future__ import annotations

import builtins
from typing import Any

from thesma._generated.models import (
    WebhookCreateResponse,
    WebhookDeliveryResponse,
    WebhookEventTypeResponse,
    WebhookReplayResponse,
    WebhookResponse,
    WebhookSecretRotateResponse,
    WebhookTestResponse,
)
from thesma._types import DataResponse, PaginatedResponse


class Webhooks:
    """Resource for webhook subscription and delivery endpoints.

    Note: 402 (plan-cap) and 410 (delivery outside retention window) are
    surfaced as the generic :class:`thesma.errors.ThesmaError` rather than
    typed subclasses. Callers needing to handle those cases specifically
    must catch ``ThesmaError`` and inspect ``status_code`` / ``error_code``.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def list_event_types(self) -> DataResponse[builtins.list[WebhookEventTypeResponse]]:
        """List the catalog of subscribable webhook event types.

        ``GET /v1/webhooks/event-types``

        Each call consumes the API key's burst + daily rate-limit budget
        like any other GET. The catalog changes very rarely; treat this as
        a once-per-deploy or once-per-onboarding lookup, not a tight poll.
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/webhooks/event-types",
            response_model=DataResponse[builtins.list[WebhookEventTypeResponse]],
        )

    def list(self) -> DataResponse[builtins.list[WebhookResponse]]:
        """List webhook subscriptions for the authenticated user.

        ``GET /v1/webhooks``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/webhooks",
            response_model=DataResponse[builtins.list[WebhookResponse]],
        )

    def create(
        self,
        *,
        url: str,
        events: builtins.list[str],
        filing_types: builtins.list[str] | None = None,
        description: str | None = None,
    ) -> DataResponse[WebhookCreateResponse]:
        """Create a new webhook subscription.

        ``POST /v1/webhooks``

        The returned ``secret`` field is the only time the api surfaces the
        HMAC signing key — store it immediately. The api does not return it
        on subsequent ``get()`` / ``list()`` calls; if lost, call
        :meth:`rotate_secret` (which invalidates the old value) or delete
        and recreate the subscription.

        Free-tier callers receive a 402 ``ThesmaError``; webhooks require
        Starter+.
        """
        body: dict[str, Any] = {
            "url": url,
            "events": events,
        }
        if filing_types is not None:
            body["filing_types"] = filing_types
        if description is not None:
            body["description"] = description
        return self._client.request(  # type: ignore[no-any-return]
            "POST",
            "/v1/webhooks",
            json=body,
            response_model=DataResponse[WebhookCreateResponse],
        )

    def get(self, subscription_id: str) -> DataResponse[WebhookResponse]:
        """Get a webhook subscription by ID.

        ``GET /v1/webhooks/{subscription_id}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/webhooks/{subscription_id}",
            response_model=DataResponse[WebhookResponse],
        )

    def update(
        self,
        subscription_id: str,
        *,
        url: str | None = None,
        events: builtins.list[str] | None = None,
        filing_types: builtins.list[str] | None = None,
        is_active: bool | None = None,
        description: str | None = None,
    ) -> DataResponse[WebhookResponse]:
        """Update a webhook subscription.

        ``PATCH /v1/webhooks/{subscription_id}``

        Only fields that are explicitly set (not ``None``) are sent. If no
        kwargs are provided, an empty JSON body ``{}`` is forwarded and the
        api returns ``400 Bad Request``.
        """
        body: dict[str, Any] = {
            k: v
            for k, v in (
                ("url", url),
                ("events", events),
                ("filing_types", filing_types),
                ("is_active", is_active),
                ("description", description),
            )
            if v is not None
        }
        return self._client.request(  # type: ignore[no-any-return]
            "PATCH",
            f"/v1/webhooks/{subscription_id}",
            json=body,
            response_model=DataResponse[WebhookResponse],
        )

    def delete(self, subscription_id: str) -> Any:
        """Delete a webhook subscription.

        ``DELETE /v1/webhooks/{subscription_id}``

        Returns ``None`` on success (204 No Content). Delivery history is
        retained server-side after deletion.

        For async clients this returns a coroutine that resolves to ``None``;
        the resource forwards the underlying ``request()`` return value so
        ``await client.webhooks.delete(...)`` works on the async client.
        """
        return self._client.request(
            "DELETE",
            f"/v1/webhooks/{subscription_id}",
        )

    def list_deliveries(
        self,
        subscription_id: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[WebhookDeliveryResponse]:
        """List delivery attempts for a webhook subscription.

        ``GET /v1/webhooks/{subscription_id}/deliveries``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/webhooks/{subscription_id}/deliveries",
            params=params,
            response_model=PaginatedResponse[WebhookDeliveryResponse],
        )

    def rotate_secret(self, subscription_id: str) -> DataResponse[WebhookSecretRotateResponse]:
        """Rotate the HMAC signing secret for a webhook subscription.

        ``POST /v1/webhooks/{subscription_id}/rotate-secret``

        The previous secret is invalidated immediately on a successful
        response — there is no grace period. Update your HMAC verification
        before, or atomically with, calling this method, or your verifier
        will reject incoming deliveries. The new secret is returned only by
        this call and is unrecoverable thereafter.
        """
        return self._client.request(  # type: ignore[no-any-return]
            "POST",
            f"/v1/webhooks/{subscription_id}/rotate-secret",
            response_model=DataResponse[WebhookSecretRotateResponse],
        )

    def send_test(self, subscription_id: str) -> DataResponse[WebhookTestResponse]:
        """Enqueue a synthetic ``webhook.test`` event for a subscription.

        ``POST /v1/webhooks/{subscription_id}/test``

        Rate-limited server-side to 5 calls per 60 seconds per subscription.
        """
        return self._client.request(  # type: ignore[no-any-return]
            "POST",
            f"/v1/webhooks/{subscription_id}/test",
            response_model=DataResponse[WebhookTestResponse],
        )

    def replay_delivery(
        self,
        subscription_id: str,
        delivery_id: str,
    ) -> DataResponse[WebhookReplayResponse]:
        """Re-queue a past delivery.

        ``POST /v1/webhooks/{subscription_id}/deliveries/{delivery_id}/replay``

        Deliveries older than the 7-day retention window return ``410
        Gone`` as a generic ``ThesmaError``.
        """
        return self._client.request(  # type: ignore[no-any-return]
            "POST",
            f"/v1/webhooks/{subscription_id}/deliveries/{delivery_id}/replay",
            response_model=DataResponse[WebhookReplayResponse],
        )
