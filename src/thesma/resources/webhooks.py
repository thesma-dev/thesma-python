"""Webhooks resource — manage webhook subscriptions and deliveries."""

from __future__ import annotations

import builtins
from typing import Any

from thesma._generated.models import WebhookCreateResponse, WebhookDeliveryResponse, WebhookResponse
from thesma._types import DataResponse, PaginatedResponse


class Webhooks:
    """Resource for webhook endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(self) -> DataResponse[builtins.list[WebhookResponse]]:
        """List all webhook subscriptions.

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
        secret: str | None = None,
    ) -> DataResponse[WebhookCreateResponse]:
        """Create a new webhook subscription.

        ``POST /v1/webhooks``
        """
        body: dict[str, Any] = {
            "url": url,
            "events": events,
        }
        if secret is not None:
            body["secret"] = secret
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
        active: bool | None = None,
    ) -> DataResponse[WebhookResponse]:
        """Update a webhook subscription.

        ``PATCH /v1/webhooks/{subscription_id}``

        Only fields that are explicitly set (not ``None``) are sent.
        """
        body: dict[str, Any] = {}
        if url is not None:
            body["url"] = url
        if events is not None:
            body["events"] = events
        if active is not None:
            body["is_active"] = active
        return self._client.request(  # type: ignore[no-any-return]
            "PATCH",
            f"/v1/webhooks/{subscription_id}",
            json=body,
            response_model=DataResponse[WebhookResponse],
        )

    def delete(self, subscription_id: str) -> None:
        """Delete a webhook subscription.

        ``DELETE /v1/webhooks/{subscription_id}``
        """
        self._client.request(
            "DELETE",
            f"/v1/webhooks/{subscription_id}",
            response_model=None,
        )

    def deliveries(
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
