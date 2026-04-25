"""Tests for generated webhook models (SDK-36).

Verifies that the post-S-14 webhook schemas are importable from
`thesma._generated.models` and are valid Pydantic models.
These schemas are kept in the generated models for consumers building
their own JWT-based webhook integrations against the api directly.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from thesma._generated.models import (
    WebhookEventTypeResponse,
    WebhookReplayResponse,
    WebhookResponse,
    WebhookSecretRotateResponse,
    WebhookTestResponse,
)


class TestWebhookSchemaImports:
    """Post-S-14 webhook schemas are importable and are Pydantic models."""

    def test_webhook_event_type_response_importable(self) -> None:
        assert WebhookEventTypeResponse is not None

    def test_webhook_secret_rotate_response_importable(self) -> None:
        assert WebhookSecretRotateResponse is not None

    def test_webhook_test_response_importable(self) -> None:
        assert WebhookTestResponse is not None

    def test_webhook_replay_response_importable(self) -> None:
        assert WebhookReplayResponse is not None

    def test_webhook_response_importable(self) -> None:
        assert WebhookResponse is not None


class TestWebhookEventTypeEnum:
    """The 5 post-S-14 event types are in the enum; old types are absent."""

    def test_new_event_types_present(self) -> None:
        from thesma._generated.models import Event as WebhookEvent

        values = {m.value for m in WebhookEvent}
        assert "filing.created" in values
        assert "corporate_event.created" in values
        assert "compensation.filed" in values
        assert "board.changed" in values
        assert "amendment.filed" in values

    def test_old_event_types_absent(self) -> None:
        from thesma._generated.models import Event as WebhookEvent

        values = {m.value for m in WebhookEvent}
        assert "filing.processed" not in values
        assert "filing.corrected" not in values


class TestWebhookSchemaValidation:
    """Webhook schema model_validate behaves sensibly."""

    def test_webhook_event_type_response_requires_fields(self) -> None:
        # Must raise ValidationError (no required fields supplied)
        with pytest.raises(ValidationError):
            WebhookEventTypeResponse.model_validate({})

    def test_webhook_secret_rotate_response_requires_fields(self) -> None:
        with pytest.raises(ValidationError):
            WebhookSecretRotateResponse.model_validate({})

    def test_webhook_test_response_requires_fields(self) -> None:
        with pytest.raises(ValidationError):
            WebhookTestResponse.model_validate({})

    def test_webhook_replay_response_requires_fields(self) -> None:
        with pytest.raises(ValidationError):
            WebhookReplayResponse.model_validate({})
