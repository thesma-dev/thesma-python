"""API key validation and header injection."""

from __future__ import annotations


def validate_api_key(api_key: str) -> str:
    """Validate and return the API key.

    Raises :class:`ValueError` if the key is empty or not a string.
    """
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError("api_key must be a non-empty string")
    return api_key


def auth_headers(api_key: str) -> dict[str, str]:
    """Return authentication headers for the Thesma API."""
    return {"X-API-Key": api_key}
