"""Shared test fixtures for the Thesma SDK test suite."""

from __future__ import annotations

import pytest


@pytest.fixture()
def api_key() -> str:
    """Provide a test API key."""
    return "th_test_000000000000000000000000"
