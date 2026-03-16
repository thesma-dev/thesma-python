"""Scaffold validation tests — verify SDK-01 project structure."""

from __future__ import annotations

import importlib.resources
import re
from pathlib import Path


def test_version_format() -> None:
    """thesma.__version__ returns a version string matching X.Y.Z.W format."""
    from thesma import __version__

    assert re.match(r"^\d+\.\d+\.\d+\.\d+$", __version__), f"Version '{__version__}' does not match X.Y.Z.W format"


def test_py_typed_marker_exists() -> None:
    """PEP 561 py.typed marker file exists in the package."""
    # Use importlib.resources for reliable package-relative path resolution
    package_dir = Path(importlib.resources.files("thesma").__fspath__())  # type: ignore[union-attr]
    py_typed = package_dir / "py.typed"
    assert py_typed.exists(), f"py.typed marker not found at {py_typed}"


def test_thesma_client_importable() -> None:
    """ThesmaClient can be imported from the thesma package."""
    from thesma import ThesmaClient

    client = ThesmaClient(api_key="th_test_key")
    assert client.api_key == "th_test_key"
    assert client.base_url == "https://api.thesma.dev"
    assert client.timeout == 30


def test_async_thesma_client_importable() -> None:
    """AsyncThesmaClient can be imported from the thesma package."""
    from thesma import AsyncThesmaClient

    client = AsyncThesmaClient(api_key="th_test_key")
    assert client.api_key == "th_test_key"
    assert client.base_url == "https://api.thesma.dev"
    assert client.timeout == 30
