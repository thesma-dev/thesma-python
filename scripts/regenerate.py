"""Regenerate Pydantic models from the Thesma API OpenAPI schema.

This script fetches the OpenAPI spec from the Thesma API, generates
Pydantic v2 models using datamodel-code-generator, and writes them
to src/thesma/_generated/models.py.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

SPEC_URL = "https://api.thesma.dev/openapi.json"
ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "src" / "thesma" / "_generated" / "models.py"
FIXTURE = ROOT / "tests" / "fixtures" / "openapi.json"

HEADER = """\
# AUTO-GENERATED from OpenAPI spec — DO NOT EDIT
# Regenerate: python scripts/regenerate.py
# Source: {url}
# Generated: {timestamp}
"""


def fetch_spec() -> dict[str, object]:
    """Fetch the OpenAPI spec from the Thesma API."""
    try:
        response = httpx.get(SPEC_URL, timeout=30)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"Error fetching OpenAPI spec: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        spec = response.json()
    except json.JSONDecodeError as exc:
        print(f"Error parsing OpenAPI spec JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    version = spec.get("info", {}).get("version")
    if not version:
        print("Error: OpenAPI spec missing info.version", file=sys.stderr)
        sys.exit(1)

    return spec  # type: ignore[return-value]


def save_fixture(spec: dict[str, object]) -> None:
    """Save a copy of the spec to tests/fixtures/ for CI fallback."""
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(spec, indent=2) + "\n")


def generate_models(spec: dict[str, object]) -> None:
    """Run datamodel-codegen to produce Pydantic v2 models."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as tmp:
        json.dump(spec, tmp)
        tmp.flush()

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "datamodel_code_generator",
                "--input",
                tmp.name,
                "--output",
                str(OUTPUT),
                "--output-model-type",
                "pydantic_v2.BaseModel",
                "--use-annotated",
                "--use-field-description",
            ],
            capture_output=True,
            text=True,
        )

    if result.returncode != 0:
        print(f"datamodel-codegen failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)


def prepend_header() -> None:
    """Add the auto-generated header to the models file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    header = HEADER.format(url=SPEC_URL, timestamp=timestamp)
    content = OUTPUT.read_text()
    OUTPUT.write_text(header + content)


def format_output() -> None:
    """Run ruff format on the generated file."""
    subprocess.run(
        [sys.executable, "-m", "ruff", "format", str(OUTPUT)],
        capture_output=True,
        text=True,
    )


def main() -> None:
    """Regenerate models from OpenAPI schema."""
    spec = fetch_spec()
    version = spec["info"]["version"]  # type: ignore[index]

    save_fixture(spec)
    generate_models(spec)
    prepend_header()
    format_output()

    print(f"Models generated successfully from API v{version}")
    print(f"Output: {OUTPUT}")
    print(f"Fixture: {FIXTURE}")


if __name__ == "__main__":
    main()
