"""Contract tests — verify generated models match the OpenAPI spec."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "openapi.json"
MODELS_PATH = Path(__file__).parent.parent / "src" / "thesma" / "_generated" / "models.py"
SPEC_URL = "https://api.thesma.dev/openapi.json"


def _normalize_schema_name(name: str) -> str:
    """Normalize an OpenAPI schema name to a Python class name.

    Handles patterns like ``DataResponse_CompanyResponse_`` → ``DataResponseCompanyResponse``
    and ``DataResponse_list_EventCategory__`` → ``DataResponseListEventCategory``.
    """
    # Remove underscores and title-case each segment
    parts = name.split("_")
    return "".join(p.capitalize() if p.islower() else p for p in parts if p)


@pytest.fixture(scope="session")
def openapi_spec() -> dict[str, Any]:
    """Load OpenAPI spec from fixture, with optional live refresh."""
    # Try live fetch first
    try:
        import httpx

        response = httpx.get(SPEC_URL, timeout=10)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
    except Exception:
        pass

    # Fall back to committed fixture
    assert FIXTURE_PATH.exists(), f"OpenAPI fixture not found at {FIXTURE_PATH}"
    with open(FIXTURE_PATH) as f:
        return json.load(f)  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def schema_names(openapi_spec: dict[str, Any]) -> list[str]:
    """Return all schema names from the spec."""
    return list(openapi_spec["components"]["schemas"].keys())


@pytest.fixture(scope="session")
def model_classes() -> dict[str, type]:
    """Return all classes from the generated models module."""
    import thesma._generated.models as models_module

    return {
        name: obj for name, obj in vars(models_module).items() if isinstance(obj, type) and not name.startswith("_")
    }


@pytest.mark.contract
def test_generated_models_header() -> None:
    """The generated models file has the AUTO-GENERATED header comment."""
    content = MODELS_PATH.read_text()
    assert content.startswith("# AUTO-GENERATED"), "Generated models file must start with '# AUTO-GENERATED' header"


@pytest.mark.contract
def test_all_schemas_have_models(
    openapi_spec: dict[str, Any],
    model_classes: dict[str, type],
) -> None:
    """Every schema in components/schemas has a corresponding model class."""
    schemas = openapi_spec["components"]["schemas"]
    missing: list[str] = []

    for schema_name in schemas:
        normalized = _normalize_schema_name(schema_name)
        if normalized not in model_classes:
            missing.append(f"{schema_name} (expected class: {normalized})")

    assert not missing, f"{len(missing)} schema(s) missing from generated models:\n" + "\n".join(
        f"  - {m}" for m in missing
    )


@pytest.mark.contract
def test_required_fields_exist(
    openapi_spec: dict[str, Any],
    model_classes: dict[str, type],
) -> None:
    """For each schema, required fields exist as attributes on the model class."""
    schemas = openapi_spec["components"]["schemas"]
    errors: list[str] = []

    for schema_name, schema in schemas.items():
        required = schema.get("required", [])
        if not required:
            continue

        normalized = _normalize_schema_name(schema_name)
        cls = model_classes.get(normalized)
        if cls is None:
            continue  # covered by test_all_schemas_have_models

        model_fields: dict[str, str] = {}
        if hasattr(cls, "model_fields"):
            for fname, finfo in cls.model_fields.items():
                model_fields[fname] = fname
                # Also map by alias (e.g. "from" -> "from_")
                alias = finfo.alias
                if alias:
                    model_fields[alias] = fname

        for field_name in required:
            if field_name not in model_fields:
                errors.append(f"{schema_name}.{field_name} (class: {normalized})")

    assert not errors, f"{len(errors)} required field(s) missing from models:\n" + "\n".join(f"  - {e}" for e in errors)


@pytest.mark.contract
def test_enum_values_match(
    openapi_spec: dict[str, Any],
    model_classes: dict[str, type],
) -> None:
    """Enum schemas produce Python Enum classes with matching member values."""
    from enum import Enum

    schemas = openapi_spec["components"]["schemas"]
    errors: list[str] = []

    for schema_name, schema in schemas.items():
        if "enum" not in schema:
            continue

        normalized = _normalize_schema_name(schema_name)
        cls = model_classes.get(normalized)
        if cls is None:
            continue

        if not issubclass(cls, Enum):
            errors.append(f"{schema_name}: {normalized} is not an Enum subclass")
            continue

        expected_values = set(schema["enum"])
        actual_values = {member.value for member in cls}

        if expected_values != actual_values:
            missing = expected_values - actual_values
            extra = actual_values - expected_values
            parts = [f"{schema_name}:"]
            if missing:
                parts.append(f"  missing values: {missing}")
            if extra:
                parts.append(f"  extra values: {extra}")
            errors.append("\n".join(parts))

    assert not errors, f"{len(errors)} enum(s) with mismatched values:\n" + "\n".join(f"  - {e}" for e in errors)
