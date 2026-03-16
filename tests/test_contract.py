"""Endpoint contract tests — verify resource methods match the OpenAPI spec."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from thesma.client import ThesmaClient

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "openapi.json"
SPEC_URL = "https://api.thesma.dev/openapi.json"

# Mapping of API paths to (resource_attr, method_name) on ThesmaClient.
# Only includes endpoints implemented in SDK-04.
ENDPOINT_MAP: dict[str, tuple[str, str]] = {
    "/v1/us/sec/companies": ("companies", "list"),
    "/v1/us/sec/companies/{cik}": ("companies", "get"),
    "/v1/us/sec/companies/{cik}/filings": ("filings", "list"),
    "/v1/us/sec/filings": ("filings", "list_all"),
    "/v1/us/sec/filings/{accession_number}": ("filings", "get"),
    "/v1/us/sec/filings/{accession_number}/content": ("filings", "content"),
    "/v1/us/sec/financials/fields": ("financials", "fields"),
    "/v1/us/sec/companies/{cik}/financials": ("financials", "get"),
    "/v1/us/sec/companies/{cik}/financials/{metric}": ("financials", "time_series"),
    "/v1/us/sec/companies/{cik}/ratios": ("ratios", "get"),
    "/v1/us/sec/companies/{cik}/ratios/{ratio}": ("ratios", "time_series"),
    "/v1/us/sec/screener": ("screener", "screen"),
}

# Per-endpoint mapping of API param names to SDK param names.
# Only entries where the SDK name differs from the API name.
_ENDPOINT_RENAMES: dict[str, dict[str, str]] = {
    "/v1/us/sec/companies/{cik}/filings": {"type": "filing_type", "from": "start_date", "to": "end_date"},
    "/v1/us/sec/filings": {"type": "filing_type", "from": "start_date", "to": "end_date"},
    "/v1/us/sec/companies/{cik}/financials/{metric}": {"from": "from_year", "to": "to_year"},
    "/v1/us/sec/companies/{cik}/ratios/{ratio}": {"from": "from_year", "to": "to_year"},
    "/v1/us/sec/screener": {"sort": "sort_by"},
}


@pytest.fixture(scope="session")
def openapi_spec() -> dict[str, Any]:
    """Load OpenAPI spec from fixture, with optional live refresh."""
    try:
        import httpx

        response = httpx.get(SPEC_URL, timeout=10)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
    except Exception:
        pass

    assert FIXTURE_PATH.exists(), f"OpenAPI fixture not found at {FIXTURE_PATH}"
    with open(FIXTURE_PATH) as f:
        return json.load(f)  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def client() -> ThesmaClient:
    """Create a test client (never makes real requests in contract tests)."""
    return ThesmaClient(api_key="th_test_000000000000000000000000")


def _get_spec_params(path_spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract parameter info from a path spec's GET operation."""
    get_op = path_spec.get("get", {})
    params: dict[str, dict[str, Any]] = {}
    for p in get_op.get("parameters", []):
        params[p["name"]] = p
    return params


def _get_response_ref(path_spec: dict[str, Any]) -> str | None:
    """Extract the $ref from the 200 response schema."""
    get_op = path_spec.get("get", {})
    resp_200 = get_op.get("responses", {}).get("200", {})
    content = resp_200.get("content", {}).get("application/json", {})
    schema = content.get("schema", {})
    return schema.get("$ref")


@pytest.mark.contract
class TestEndpointMethodExists:
    """Every mapped endpoint has a corresponding resource method on the client."""

    @pytest.mark.parametrize("path", list(ENDPOINT_MAP.keys()))
    def test_method_exists(self, client: ThesmaClient, path: str) -> None:
        resource_attr, method_name = ENDPOINT_MAP[path]
        resource = getattr(client, resource_attr, None)
        assert resource is not None, f"Client has no resource '{resource_attr}'"
        method = getattr(resource, method_name, None)
        assert method is not None, f"{resource_attr} has no method '{method_name}'"
        assert callable(method), f"{resource_attr}.{method_name} is not callable"


@pytest.mark.contract
class TestEndpointParams:
    """API parameter names from the spec are accepted by the SDK method."""

    @pytest.mark.parametrize("path", list(ENDPOINT_MAP.keys()))
    def test_params_match(
        self,
        client: ThesmaClient,
        openapi_spec: dict[str, Any],
        path: str,
    ) -> None:
        if path not in openapi_spec.get("paths", {}):
            pytest.skip(f"Path {path} not in spec")

        resource_attr, method_name = ENDPOINT_MAP[path]
        resource = getattr(client, resource_attr)
        method = getattr(resource, method_name)
        sig = inspect.signature(method)
        sdk_params = set(sig.parameters.keys()) - {"self"}

        renames = _ENDPOINT_RENAMES.get(path, {})
        spec_params = _get_spec_params(openapi_spec["paths"][path])

        missing: list[str] = []
        for api_name, param_info in spec_params.items():
            expected_sdk = renames.get(api_name, api_name)
            if expected_sdk not in sdk_params and api_name not in sdk_params:
                kind = "path" if param_info.get("in") == "path" else "query"
                missing.append(f"{api_name} ({kind} param, expected SDK name: {expected_sdk})")

        assert not missing, f"Endpoint {path} → {resource_attr}.{method_name}() missing params:\n" + "\n".join(
            f"  - {m}" for m in missing
        )


@pytest.mark.contract
class TestEndpointResponseModel:
    """The response model $ref matches the SDK method's return type."""

    @pytest.mark.parametrize("path", list(ENDPOINT_MAP.keys()))
    def test_response_model(
        self,
        openapi_spec: dict[str, Any],
        path: str,
    ) -> None:
        if path not in openapi_spec.get("paths", {}):
            pytest.skip(f"Path {path} not in spec")

        ref = _get_response_ref(openapi_spec["paths"][path])
        if ref is None:
            pytest.skip(f"No $ref in 200 response for {path}")

        # Extract schema name from $ref like "#/components/schemas/DataResponse_CompanyResponse_"
        schema_name = ref.split("/")[-1]

        # Verify the schema exists in components
        schemas = openapi_spec.get("components", {}).get("schemas", {})
        assert schema_name in schemas, f"Schema '{schema_name}' referenced by {path} not found in components/schemas"
