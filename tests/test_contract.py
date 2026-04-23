"""Endpoint contract tests — verify resource methods match the OpenAPI spec."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from thesma._generated.models import CompanyListItem
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


# --- SDK-25: URL renames + HATEOAS ------------------------------------------

_RESOURCES_DIR = Path(__file__).resolve().parent.parent / "src" / "thesma" / "resources"


def test_no_pre_s4_url_literals_in_resources() -> None:
    """Regression: no resource module contains the pre-S4 URL paths.

    S4 renamed ``/executive-compensation`` → ``/compensation`` and
    ``/institutional-holders`` → ``/holders``. Any copy-paste of an older
    snippet into a new resource method would silently 404 at runtime.
    """
    for py_file in _RESOURCES_DIR.glob("*.py"):
        text = py_file.read_text()
        assert "executive-compensation" not in text, f"{py_file.name}: pre-S4 URL"
        assert "institutional-holders" not in text, f"{py_file.name}: pre-S4 URL"


@pytest.mark.contract
def test_s4_new_paths_present_in_spec(openapi_spec: dict[str, Any]) -> None:
    """Post-S4 paths exist in the OpenAPI spec."""
    paths = openapi_spec.get("paths", {})
    assert "/v1/us/sec/companies/{cik}/compensation" in paths
    assert "/v1/us/sec/companies/{cik}/holders" in paths


@pytest.mark.contract
def test_s4_old_paths_absent_from_spec(openapi_spec: dict[str, Any]) -> None:
    """Pre-S4 paths are gone from the OpenAPI spec."""
    paths = openapi_spec.get("paths", {})
    assert "/v1/us/sec/companies/{cik}/executive-compensation" not in paths
    assert "/v1/us/sec/companies/{cik}/institutional-holders" not in paths


def test_company_list_item_has_detail_url() -> None:
    """``CompanyListItem.detail_url`` is a required ``str`` field post-S4."""
    assert "detail_url" in CompanyListItem.model_fields
    assert CompanyListItem.model_fields["detail_url"].is_required()


# Minimal realistic EnrichedCompanyData payload with 11 absolute ``*_url`` fields
# plus the base-company fields the envelope passes through via ``extra="allow"``.
_ENRICHED_COMPANY_FIXTURE: dict[str, Any] = {
    "cik": "0000320193",
    "ticker": "AAPL",
    "name": "Apple Inc.",
    "filings_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/filings",
    "financials_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/financials",
    "ratios_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/ratios",
    "events_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/events",
    "insider_trades_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/insider-trades",
    "insider_holdings_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/insider-holdings",
    "holders_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/holders",
    "compensation_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/compensation",
    "board_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/board",
    "proxy_votes_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/proxy-votes",
    "beneficial_ownership_url": "https://api.thesma.dev/v1/us/sec/companies/0000320193/beneficial-ownership",
}


def test_enriched_company_data_carries_11_hateoas_urls() -> None:
    """``EnrichedCompanyData`` surfaces all 11 ``*_url`` fields as absolute URLs.

    Generated ``EnrichedCompanyData`` uses ``extra="allow"`` passthrough rather
    than declaring the URL fields as typed attributes, so HATEOAS links are
    accessed via ``model_extra``. The test works whether codegen produces
    explicit fields OR passthrough — ``getattr`` with ``model_extra`` fallback
    covers both cases.
    """
    from thesma._generated.models import EnrichedCompanyData

    data = EnrichedCompanyData.model_validate(_ENRICHED_COMPANY_FIXTURE)
    expected_url_fields = {
        "filings_url",
        "financials_url",
        "ratios_url",
        "events_url",
        "insider_trades_url",
        "insider_holdings_url",
        "holders_url",
        "compensation_url",
        "board_url",
        "proxy_votes_url",
        "beneficial_ownership_url",
    }
    for name in expected_url_fields:
        value = getattr(data, name, None)
        if value is None and data.model_extra:
            value = data.model_extra.get(name)
        assert value is not None, f"{name} missing from EnrichedCompanyData"
        assert isinstance(value, str) and value.startswith("https://"), f"{name} not absolute: {value}"
