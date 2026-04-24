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


# --- SDK hand-corrections --------------------------------------------------
#
# Codegen emits several shapes that don't match the hand-tuned public API.
# These post-process replacements re-apply the hand-corrections so they
# survive every regen. Two generations of patches currently live here:
#
# - SDK-24 (patches #1-#4): IFRS typed-field hoist on the financial
#   statement response.
# - SDK-33 (patches #5-#12): enrichment-envelope typed hoist — the 6
#   ``Enriched*`` stubs plus the 2 list-element stubs that codegen leaves
#   opaque because the server uses ``@model_serializer`` and the schema
#   renders as ``additionalProperties: true``.
#
# Adding a new hand-correction? Append a (old, new) pair to ``_SDK_PATCHES``
# below and a matching regression test in ``tests/test_contract.py`` or
# ``tests/test_models.py``.

_SDK_PATCHES: list[tuple[str, str]] = [
    # 1) ReportingNotes.presentation_format: Literal, not PresentationFormat enum.
    #    Codegen (datamodel-code-generator 0.55) emits single quotes and wraps
    #    the Annotated across three lines; ruff format (run AFTER patches apply)
    #    is what normalises quotes to double in the rest of the file.
    (
        "class ReportingNotes(BaseModel):\n"
        "    presentation_format: Annotated[\n"
        "        PresentationFormat, Field(title='Presentation Format')\n"
        "    ]",
        "class ReportingNotes(BaseModel):\n"
        "    # SDK-24 hand-correction: typed as Literal (not PresentationFormat enum)\n"
        "    # per SDK-24 Section 1. Consumer code reads `.presentation_format` as a\n"
        "    # string; enum access would require `.value` and change call sites.\n"
        "    presentation_format: Annotated[\n"
        '        Literal["by_function", "by_nature", "unknown"],\n'
        '        Field(title="Presentation Format"),\n'
        "    ]",
    ),
    # 2) FinancialStatementResponse.model_config = populate_by_name=True.
    (
        "class FinancialStatementResponse(BaseModel):\n    company: CompanySummary\n",
        "class FinancialStatementResponse(BaseModel):\n"
        "    # SDK-24 hand-correction: populate_by_name enables construction via either\n"
        "    # the Python attribute (`reporting_notes`) or the wire alias\n"
        "    # (`_reporting_notes`). Codegen does not emit this block.\n"
        "    model_config = ConfigDict(populate_by_name=True)\n"
        "\n"
        "    company: CompanySummary\n",
    ),
    # 3) FinancialStatementResponse.taxonomy: str, not Taxonomy enum (forward-compat).
    #    Single quotes in the `old` match pre-ruff codegen. The docstring
    #    content disambiguates from the same-shaped field on TimeSeriesPoint
    #    and MultiStatementResponse (which keep the Taxonomy enum).
    (
        "    taxonomy: Annotated[Taxonomy, Field(title='Taxonomy')]\n"
        '    """\n'
        "    XBRL taxonomy used for the filing. Existing US-GAAP-only data always returns 'us-gaap'."
        " IFRS-full parsing lands with IFRS-04.\n"
        '    """\n',
        "    # SDK-24 hand-correction: typed as `str` (not `Taxonomy` enum) per\n"
        "    # SDK-24 Section 1 — the 3.3% empty-taxonomy cohort and any future\n"
        "    # taxonomy-version strings must not raise ValidationError.\n"
        '    taxonomy: Annotated[str, Field(title="Taxonomy")]\n'
        '    """\n'
        '    XBRL taxonomy used for the filing. Common values are ``"us-gaap"`` and\n'
        '    ``"ifrs-full"``; consumer code should handle other strings (including\n'
        "    the empty string for the small residual cohort that could not be\n"
        "    classified, or future taxonomy-version identifiers) gracefully.\n"
        '    """\n',
    ),
    # 4) FinancialStatementResponse.reporting_notes: Python-named + Optional.
    #    Single quotes in the `old` match pre-ruff codegen. Docstring content
    #    disambiguates from MultiStatementResponse's `field_reporting_notes`
    #    (which keeps the codegen-mangled name and remains required).
    (
        "    field_reporting_notes: Annotated[ReportingNotes, Field(alias='_reporting_notes')]\n"
        '    """\n'
        "    Reporting metadata: presentation format, IFRS 18 applied, and any conditional"
        " edge-case flags that fired during parse. The two primary keys (presentation_format,"
        " ifrs_18_applied) are always present; conditional keys appear only when their"
        ' condition fires.\n    """\n',
        "    # SDK-24 hand-correction: Python attribute is `reporting_notes` (codegen\n"
        "    # mangles the leading-underscore wire key to `field_reporting_notes`);\n"
        "    # field is Optional with default None so pre-IFRS-07 payloads still parse.\n"
        "    reporting_notes: Annotated[\n"
        "        ReportingNotes | None,\n"
        '        Field(alias="_reporting_notes", title="Reporting Notes"),\n'
        "    ] = None\n"
        '    """\n'
        "    Reporting metadata: presentation format, IFRS 18 applied, and any\n"
        "    conditional edge-case flags that fired during parse. The two primary\n"
        "    keys (presentation_format, ifrs_18_applied) are always present;\n"
        "    conditional keys appear only when their condition fires.\n"
        '    """\n',
    ),
    # ------------------------------------------------------------------
    # SDK-33 patches (#5-#12): enrichment-envelope typed hoist
    # ------------------------------------------------------------------
    # Server-side @model_serializer in govdata-api/src/api/schemas/
    # bls_enrichment_envelope.py hides field schemas from OpenAPI, so
    # codegen emits every Enriched* class and the two list-element
    # classes as opaque ``extra='allow'`` stubs. These patches replace
    # those stubs with declared typed fields. All ``old`` strings are
    # the uniform codegen shape captured 2026-04-24 from codegen 0.55
    # (datamodel-code-generator). Note: forward-ref to classes defined
    # later in the file works because ``from __future__ import
    # annotations`` (line 9 of models.py) defers annotation evaluation
    # — but subclass headers (``class X(Parent)``) ARE eagerly resolved,
    # so patches #11/#12 keep ``(BaseModel)`` rather than subclassing
    # their respective parents (which codegen emits later in the file).
    # 5) EnrichedCompanyData: typed enrichment + 7 S1 expander slots.
    #    Keeps ``extra='allow'`` so base company fields (cik, name,
    #    ticker, sic_code, exchange, domicile, …) continue to pass
    #    through; the SDK has no ``CompanyResponse`` codegen class to
    #    subclass, and existing consumers rely on the passthrough.
    (
        "class EnrichedCompanyData(BaseModel):\n    model_config = ConfigDict(\n        extra='allow',\n    )",
        "class EnrichedCompanyData(BaseModel):\n"
        "    # SDK-33 hand-correction: declare typed enrichment siblings\n"
        "    # (labor_context, lending_context) plus the 7 S1 expander\n"
        "    # slots. Base identification fields (cik, name, ticker, …)\n"
        "    # pass through via ``extra='allow'`` because the SDK has no\n"
        "    # CompanyResponse codegen class — preserves pre-SDK-33\n"
        "    # passthrough behaviour for those fields.\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None\n"
        "    financials: Any | None = None\n"
        "    ratios: Any | None = None\n"
        "    events: Any | None = None\n"
        "    insider_trades: Any | None = None\n"
        "    holders: Any | None = None\n"
        "    compensation: Any | None = None\n"
        "    board: Any | None = None",
    ),
    # 6) EnrichedCompanyDataResponse: data + envelope-metadata siblings.
    (
        "class EnrichedCompanyDataResponse(BaseModel):\n    model_config = ConfigDict(\n        extra='allow',\n    )",
        "class EnrichedCompanyDataResponse(BaseModel):\n"
        "    # SDK-33 hand-correction: declare data + envelope-metadata\n"
        "    # siblings (_warnings / _enrichment_warnings) the server\n"
        "    # emits via @model_serializer. Inbound ``alias`` (not\n"
        "    # ``serialization_alias``) so raw wire keys parse correctly.\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    data: EnrichedCompanyData\n"
        "    warnings_: Annotated[\n"
        '        list[str] | None, Field(alias="_warnings")\n'
        "    ] = None\n"
        "    enrichment_warnings: Annotated[\n"
        '        list[EnrichmentWarning] | None, Field(alias="_enrichment_warnings")\n'
        "    ] = None",
    ),
    # 7) EnrichedCompensationDataResponse: data + labor_context only.
    #    Compensation is labor-only; no lending_context per
    #    bls_enrichment_envelope.py:158-179.
    (
        "class EnrichedCompensationDataResponse(BaseModel):\n"
        "    model_config = ConfigDict(\n"
        "        extra='allow',\n"
        "    )",
        "class EnrichedCompensationDataResponse(BaseModel):\n"
        "    # SDK-33 hand-correction: data + labor_context + envelope\n"
        "    # metadata siblings. NO lending_context — compensation is\n"
        "    # labor-only per the server envelope definition.\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    data: CompensationResponse\n"
        "    labor_context: LaborContext | None = None\n"
        "    warnings_: Annotated[\n"
        '        list[str] | None, Field(alias="_warnings")\n'
        "    ] = None\n"
        "    enrichment_warnings: Annotated[\n"
        '        list[EnrichmentWarning] | None, Field(alias="_enrichment_warnings")\n'
        "    ] = None",
    ),
    # 8) EnrichedFinancialDataResponse: data + labor + lending + siblings.
    (
        "class EnrichedFinancialDataResponse(BaseModel):\n"
        "    model_config = ConfigDict(\n"
        "        extra='allow',\n"
        "    )",
        "class EnrichedFinancialDataResponse(BaseModel):\n"
        "    # SDK-33 hand-correction: data + labor_context +\n"
        "    # lending_context + envelope-metadata siblings.\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    data: FinancialStatementResponse\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None\n"
        "    warnings_: Annotated[\n"
        '        list[str] | None, Field(alias="_warnings")\n'
        "    ] = None\n"
        "    enrichment_warnings: Annotated[\n"
        '        list[EnrichmentWarning] | None, Field(alias="_enrichment_warnings")\n'
        "    ] = None",
    ),
    # 9) EnrichedMultiStatementPaginatedResponse: list + pagination +
    #    envelope-root labor/lending + siblings. Uses the codegen
    #    PaginationMeta (same file, later in module); the hand-written
    #    thesma._types.PaginationMeta adds a validator but would
    #    introduce a circular import here — do NOT import it.
    (
        "class EnrichedMultiStatementPaginatedResponse(BaseModel):\n"
        "    model_config = ConfigDict(\n"
        "        extra='allow',\n"
        "    )",
        "class EnrichedMultiStatementPaginatedResponse(BaseModel):\n"
        "    # SDK-33 hand-correction: list of MultiStatementListItem +\n"
        "    # codegen PaginationMeta + envelope-root labor/lending +\n"
        "    # envelope-metadata siblings. PaginationMeta is the codegen\n"
        "    # class in THIS file (not thesma._types.PaginationMeta).\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    data: list[MultiStatementListItem]\n"
        "    pagination: PaginationMeta\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None\n"
        "    warnings_: Annotated[\n"
        '        list[str] | None, Field(alias="_warnings")\n'
        "    ] = None\n"
        "    enrichment_warnings: Annotated[\n"
        '        list[EnrichmentWarning] | None, Field(alias="_enrichment_warnings")\n'
        "    ] = None",
    ),
    # 10) EnrichedMultiStatementResponse: data + labor + lending + siblings.
    (
        "class EnrichedMultiStatementResponse(BaseModel):\n"
        "    model_config = ConfigDict(\n"
        "        extra='allow',\n"
        "    )",
        "class EnrichedMultiStatementResponse(BaseModel):\n"
        "    # SDK-33 hand-correction: data + labor_context +\n"
        "    # lending_context + envelope-metadata siblings.\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    data: MultiStatementResponse\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None\n"
        "    warnings_: Annotated[\n"
        '        list[str] | None, Field(alias="_warnings")\n'
        "    ] = None\n"
        "    enrichment_warnings: Annotated[\n"
        '        list[EnrichmentWarning] | None, Field(alias="_enrichment_warnings")\n'
        "    ] = None",
    ),
    # 11) FinancialStatementListItem: keep (BaseModel) because codegen
    #     emits this class BEFORE FinancialStatementResponse (opposite
    #     order from server); subclassing the parent would NameError at
    #     class-definition time. ``extra='allow'`` preserves passthrough
    #     for the 20+ statement fields (revenue, net_income, …).
    (
        "class FinancialStatementListItem(BaseModel):\n    model_config = ConfigDict(\n        extra='allow',\n    )",
        "class FinancialStatementListItem(BaseModel):\n"
        "    # SDK-33 hand-correction: declare typed enrichment fields;\n"
        "    # statement fields pass through via ``extra='allow'`` (cannot\n"
        "    # subclass FinancialStatementResponse because codegen emits\n"
        "    # this class earlier in the file than the parent).\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None",
    ),
    # 12) MultiStatementListItem: same rationale as #11.
    (
        "class MultiStatementListItem(BaseModel):\n    model_config = ConfigDict(\n        extra='allow',\n    )",
        "class MultiStatementListItem(BaseModel):\n"
        "    # SDK-33 hand-correction: declare typed enrichment fields;\n"
        "    # statement fields pass through via ``extra='allow'`` (cannot\n"
        "    # subclass MultiStatementResponse — codegen-order constraint).\n"
        '    model_config = ConfigDict(populate_by_name=True, extra="allow")\n'
        "\n"
        "    labor_context: LaborContext | None = None\n"
        "    lending_context: LendingContext | None = None",
    ),
]


def apply_hand_corrections() -> None:
    """Re-apply SDK hand-corrections that codegen overwrites.

    See ``_SDK_PATCHES`` for the list. Each patch is applied via exact
    string replacement; if a patch no longer matches (codegen output
    shifted), this function raises so the mismatch is visible, not silent —
    the regen then fails loudly rather than shipping a file missing the
    hand-correction.
    """
    content = OUTPUT.read_text()
    unmatched: list[int] = []
    for idx, (old, new) in enumerate(_SDK_PATCHES, start=1):
        if old not in content:
            unmatched.append(idx)
            continue
        content = content.replace(old, new, 1)
    if unmatched:
        OUTPUT.write_text(content)  # flush partially-applied corrections so the diff is useful
        raise RuntimeError(
            f"SDK hand-correction patch(es) {unmatched} did not match current codegen output. "
            "Inspect scripts/regenerate.py::_SDK_PATCHES and realign the `old` strings with the "
            "post-regen output (usually a comment/whitespace shift in codegen is the culprit). "
            "Partial corrections have been flushed to models.py — re-run after fixing the patch."
        )
    OUTPUT.write_text(content)


def main() -> None:
    """Regenerate models from OpenAPI schema."""
    spec = fetch_spec()
    version = spec["info"]["version"]  # type: ignore[index]

    save_fixture(spec)
    generate_models(spec)
    prepend_header()
    apply_hand_corrections()
    format_output()

    print(f"Models generated successfully from API v{version}")
    print(f"Output: {OUTPUT}")
    print(f"Fixture: {FIXTURE}")


if __name__ == "__main__":
    main()
