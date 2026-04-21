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


class TestSbaEnrichmentModels:
    """Unit tests for the SBA enrichment models regenerated by SDK-22."""

    @staticmethod
    def _local_market_payload() -> dict[str, Any]:
        return {
            "county_fips": "06037",
            "county_name": "Los Angeles County, CA",
            "county_fips_confidence": "high",
            "quarterly_loan_count": 142,
            "quarterly_total_amount": 38_500_000,
            "avg_loan_size": 271_127,
            "quarterly_yoy_change_pct": 8.4,
            "charge_off_rate_trailing_4q": 2.1,
            "top_industry_naics": "722511",
            "top_industry_name": "Full-Service Restaurants",
            "data_period": "2025-Q3",
            "source": "SBA",
        }

    @staticmethod
    def _industry_lending_payload() -> dict[str, Any]:
        return {
            "naics_code": "511210",
            "naics_description": "Software Publishers",
            "naics_match_level": "6-digit",
            "national_quarterly_loan_count": 920,
            "national_quarterly_total_amount": 210_000_000,
            "national_avg_loan_size": 228_260,
            "national_yoy_change_pct": 6.1,
            "national_charge_off_rate_trailing_4q": 1.3,
            "data_period": "2025-Q3",
            "source": "SBA",
        }

    def test_lending_context_full_shape_parses(self) -> None:
        from thesma._generated.models import IndustryLending, LendingContext, LocalLendingMarket

        ctx = LendingContext(
            local_market=LocalLendingMarket(**self._local_market_payload()),
            industry_lending=IndustryLending(**self._industry_lending_payload()),
        )
        dumped = ctx.model_dump(mode="json")
        assert dumped["local_market"]["county_fips"] == "06037"
        assert dumped["industry_lending"]["naics_code"] == "511210"

    def test_lending_context_both_null_children_parses(self) -> None:
        from thesma._generated.models import LendingContext

        ctx = LendingContext(local_market=None, industry_lending=None)
        assert ctx.model_dump() == {"local_market": None, "industry_lending": None}

    def test_lending_context_partial_local_only_parses(self) -> None:
        from thesma._generated.models import LendingContext, LocalLendingMarket

        ctx = LendingContext(
            local_market=LocalLendingMarket(**self._local_market_payload()),
            industry_lending=None,
        )
        assert ctx.local_market is not None
        assert ctx.industry_lending is None

    def test_local_lending_market_county_fips_confidence_unknown(self) -> None:
        from thesma._generated.models import LocalLendingMarket

        payload = {**self._local_market_payload(), "county_fips_confidence": "unknown"}
        m = LocalLendingMarket(**payload)
        assert m.county_fips_confidence.value == "unknown"

    def test_local_lending_market_county_fips_confidence_invalid_raises(self) -> None:
        import pydantic

        from thesma._generated.models import LocalLendingMarket

        payload = {**self._local_market_payload(), "county_fips_confidence": "bogus"}
        with pytest.raises(pydantic.ValidationError):
            LocalLendingMarket(**payload)

    def test_lending_context_summary_flat_shape_parses(self) -> None:
        from thesma._generated.models import LendingContextSummary

        summary = LendingContextSummary(
            local_sba_loan_count_4q=500,
            local_sba_lending_growth_yoy=4.2,
            industry_sba_lending_growth_yoy=3.1,
            industry_sba_charge_off_rate=1.8,
        )
        dumped = summary.model_dump()
        assert set(dumped.keys()) == {
            "local_sba_loan_count_4q",
            "local_sba_lending_growth_yoy",
            "industry_sba_lending_growth_yoy",
            "industry_sba_charge_off_rate",
        }
        assert "data_freshness" not in dumped

    def test_data_freshness_with_sba_period(self) -> None:
        from thesma._generated.models import DataFreshness

        df = DataFreshness(sba_period="2025-Q4")
        dumped = df.model_dump()
        assert dumped["sba_period"] == "2025-Q4"
        assert dumped["ces_period"] is None
        assert dumped["qcew_period"] is None
        assert dumped["jolts_period"] is None
        assert dumped["laus_period"] is None

    def test_data_freshness_with_all_five_periods(self) -> None:
        from thesma._generated.models import DataFreshness

        df = DataFreshness(
            ces_period="2025-11",
            qcew_period="2025-Q2",
            jolts_period="2025-10",
            laus_period="2025-11",
            sba_period="2025-Q4",
        )
        dumped = df.model_dump()
        assert dumped["ces_period"] == "2025-11"
        assert dumped["qcew_period"] == "2025-Q2"
        assert dumped["jolts_period"] == "2025-10"
        assert dumped["laus_period"] == "2025-11"
        assert dumped["sba_period"] == "2025-Q4"


class TestIfrsReportingNotesModels:
    """Unit tests for the IFRS reporting-notes models hoisted by SDK-24."""

    @staticmethod
    def _financial_statement_payload(
        *,
        taxonomy: str = "us-gaap",
        currency: str = "USD",
        include_reporting_notes: bool = True,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
            "statement": "income",
            "period": "annual",
            "fiscal_year": 2024,
            "filing_accession": "0000320193-24-000081",
            "currency": currency,
            "taxonomy": taxonomy,
            "line_items": {"revenue": 391035000000},
            "metadata": {
                "source": "ixbrl",
                "data_completeness": 15,
                "expected_fields": 16,
                "source_tags": {"revenue": "us-gaap:Revenues"},
            },
        }
        if include_reporting_notes:
            payload["_reporting_notes"] = {
                "presentation_format": "by_function",
                "ifrs_18_applied": False,
            }
        return payload

    @staticmethod
    def _ifrs_financial_statement_payload() -> dict[str, Any]:
        return {
            "company": {"cik": "0001639920", "ticker": "SPOT", "name": "Spotify Technology S.A."},
            "statement": "income",
            "period": "annual",
            "fiscal_year": 2024,
            "filing_accession": "0001639920-25-000012",
            "currency": "EUR",
            "taxonomy": "ifrs-full",
            "_reporting_notes": {
                "presentation_format": "by_nature",
                "ifrs_18_applied": True,
                "taxonomy_changed_in_amendment": False,
                "currency_changed_in_amendment": False,
                "taxonomy_detection_ambiguous": False,
                "currency_detection_ambiguous": False,
            },
            "line_items": {"revenue": 15670000000},
            "metadata": {
                "source": "ixbrl",
                "data_completeness": 15,
                "expected_fields": 16,
                "source_tags": {"revenue": "ifrs-full:Revenue"},
            },
        }

    # --- ReportingNotes -----------------------------------------------------

    def test_reporting_notes_required_fields_only(self) -> None:
        from thesma._generated.models import ReportingNotes

        rn = ReportingNotes(presentation_format="by_function", ifrs_18_applied=False)
        dumped = rn.model_dump()
        assert set(dumped.keys()) == {
            "presentation_format",
            "ifrs_18_applied",
            "taxonomy_changed_in_amendment",
            "currency_changed_in_amendment",
            "taxonomy_detection_ambiguous",
            "currency_detection_ambiguous",
            "presentation_format_detection_note",
        }
        assert dumped["taxonomy_changed_in_amendment"] is None
        assert dumped["currency_changed_in_amendment"] is None
        assert dumped["taxonomy_detection_ambiguous"] is None
        assert dumped["currency_detection_ambiguous"] is None
        assert dumped["presentation_format_detection_note"] is None

        unset_only = rn.model_dump(exclude_unset=True)
        assert set(unset_only.keys()) == {"presentation_format", "ifrs_18_applied"}

    def test_reporting_notes_full_shape_parses(self) -> None:
        from thesma._generated.models import PresentationFormatDetectionNote, ReportingNotes

        note = PresentationFormatDetectionNote(
            scanned_by_function_tags=["ifrs-full:CostOfSales"],
            scanned_by_nature_tags=["ifrs-full:RawMaterialsAndConsumablesUsed"],
            matched=["ifrs-full:CostOfSales"],
        )
        rn = ReportingNotes(
            presentation_format="unknown",
            ifrs_18_applied=True,
            taxonomy_changed_in_amendment=True,
            currency_changed_in_amendment=False,
            taxonomy_detection_ambiguous=True,
            currency_detection_ambiguous=False,
            presentation_format_detection_note=note,
        )
        dumped = rn.model_dump()
        assert dumped["presentation_format"] == "unknown"
        assert dumped["ifrs_18_applied"] is True
        assert dumped["taxonomy_changed_in_amendment"] is True
        assert dumped["currency_changed_in_amendment"] is False
        assert dumped["taxonomy_detection_ambiguous"] is True
        assert dumped["currency_detection_ambiguous"] is False
        assert dumped["presentation_format_detection_note"] == {
            "scanned_by_function_tags": ["ifrs-full:CostOfSales"],
            "scanned_by_nature_tags": ["ifrs-full:RawMaterialsAndConsumablesUsed"],
            "matched": ["ifrs-full:CostOfSales"],
        }

    def test_reporting_notes_presentation_format_unknown_parses(self) -> None:
        from thesma._generated.models import ReportingNotes

        rn = ReportingNotes(presentation_format="unknown", ifrs_18_applied=False)
        assert rn.presentation_format == "unknown"

    def test_reporting_notes_presentation_format_invalid_raises(self) -> None:
        import pydantic

        from thesma._generated.models import ReportingNotes

        with pytest.raises(pydantic.ValidationError):
            ReportingNotes(presentation_format="sideways", ifrs_18_applied=False)  # type: ignore[arg-type]

    def test_reporting_notes_missing_required_raises(self) -> None:
        import pydantic

        from thesma._generated.models import ReportingNotes

        with pytest.raises(pydantic.ValidationError):
            ReportingNotes(presentation_format="by_function")  # type: ignore[call-arg]

    # --- PresentationFormatDetectionNote -----------------------------------

    def test_presentation_format_detection_note_empty_lists_parse(self) -> None:
        from thesma._generated.models import PresentationFormatDetectionNote

        note = PresentationFormatDetectionNote(
            scanned_by_function_tags=[],
            scanned_by_nature_tags=[],
            matched=[],
        )
        assert note.model_dump() == {
            "scanned_by_function_tags": [],
            "scanned_by_nature_tags": [],
            "matched": [],
        }

    def test_presentation_format_detection_note_missing_required_raises(self) -> None:
        import pydantic

        from thesma._generated.models import PresentationFormatDetectionNote

        with pytest.raises(pydantic.ValidationError):
            PresentationFormatDetectionNote(  # type: ignore[call-arg]
                scanned_by_function_tags=[],
                scanned_by_nature_tags=[],
            )

    # --- FinancialStatementResponse: taxonomy ------------------------------

    def test_financial_statement_response_taxonomy_us_gaap_parses(self) -> None:
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._financial_statement_payload(taxonomy="us-gaap"))
        assert resp.taxonomy == "us-gaap"

    def test_financial_statement_response_taxonomy_ifrs_full_parses(self) -> None:
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._financial_statement_payload(taxonomy="ifrs-full"))
        assert resp.taxonomy == "ifrs-full"

    def test_financial_statement_response_taxonomy_empty_string_parses(self) -> None:
        """The 3.3 % empty-taxonomy cohort (T-161) must not ValidationError."""
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._financial_statement_payload(taxonomy=""))
        assert resp.taxonomy == ""

    def test_financial_statement_response_taxonomy_unknown_string_parses(self) -> None:
        """Forward-compat: hypothetical future taxonomy-version strings must not ValidationError."""
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(
            self._financial_statement_payload(taxonomy="us-gaap-2026"),
        )
        assert resp.taxonomy == "us-gaap-2026"

    # --- FinancialStatementResponse: reporting_notes alias / construction --

    def test_financial_statement_response_reporting_notes_python_attribute_access(self) -> None:
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._ifrs_financial_statement_payload())
        assert resp.reporting_notes is not None
        assert resp.reporting_notes.presentation_format == "by_nature"

    def test_financial_statement_response_reporting_notes_construct_by_python_name(self) -> None:
        """Exercises ``populate_by_name=True`` — construct via the Python attribute name."""
        from thesma._generated.models import FinancialStatementResponse, ReportingNotes

        resp = FinancialStatementResponse(
            company={"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},  # type: ignore[arg-type]
            statement="income",  # type: ignore[arg-type]
            period="annual",  # type: ignore[arg-type]
            fiscal_year=2024,
            filing_accession="0000320193-24-000081",
            currency="USD",
            taxonomy="us-gaap",
            reporting_notes=ReportingNotes(presentation_format="by_function", ifrs_18_applied=False),
            line_items={"revenue": 391035000000},
            metadata={  # type: ignore[arg-type]
                "source": "ixbrl",
                "data_completeness": 15,
                "expected_fields": 16,
                "source_tags": {"revenue": "us-gaap:Revenues"},
            },
        )
        assert resp.reporting_notes is not None
        assert resp.reporting_notes.presentation_format == "by_function"

    def test_financial_statement_response_reporting_notes_construct_by_alias_name(self) -> None:
        """Exercises alias-based construction via ``**kwargs`` unpacking from a wire-shaped dict."""
        from thesma._generated.models import FinancialStatementResponse

        payload = self._financial_statement_payload()
        resp = FinancialStatementResponse(**payload)
        assert resp.reporting_notes is not None
        assert resp.reporting_notes.presentation_format == "by_function"

    def test_financial_statement_response_reporting_notes_model_dump_default_uses_alias(self) -> None:
        """``model_dump(by_alias=True)`` emits the wire-level ``_reporting_notes`` key.

        Pydantic v2's serialisation default is ``by_alias=False`` (field name); consumers
        round-tripping back to the API must pass ``by_alias=True`` explicitly. We exercise
        that form here since the wire-level key is the load-bearing round-trip shape.
        """
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._financial_statement_payload())
        dumped = resp.model_dump(by_alias=True)
        assert "_reporting_notes" in dumped
        assert "reporting_notes" not in dumped

    def test_financial_statement_response_reporting_notes_model_dump_by_alias_false(self) -> None:
        """``model_dump(by_alias=False)`` (Pydantic v2 default) emits the Python attribute name."""
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._financial_statement_payload())
        dumped = resp.model_dump(by_alias=False)
        assert "reporting_notes" in dumped
        assert "_reporting_notes" not in dumped
        # Pydantic v2 default for ``model_dump()`` is ``by_alias=False``, so ``resp.model_dump()``
        # yields the same Python-attribute shape — confirms the opt-out path is also the default.
        default_dumped = resp.model_dump()
        assert "reporting_notes" in default_dumped

    def test_financial_statement_response_reporting_notes_optional_absent(self) -> None:
        from thesma._generated.models import FinancialStatementResponse

        payload = self._financial_statement_payload(include_reporting_notes=False)
        resp = FinancialStatementResponse.model_validate(payload)
        assert resp.reporting_notes is None

    # --- extras-assertion tripwire -----------------------------------------

    def test_financial_statement_response_extras_empty_on_ifrs_payload(self) -> None:
        """Hazard-closure tripwire — any new API field not hoisted into the typed model lands in ``.model_extra``.

        Intentionally omits ``labor_context`` / ``lending_context`` at the envelope root — those are
        the documented-and-deferred envelope-drop limitation (see ``Financials.get`` docstring).
        """
        from thesma._generated.models import FinancialStatementResponse

        resp = FinancialStatementResponse.model_validate(self._ifrs_financial_statement_payload())
        # Pydantic-v2: ``model_extra`` is ``None`` when ``extra="ignore"`` (the default),
        # or ``{}``/``None`` with no unexpected keys — ``not resp.model_extra`` covers both.
        assert not resp.model_extra
