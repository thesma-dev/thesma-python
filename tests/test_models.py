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
        """SBA period tracking lives on ``LendingDataFreshness`` post-S3.

        Prior SDK-22 shape put ``sba_period`` on the unified ``DataFreshness``;
        S3 split labor-BLS freshness (``DataFreshness``) from lending-SBA
        freshness (``LendingDataFreshness``) so the SBA surface is owned by
        ``LendingContext`` and labor-cluster freshness isn't polluted by SBA
        cadence.
        """
        from thesma._generated.models import LendingDataFreshness

        df = LendingDataFreshness(sba_period="2025-Q4")
        dumped = df.model_dump()
        assert dumped["sba_period"] == "2025-Q4"

    def test_data_freshness_with_all_five_periods(self) -> None:
        """Labor-side ``DataFreshness`` carries the 6 BLS/SEC period anchors post-S3.

        ``oews_period`` and ``sec_exec_comp_snapshot_date`` are new per S3;
        ``sba_period`` moved to ``LendingDataFreshness``.
        """
        from thesma._generated.models import DataFreshness

        df = DataFreshness(
            ces_period="2025-11",
            qcew_period="2025-Q2",
            jolts_period="2025-10",
            laus_period="2025-11",
            oews_period="2024",
            sec_exec_comp_snapshot_date="2025-03-15",
        )
        dumped = df.model_dump()
        assert dumped["ces_period"] == "2025-11"
        assert dumped["qcew_period"] == "2025-Q2"
        assert dumped["jolts_period"] == "2025-10"
        assert dumped["laus_period"] == "2025-11"
        assert dumped["oews_period"] == "2024"
        assert dumped["sec_exec_comp_snapshot_date"] == "2025-03-15"
        # sba_period is NOT on DataFreshness post-S3 — it lives on LendingDataFreshness.
        assert "sba_period" not in dumped


# --- SDK-28: unified LaborContext + SDK-29: holders temporal + SDK-31: enrichment warnings ---


class TestUnifiedLaborContext:
    """S3 reshaped ``LaborContextSummary`` to 4 derived fields and added ``summary`` +
    ``data_freshness`` on ``LaborContext``. Regression tests lock the post-S3 shape.
    """

    def test_labor_context_summary_exactly_four_fields(self) -> None:
        from thesma._generated.models import LaborContextSummary

        summary_fields = set(LaborContextSummary.model_fields.keys())
        assert summary_fields == {
            "industry_hiring_trend",
            "local_unemployment_trend",
            "comp_to_market_ratio",
            "labour_market_tightness",
        }

    def test_labor_context_gained_summary_and_data_freshness(self) -> None:
        from thesma._generated.models import LaborContext

        ctx_fields = set(LaborContext.model_fields.keys())
        assert "summary" in ctx_fields
        assert "data_freshness" in ctx_fields
        # Existing sub-objects preserved:
        for name in ("industry", "local_market", "turnover", "compensation_benchmark"):
            assert name in ctx_fields

    def test_data_freshness_has_new_post_s3_fields(self) -> None:
        from thesma._generated.models import DataFreshness

        df_fields = set(DataFreshness.model_fields.keys())
        assert "oews_period" in df_fields
        assert "sec_exec_comp_snapshot_date" in df_fields
        # Existing fields preserved:
        for name in ("ces_period", "qcew_period", "jolts_period", "laus_period"):
            assert name in df_fields

    def test_labor_context_summary_roundtrip(self) -> None:
        from thesma._generated.models import LaborContextSummary

        summary = LaborContextSummary(
            industry_hiring_trend="accelerating",
            local_unemployment_trend="improving",
            comp_to_market_ratio=1.12,
            labour_market_tightness=1.3,
        )
        dumped = summary.model_dump()
        assert dumped == {
            "industry_hiring_trend": "accelerating",
            "local_unemployment_trend": "improving",
            "comp_to_market_ratio": 1.12,
            "labour_market_tightness": 1.3,
        }

    def test_labor_context_summary_drops_pre_s3_extra_fields(self) -> None:
        """Pre-S3 flat shape fields (e.g. ``industry_employment_growth_yoy``) are silently
        dropped by the regenerated class (codegen default ``extra="ignore"``).
        """
        from thesma._generated.models import LaborContextSummary

        summary = LaborContextSummary.model_validate(
            {
                "industry_hiring_trend": "stable",
                "industry_employment_growth_yoy": 2.4,  # pre-S3 flat field
                "industry_wage_growth_yoy": 3.1,  # pre-S3 flat field
            }
        )
        assert summary.industry_hiring_trend == "stable"
        assert not hasattr(summary, "industry_employment_growth_yoy")


class TestHoldersTemporalContext:
    """S7 added ``report_quarter`` and ``filed_at`` to HolderListItem + FundHoldingListItem."""

    def test_holder_list_item_has_temporal_fields(self) -> None:
        from thesma._generated.models import HolderListItem

        fields = HolderListItem.model_fields
        assert "report_quarter" in fields
        assert "filed_at" in fields
        assert fields["report_quarter"].is_required()
        assert fields["filed_at"].is_required()

    def test_fund_holding_list_item_has_temporal_fields(self) -> None:
        from thesma._generated.models import FundHoldingListItem

        fields = FundHoldingListItem.model_fields
        assert "report_quarter" in fields
        assert "filed_at" in fields
        # Distinct required fields on this class:
        for name in ("held_company_name", "cusip", "shares", "market_value", "position_type", "filing_accession"):
            assert name in fields

    def test_holder_filed_at_is_datetime(self) -> None:
        from datetime import datetime

        from thesma._generated.models import HolderListItem

        holder = HolderListItem.model_validate(
            {
                "fund_cik": "0001632972",
                "shares": 5621716.0,
                "market_value": 1320613436.0,
                "filing_accession": "0001214659-26-004436",
                "report_quarter": "2026-Q1",
                "filed_at": "2026-04-15T14:22:00Z",
            }
        )
        assert isinstance(holder.filed_at, datetime)
        assert holder.report_quarter == "2026-Q1"

    def test_holder_roundtrip_preserves_temporal_fields(self) -> None:
        from thesma._generated.models import HolderListItem

        original = {
            "fund_cik": "0001632972",
            "fund_name": "WEALTH ENHANCEMENT",
            "shares": 5621716.0,
            "market_value": 1320613436.0,
            "filing_accession": "0001214659-26-004436",
            "report_quarter": "2025-Q3",
            "filed_at": "2025-11-14T16:30:00Z",
        }
        holder = HolderListItem.model_validate(original)
        dumped = holder.model_dump(mode="json")
        assert dumped["report_quarter"] == "2025-Q3"
        assert dumped["filed_at"].startswith("2025-11-14T")

    def test_fund_holding_roundtrip_preserves_temporal_fields(self) -> None:
        from thesma._generated.models import FundHoldingListItem

        original = {
            "held_company_name": "Apple Inc.",
            "cusip": "037833100",
            "held_company_cik": "0000320193",
            "shares": 1000000.0,
            "market_value": 200000000.0,
            "position_type": "equity",
            "filing_accession": "0001632972-25-000042",
            "report_quarter": "2025-Q3",
            "filed_at": "2025-11-14T16:30:00Z",
        }
        fh = FundHoldingListItem.model_validate(original)
        dumped = fh.model_dump(mode="json")
        assert dumped["report_quarter"] == "2025-Q3"
        assert dumped["filed_at"].startswith("2025-11-14T")
        assert dumped["position_type"] == "equity"


class TestInsiderTradesAggregation:
    """T5 introduced ``InsiderTradeAggregateListItem`` + ``PriceRange`` for the aggregated shape."""

    def test_insider_trade_aggregate_list_item_carries_aggregate_fields(self) -> None:
        from thesma._generated.models import InsiderTradeAggregateListItem

        fields = set(InsiderTradeAggregateListItem.model_fields.keys())
        assert "price_range" in fields
        assert "slice_count" in fields
        assert "shares" in fields
        assert "total_value" in fields

    def test_price_range_has_low_and_high(self) -> None:
        from thesma._generated.models import PriceRange

        pr = PriceRange(low=171.97, high=177.51)
        assert pr.low == 171.97
        assert pr.high == 177.51

    def test_insider_trade_list_item_flat_shape_preserved(self) -> None:
        """Pre-T5 per-slice shape is still a distinct class used on ``flat=True`` responses."""
        from thesma._generated.models import InsiderTradeListItem

        flat_fields = set(InsiderTradeListItem.model_fields.keys())
        for field in (
            "person",
            "cik",
            "transaction_date",
            "shares",
            "price_per_share",
            "ownership",
            "filing_accession",
        ):
            assert field in flat_fields
        # New aggregate-only fields must NOT be on the flat class:
        assert "price_range" not in flat_fields
        assert "slice_count" not in flat_fields


class TestEnrichmentWarning:
    """T4 added a typed ``EnrichmentWarning`` class plus envelope ``_enrichment_warnings`` field."""

    def test_enrichment_warning_class_shape(self) -> None:
        from thesma._generated.models import EnrichmentWarning

        w = EnrichmentWarning.model_validate(
            {
                "field": "labor_context",
                "reason": "timeout",
                "message": "labor_context did not complete within 2.0s",
            }
        )
        assert w.field == "labor_context"
        assert w.reason == "timeout"
        assert w.message is not None and w.message.startswith("labor_context did not complete")

    def test_enrichment_warning_message_optional(self) -> None:
        from thesma._generated.models import EnrichmentWarning

        w = EnrichmentWarning.model_validate({"field": "lending_context", "reason": "build_failed"})
        assert w.message is None

    def test_enrichment_warning_reason_is_permissive_str(self) -> None:
        """Per SDK-24 precedent: ``reason`` is ``str``, not ``Literal``. Unknown values must parse
        so older SDKs don't ``ValidationError`` when the API adds new reason codes.
        """
        from thesma._generated.models import EnrichmentWarning

        w = EnrichmentWarning.model_validate({"field": "labor_context", "reason": "future_unknown_reason"})
        assert w.reason == "future_unknown_reason"

    def test_enrichment_warnings_typed_round_trip(self) -> None:
        """SDK-33: ``_enrichment_warnings`` parses into a typed
        ``list[EnrichmentWarning]`` on the declared ``enrichment_warnings``
        attribute. Round-tripping via ``model_dump(by_alias=True)``
        emits the underscore-prefixed wire key back out.
        """
        from thesma._generated.models import EnrichedCompanyDataResponse

        resp = EnrichedCompanyDataResponse.model_validate(
            {
                "data": {"cik": "0000320193", "name": "Apple Inc."},
                "_enrichment_warnings": [
                    {"field": "labor_context", "reason": "timeout", "message": "..."},
                ],
            }
        )
        # Typed access — the key SDK-33 contract.
        assert resp.enrichment_warnings is not None
        assert len(resp.enrichment_warnings) == 1
        assert resp.enrichment_warnings[0].field == "labor_context"
        assert resp.enrichment_warnings[0].reason == "timeout"

        # Wire-alias emission on serialize.
        dumped = resp.model_dump(by_alias=True, exclude_none=True)
        assert "_enrichment_warnings" in dumped
        assert "enrichment_warnings" not in dumped
        assert dumped["_enrichment_warnings"][0]["field"] == "labor_context"

    def test_enrichment_warnings_parses_from_raw_wire_keys(self) -> None:
        """Regression: envelope siblings must parse from the underscore-
        prefixed wire keys, not from the Python attribute names. Using
        ``Field(serialization_alias=...)`` instead of ``Field(alias=...)``
        on the hand-correction patches would silently re-introduce the
        SDK-33 drop bug — this test catches that regression.
        """
        from thesma._generated.models import EnrichedCompanyDataResponse

        resp = EnrichedCompanyDataResponse.model_validate(
            {
                "data": {"cik": "0000320193"},
                "_warnings": ["deprecated path"],
                "_enrichment_warnings": [
                    {"field": "labor_context", "reason": "build_failed"},
                ],
            }
        )
        assert resp.warnings_ == ["deprecated path"]
        assert resp.enrichment_warnings is not None
        assert resp.enrichment_warnings[0].field == "labor_context"

    def test_enrichment_warnings_absent_when_not_emitted(self) -> None:
        """Server's ``@model_serializer`` omits ``_enrichment_warnings``
        entirely when no warnings fire — the declared field must default
        to ``None`` (not ``[]``) so consumers can distinguish "no warnings
        emitted" from "endpoint pre-dates T4".
        """
        from thesma._generated.models import EnrichedCompanyDataResponse

        resp = EnrichedCompanyDataResponse.model_validate({"data": {"cik": "0000320193"}})
        assert resp.enrichment_warnings is None
        assert resp.warnings_ is None


class TestEnrichedEnvelopeShapes:
    """SDK-33: verify the 8 hand-corrected classes carry declared fields + aliases.

    Every test calls ``cls.model_rebuild()`` first because codegen's
    ``from __future__ import annotations`` leaves the full ``Annotated[...]``
    stored as a forward-ref string; Pydantic only extracts the ``Field(alias=...)``
    metadata once the annotation is resolved (via rebuild or first instantiation).
    """

    def test_enriched_company_data_has_typed_enrichment_and_slots(self) -> None:
        from thesma._generated.models import EnrichedCompanyData

        EnrichedCompanyData.model_rebuild()
        fields = EnrichedCompanyData.model_fields
        for name in (
            "labor_context",
            "lending_context",
            "financials",
            "ratios",
            "events",
            "insider_trades",
            "holders",
            "compensation",
            "board",
        ):
            assert name in fields, f"EnrichedCompanyData: missing {name}"
        # S1 expander slots are Any | None so payload/error/URL/None all parse.
        # ``extra="allow"`` kept so cik/name/ticker/etc. still pass through.
        assert EnrichedCompanyData.model_config.get("extra") == "allow"

    def test_enriched_company_data_response_fields(self) -> None:
        from thesma._generated.models import EnrichedCompanyDataResponse

        EnrichedCompanyDataResponse.model_rebuild()
        fields = EnrichedCompanyDataResponse.model_fields
        for name in ("data", "warnings_", "enrichment_warnings"):
            assert name in fields
        assert fields["warnings_"].alias == "_warnings"
        assert fields["enrichment_warnings"].alias == "_enrichment_warnings"

    def test_enriched_compensation_data_response_fields(self) -> None:
        from thesma._generated.models import EnrichedCompensationDataResponse

        EnrichedCompensationDataResponse.model_rebuild()
        fields = EnrichedCompensationDataResponse.model_fields
        for name in ("data", "labor_context", "warnings_", "enrichment_warnings"):
            assert name in fields
        # Deliberately NO lending_context — compensation is labor-only.
        assert "lending_context" not in fields
        assert fields["enrichment_warnings"].alias == "_enrichment_warnings"

    def test_enriched_financial_data_response_fields(self) -> None:
        from thesma._generated.models import EnrichedFinancialDataResponse

        EnrichedFinancialDataResponse.model_rebuild()
        fields = EnrichedFinancialDataResponse.model_fields
        for name in ("data", "labor_context", "lending_context", "warnings_", "enrichment_warnings"):
            assert name in fields
        assert fields["enrichment_warnings"].alias == "_enrichment_warnings"

    def test_enriched_multi_statement_response_fields(self) -> None:
        from thesma._generated.models import EnrichedMultiStatementResponse

        EnrichedMultiStatementResponse.model_rebuild()
        fields = EnrichedMultiStatementResponse.model_fields
        for name in ("data", "labor_context", "lending_context", "warnings_", "enrichment_warnings"):
            assert name in fields
        assert fields["enrichment_warnings"].alias == "_enrichment_warnings"

    def test_enriched_multi_statement_paginated_response_fields(self) -> None:
        from thesma._generated.models import EnrichedMultiStatementPaginatedResponse

        EnrichedMultiStatementPaginatedResponse.model_rebuild()
        fields = EnrichedMultiStatementPaginatedResponse.model_fields
        for name in (
            "data",
            "pagination",
            "labor_context",
            "lending_context",
            "warnings_",
            "enrichment_warnings",
        ):
            assert name in fields

    def test_financial_statement_list_item_has_enrichment_fields(self) -> None:
        from thesma._generated.models import FinancialStatementListItem

        FinancialStatementListItem.model_rebuild()
        fields = FinancialStatementListItem.model_fields
        assert "labor_context" in fields
        assert "lending_context" in fields
        # Cannot subclass FinancialStatementResponse (codegen-order constraint);
        # statement fields pass through via extra="allow".
        assert FinancialStatementListItem.model_config.get("extra") == "allow"

    def test_multi_statement_list_item_has_enrichment_fields(self) -> None:
        from thesma._generated.models import MultiStatementListItem

        MultiStatementListItem.model_rebuild()
        fields = MultiStatementListItem.model_fields
        assert "labor_context" in fields
        assert "lending_context" in fields
        assert MultiStatementListItem.model_config.get("extra") == "allow"


class TestDataResponseSiblings:
    """SDK-33: verify DataResponse[T] exposes envelope-metadata siblings."""

    def test_data_response_parses_raw_wire_keys(self) -> None:
        """Regression test for the alias-vs-serialization_alias trap —
        the declared fields must parse from the underscore-prefixed wire
        keys, not from the Python attribute names.
        """
        from thesma._generated.models import EnrichmentWarning
        from thesma._types import DataResponse

        resp = DataResponse[int].model_validate(
            {
                "data": 42,
                "_warnings": ["deprecated endpoint"],
                "_enrichment_warnings": [
                    {"field": "labor_context", "reason": "timeout"},
                ],
            }
        )
        assert resp.data == 42
        assert resp.warnings_ == ["deprecated endpoint"]
        assert resp.enrichment_warnings is not None
        assert len(resp.enrichment_warnings) == 1
        assert isinstance(resp.enrichment_warnings[0], EnrichmentWarning)
        assert resp.enrichment_warnings[0].field == "labor_context"

    def test_data_response_no_siblings_defaults_to_none(self) -> None:
        from thesma._types import DataResponse

        resp = DataResponse[int].model_validate({"data": 42})
        assert resp.warnings_ is None
        assert resp.enrichment_warnings is None

    def test_data_response_model_dump_emits_wire_aliases(self) -> None:
        """Round-trip: ``model_dump(by_alias=True, exclude_none=True)``
        emits the underscore-prefixed wire keys, not the Python attribute
        names. ``exclude_none=True`` required because Pydantic v2's default
        ``model_dump`` includes ``None``-valued fields verbatim.
        """
        from thesma._generated.models import EnrichmentWarning
        from thesma._types import DataResponse

        resp = DataResponse[int](
            data=42,
            enrichment_warnings=[EnrichmentWarning(field="labor_context", reason="timeout")],
        )
        dumped = resp.model_dump(by_alias=True, exclude_none=True)
        assert dumped["data"] == 42
        assert "_enrichment_warnings" in dumped
        assert "enrichment_warnings" not in dumped  # Python name never on wire
        assert "_warnings" not in dumped  # None-valued, excluded
        assert dumped["_enrichment_warnings"][0]["field"] == "labor_context"


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
