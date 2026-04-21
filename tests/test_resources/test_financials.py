"""Tests for the Financials resource."""

from __future__ import annotations

import httpx
import pytest
import respx

from thesma._generated.models import FieldsResponse, FinancialStatementResponse, TimeSeriesResponse
from thesma._types import DataResponse
from thesma.client import AsyncThesmaClient, ThesmaClient

BASE = "https://api.thesma.dev"

FINANCIAL_STATEMENT_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000081",
        "currency": "USD",
        "taxonomy": "us-gaap",
        "_reporting_notes": {
            "presentation_format": "by_function",
            "ifrs_18_applied": False,
        },
        "line_items": {"revenue": 391035000000},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "us-gaap:Revenues"},
        },
    },
}

TIME_SERIES_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "metric": "revenue",
        "period": "annual",
        "currency": "USD",
        "series": [
            {
                "fiscal_year": 2024,
                "value": 391035000000,
                "filing_accession": "0000320193-24-000081",
                "currency": "USD",
                "taxonomy": "us-gaap",
            },
            {
                "fiscal_year": 2023,
                "value": 383285000000,
                "filing_accession": "0000320193-23-000077",
                "currency": "USD",
                "taxonomy": "us-gaap",
            },
        ],
    },
}

FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON = {
    "data": {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": 2024,
        "filing_accession": "0000320193-24-000081",
        "currency": "USD",
        "taxonomy": "us-gaap",
        "_reporting_notes": {
            "presentation_format": "by_function",
            "ifrs_18_applied": False,
        },
        "line_items": {"revenue": 391035000000},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "us-gaap:Revenues"},
        },
        "labor_context": {
            "industry": {"naics_code": "334111"},
            "local_market": {
                "county_fips": "06085",
                "county_name": "Santa Clara County, CA",
                "county_fips_confidence": "high",
                "industry_employment": 142000,
                "industry_employment_yoy_pct": 3.8,
                "industry_avg_weekly_wage": 3200,
                "industry_wage_yoy_pct": 4.2,
                "total_employment": 1050000,
                "total_avg_weekly_wage": 2800,
                "data_period": "2025-Q2",
                "data_lag_months": 6,
                "match_precision": "6-digit",
                "unemployment_rate": 2.8,
                "unemployment_rate_yoy_change": -0.4,
                "labor_force": 1050450,
                "labor_force_yoy_change_pct": 1.2,
                "laus_data_period": "2025-11",
                "laus_data_lag_weeks": 7,
                "match_level": "county",
                "seasonal_adjustment": "not_seasonally_adjusted",
                "source": "LAUS+QCEW",
            },
            "compensation_benchmark": None,
        },
    },
}

FIELDS_JSON = {
    "data": {
        "income": {
            "fields": [
                {"name": "revenue", "description": "Total revenue", "bank_specific": False},
            ],
        },
        "balance_sheet": {
            "fields": [
                {"name": "total_assets", "description": "Total assets", "bank_specific": False},
            ],
        },
        "cash_flow": {
            "fields": [
                {"name": "operating_cash_flow", "description": "Operating cash flow", "bank_specific": False},
            ],
        },
    },
}

FINANCIAL_STATEMENT_IFRS_JSON = {
    "data": {
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
    },
}

FINANCIAL_STATEMENT_WITH_DETECTION_NOTE_JSON = {
    "data": {
        "company": {"cik": "0001639920", "ticker": "SPOT", "name": "Spotify Technology S.A."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": 2024,
        "filing_accession": "0001639920-25-000012",
        "currency": "EUR",
        "taxonomy": "ifrs-full",
        "_reporting_notes": {
            "presentation_format": "unknown",
            "ifrs_18_applied": False,
            "presentation_format_detection_note": {
                "scanned_by_function_tags": ["ifrs-full:CostOfSales"],
                "scanned_by_nature_tags": ["ifrs-full:RawMaterialsAndConsumablesUsed"],
                "matched": [],
            },
        },
        "line_items": {"revenue": 15670000000},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "ifrs-full:Revenue"},
        },
    },
}


class TestFinancialsGet:
    @respx.mock
    def test_get_with_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", period="annual")

        assert route.called
        request = route.calls.last.request
        assert "statement=income" in str(request.url)
        assert "period=annual" in str(request.url)
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FinancialStatementResponse)
        client.close()

    @respx.mock
    def test_get_with_year_and_quarter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income", period="quarterly", year=2024, quarter=3)

        request = route.calls.last.request
        assert "year=2024" in str(request.url)
        assert "quarter=3" in str(request.url)
        client.close()


class TestFinancialsTimeSeries:
    @respx.mock
    def test_time_series_url(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials/revenue").mock(
            return_value=httpx.Response(200, json=TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.time_series("0000320193", "revenue")

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, TimeSeriesResponse)
        client.close()

    @respx.mock
    def test_time_series_with_period(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials/revenue").mock(
            return_value=httpx.Response(200, json=TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.time_series("0000320193", "revenue", period="quarterly")

        request = route.calls.last.request
        assert "period=quarterly" in str(request.url)
        client.close()


class TestFinancialsLaborContextInclude:
    @respx.mock
    def test_financials_get_laus_local_market_fields_present(self, api_key: str) -> None:
        """LAUS-enriched financials responses deserialize cleanly via ?include=labor_context.

        ``FinancialStatementResponse`` uses Pydantic's default ``extra="ignore"``,
        so the top-level ``labor_context`` key is dropped when the strict model
        parses the payload. The test therefore asserts two things:

        1. The ``include=labor_context`` query parameter reaches the API.
        2. The payload (including the LAUS-enriched ``local_market`` block)
           parses without raising a validation error — confirming the new
           LAUS fields on the generated ``LocalMarketContext`` model are
           compatible with the serialized shape the financials endpoint
           returns.
        """
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", include="labor_context")

        request = route.calls.last.request
        assert "include=labor_context" in str(request.url)
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FinancialStatementResponse)
        # Confirm the well-known financials fields still round-trip cleanly.
        assert result.data.fiscal_year == 2024
        assert result.data.line_items["revenue"] == 391035000000
        client.close()


class TestFinancialsLendingContextInclude:
    @respx.mock
    def test_get_with_include_lending_context_forwards_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income", period="annual", include="lending_context")

        assert "include=lending_context" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_with_include_combined_forwards_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", include="labor_context,lending_context")

        assert "include=labor_context%2Clending_context" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_without_include_omits_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.get("0000320193", statement="income")

        assert "include=" not in str(route.calls.last.request.url)
        client.close()


class TestFinancialsIfrsTypedFields:
    """SDK-24: hoisted ``taxonomy`` and ``reporting_notes`` typed fields flow through live parsing."""

    @respx.mock
    def test_get_us_gaap_payload_exposes_taxonomy_and_reporting_notes(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", period="annual")

        assert result is not None
        assert result.data.taxonomy == "us-gaap"
        assert result.data.currency == "USD"
        assert result.data.reporting_notes is not None
        assert result.data.reporting_notes.presentation_format == "by_function"
        assert result.data.reporting_notes.ifrs_18_applied is False
        client.close()

    @respx.mock
    def test_get_ifrs_payload_exposes_eur_currency_and_ifrs_full(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0001639920/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_IFRS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0001639920", statement="income", period="annual")

        assert result is not None
        assert result.data.taxonomy == "ifrs-full"
        assert result.data.currency == "EUR"
        assert result.data.reporting_notes is not None
        assert result.data.reporting_notes.presentation_format == "by_nature"
        assert result.data.reporting_notes.ifrs_18_applied is True
        assert result.data.reporting_notes.taxonomy_changed_in_amendment is False
        assert result.data.reporting_notes.currency_changed_in_amendment is False
        assert result.data.reporting_notes.taxonomy_detection_ambiguous is False
        assert result.data.reporting_notes.currency_detection_ambiguous is False
        client.close()

    @respx.mock
    def test_get_with_detection_note_populated(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0001639920/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_WITH_DETECTION_NOTE_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0001639920", statement="income", period="annual")

        assert result is not None
        assert result.data.reporting_notes is not None
        assert result.data.reporting_notes.presentation_format == "unknown"
        assert result.data.reporting_notes.presentation_format_detection_note is not None
        note = result.data.reporting_notes.presentation_format_detection_note
        assert note.scanned_by_function_tags == ["ifrs-full:CostOfSales"]
        assert note.scanned_by_nature_tags == ["ifrs-full:RawMaterialsAndConsumablesUsed"]
        assert note.matched == []
        client.close()

    @respx.mock
    def test_get_with_include_labor_context_still_drops_envelope_key(self, api_key: str) -> None:
        """Regression guard: new typed fields ride with the envelope, ``labor_context`` still drops."""
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", include="labor_context")

        assert result is not None
        assert result.data.taxonomy == "us-gaap"
        assert result.data.reporting_notes is not None
        # Envelope-sibling enrichment still drops per ``extra="ignore"`` — SDK-24 does NOT change this.
        assert getattr(result.data, "labor_context", None) is None
        client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_get_exposes_typed_fields(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0001639920/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_IFRS_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.financials.get("0001639920", statement="income", period="annual")

        assert result is not None
        assert result.data.taxonomy == "ifrs-full"
        assert result.data.currency == "EUR"
        assert result.data.reporting_notes is not None
        assert result.data.reporting_notes.presentation_format == "by_nature"

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_get_with_detection_note_populated(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0001639920/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_WITH_DETECTION_NOTE_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            result = await client.financials.get("0001639920", statement="income", period="annual")

        assert result is not None
        assert result.data.reporting_notes is not None
        assert result.data.reporting_notes.presentation_format == "unknown"
        assert result.data.reporting_notes.presentation_format_detection_note is not None
        note = result.data.reporting_notes.presentation_format_detection_note
        assert note.scanned_by_function_tags == ["ifrs-full:CostOfSales"]
        assert note.scanned_by_nature_tags == ["ifrs-full:RawMaterialsAndConsumablesUsed"]
        assert note.matched == []

    def test_pre_ifrs07_payload_shape_behaviour(self) -> None:
        """Backwards-compat guard: pre-IFRS-07 shapes fail cleanly; partial IFRS-07 shapes parse."""
        import pydantic

        # Sub-case (a): pre-IFRS-07 payload — ``taxonomy`` entirely absent.
        pre_ifrs_payload = {
            "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
            "statement": "income",
            "period": "annual",
            "fiscal_year": 2024,
            "filing_accession": "0000320193-24-000081",
            "currency": "USD",
            "line_items": {"revenue": 391035000000},
            "metadata": {
                "source": "ixbrl",
                "data_completeness": 15,
                "expected_fields": 16,
                "source_tags": {"revenue": "us-gaap:Revenues"},
            },
        }
        with pytest.raises(pydantic.ValidationError):
            FinancialStatementResponse.model_validate(pre_ifrs_payload)

        # Sub-case (b): partial-IFRS-07 payload — ``taxonomy`` present, ``_reporting_notes`` absent.
        partial_ifrs_payload = {
            "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
            "statement": "income",
            "period": "annual",
            "fiscal_year": 2024,
            "filing_accession": "0000320193-24-000081",
            "currency": "USD",
            "taxonomy": "us-gaap",
            "line_items": {"revenue": 391035000000},
            "metadata": {
                "source": "ixbrl",
                "data_completeness": 15,
                "expected_fields": 16,
                "source_tags": {"revenue": "us-gaap:Revenues"},
            },
        }
        resp = FinancialStatementResponse.model_validate(partial_ifrs_payload)
        assert resp.reporting_notes is None


class TestFinancialsFields:
    @respx.mock
    def test_fields(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/financials/fields").mock(
            return_value=httpx.Response(200, json=FIELDS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.fields()

        assert route.called
        assert isinstance(result, DataResponse)
        assert isinstance(result.data, FieldsResponse)
        client.close()
