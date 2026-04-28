"""Tests for the Financials resource."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from thesma._generated.models import (
    EnrichedFinancialDataResponse,
    EnrichedMultiStatementPaginatedResponse,
    EnrichedMultiStatementResponse,
    FieldsResponse,
    FinancialStatementResponse,
    TimeSeriesResponse,
)
from thesma._types import DataResponse, PaginatedResponse
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import BadRequestError

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
    },
    # Post-SDK-33: labor_context sits at envelope root, not inside ``data``,
    # matching what the server's EnrichedFinancialDataResponse serializer emits.
    "labor_context": {
        "industry": {
            "naics_code": "334111",
            "naics_description": "Electronic Computer Manufacturing",
            "naics_match_level": "6-digit",
            "data_period": "2025-Q2",
        },
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
        assert isinstance(result, EnrichedFinancialDataResponse)
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
        assert isinstance(result, EnrichedFinancialDataResponse)
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
    def test_get_with_include_labor_context_surfaces_typed_envelope(self, api_key: str) -> None:
        """SDK-33: ``labor_context`` now surfaces as a typed envelope-root
        attribute (``result.labor_context``). Pre-SDK-33 it was silently
        dropped by ``DataResponse[FinancialStatementResponse]``.
        """
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIALS_WITH_LAUS_LOCAL_MARKET_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.financials.get("0000320193", statement="income", include="labor_context")

        assert result is not None
        assert isinstance(result, EnrichedFinancialDataResponse)
        assert result.data.taxonomy == "us-gaap"
        assert result.data.reporting_notes is not None
        # Envelope-root typed labor_context is now populated.
        assert result.labor_context is not None
        assert result.labor_context.local_market is not None
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


# --- SDK-26 (per_page) + SDK-27 (statement=all) -----------------------------


def _list_item(year: int, *, currency: str = "USD", taxonomy: str = "us-gaap") -> dict[str, Any]:
    return {
        "company": {"cik": "0001639920", "ticker": "SPOT", "name": "Spotify Technology S.A."},
        "statement": "income",
        "period": "annual",
        "fiscal_year": year,
        "filing_accession": f"0001639920-{year % 100:02d}-000012",
        "currency": currency,
        "taxonomy": taxonomy,
        "line_items": {"revenue": 10_000_000_000 + year},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "ifrs-full:Revenue"},
        },
    }


def _statement_body(line_item_value: int) -> dict[str, Any]:
    return {
        "line_items": {"revenue": line_item_value},
        "field_confidence": {},
        "metadata": {
            "source": "ixbrl",
            "data_completeness": 15,
            "expected_fields": 16,
            "source_tags": {"revenue": "us-gaap:Revenues"},
        },
    }


def _multi_statement_period(year: int) -> dict[str, Any]:
    """Build a MultiStatementResponse-shaped payload period.

    Includes ``_reporting_notes`` at the top level because the regenerated
    ``MultiStatementResponse`` declares it as a required field with alias
    ``_reporting_notes``. The envelope classes (``EnrichedMultiStatementResponse`` /
    ``EnrichedMultiStatementPaginatedResponse``) are ``extra="allow"`` passthroughs
    today and never validate the nested period shape, but the fixture stays
    correct so future typing-tightening does not mass-break these tests.
    """
    return {
        "company": {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc."},
        "period": "annual",
        "fiscal_year": year,
        "filing_accession": f"0000320193-{year % 100:02d}-000081",
        "taxonomy": "us-gaap",
        "currency": "USD",
        "_reporting_notes": {"presentation_format": "by_function", "ifrs_18_applied": False},
        "statements": {
            "income": _statement_body(391_035_000_000),
            "balance_sheet": _statement_body(364_980_000_000),
            "cash_flow": _statement_body(118_000_000_000),
        },
    }


class TestFinancialsPerPage:
    """SDK-26: ``per_page=N`` returns the IFRS-09 paginated list envelope."""

    @respx.mock
    def test_get_without_per_page_returns_single_shape(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0000320193", statement="income")
        assert isinstance(resp, EnrichedFinancialDataResponse)
        assert isinstance(resp.data, FinancialStatementResponse)
        assert resp.data.fiscal_year == 2024
        client.close()

    @respx.mock
    def test_get_with_per_page_returns_paginated_shape(self, api_key: str) -> None:
        payload = {
            "data": [_list_item(y, currency="EUR", taxonomy="ifrs-full") for y in (2024, 2023, 2022, 2021, 2020)],
            "pagination": {"page": 1, "per_page": 5, "total": 5, "total_pages": 1},
        }
        route = respx.get(f"{BASE}/v1/us/sec/companies/0001639920/financials").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0001639920", statement="income", period="annual", per_page=5)
        assert isinstance(resp, PaginatedResponse)
        assert len(resp.data) == 5
        assert resp.pagination.total == 5
        # Fields reachable via the model — whether typed or via model_extra passthrough.
        first = resp.data[0]
        first_extra = first.model_extra or {}
        assert first_extra.get("currency") == "EUR" or getattr(first, "currency", None) == "EUR"
        assert "per_page=5" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_get_per_page_with_year_raises_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(
                400,
                json={
                    "error": {
                        "code": "validation_error",
                        "message": "per_page is mutually exclusive with year/quarter.",
                        "status": 400,
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.financials.get("0000320193", year=2024, per_page=5)
        client.close()

    @respx.mock
    def test_per_page_with_labor_context_attaches_per_element(self, api_key: str) -> None:
        """SDK-26 contract: list-mode enrichment lands per-element (IFRS-09 lock)."""
        item_0 = _list_item(2024, taxonomy="us-gaap")
        item_0["labor_context"] = {
            "industry": {
                "naics_code": "334111",
                "naics_description": "Electronic Computer Manufacturing",
                "naics_match_level": "6-digit",
                "data_period": "2025-Q2",
            },
            "summary": {
                "industry_hiring_trend": "stable",
                "local_unemployment_trend": "improving",
                "comp_to_market_ratio": 1.12,
                "labour_market_tightness": 1.3,
            },
            "data_freshness": {"ces_period": "2025-11"},
        }
        item_1 = _list_item(2023, taxonomy="us-gaap")
        item_1["labor_context"] = item_0["labor_context"]
        payload = {
            "data": [item_0, item_1],
            "pagination": {"page": 1, "per_page": 2, "total": 2, "total_pages": 1},
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get(
            "0000320193",
            statement="income",
            per_page=2,
            include="labor_context",
        )
        assert isinstance(resp, PaginatedResponse)
        for row in resp.data:
            row_extra = row.model_extra or {}
            assert row_extra.get("labor_context") is not None or getattr(row, "labor_context", None) is not None
        client.close()


class TestFinancialsStatementAll:
    """SDK-27: ``statement='all'`` returns all three statements in one call."""

    @respx.mock
    def test_get_statement_all_single_period(self, api_key: str) -> None:
        payload = {"data": _multi_statement_period(2024)}
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0000320193", statement="all", year=2024)
        assert isinstance(resp, EnrichedMultiStatementResponse)
        # SDK-33: typed access on ``resp.data`` — the inner statements dict
        # now parses into ``MultiStatementResponse.statements``.
        assert resp.data.statements["income"] is not None
        assert resp.data.statements["balance_sheet"] is not None
        assert resp.data.statements["cash_flow"] is not None
        client.close()

    @respx.mock
    def test_get_statement_all_missing_cash_flow(self, api_key: str) -> None:
        period = _multi_statement_period(2024)
        period["statements"]["cash_flow"] = None
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json={"data": period}),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0000320193", statement="all", year=2024)
        assert isinstance(resp, EnrichedMultiStatementResponse)
        assert resp.data.statements["cash_flow"] is None
        assert resp.data.statements["income"] is not None
        client.close()

    @respx.mock
    def test_get_statement_all_with_per_page(self, api_key: str) -> None:
        payload = {
            "data": [_multi_statement_period(y) for y in (2024, 2023, 2022)],
            "pagination": {"page": 1, "per_page": 3, "total": 3, "total_pages": 1},
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0000320193", statement="all", per_page=3)
        assert isinstance(resp, EnrichedMultiStatementPaginatedResponse)
        assert len(resp.data) == 3
        for period in resp.data:
            # Per-element list items keep ``extra="allow"`` passthrough
            # for ``statements`` (they don't subclass MultiStatementResponse
            # because of codegen emission order).
            period_extra = period.model_extra or {}
            assert period_extra.get("statements", {}).get("income") is not None
        client.close()

    @respx.mock
    def test_get_statement_all_per_page_enrichment_at_envelope_root(self, api_key: str) -> None:
        """LOAD-BEARING: `statement='all'` + `per_page` places enrichment at envelope root, not per-element.

        Locks the S2 exception to the IFRS-09 per-element rule. Don't let drift creep in.
        """
        payload = {
            "data": [_multi_statement_period(2024), _multi_statement_period(2023)],
            "pagination": {"page": 1, "per_page": 2, "total": 2, "total_pages": 1},
            "labor_context": {
                "industry": {
                    "naics_code": "334111",
                    "naics_description": "Electronic Computer Manufacturing",
                    "naics_match_level": "6-digit",
                    "data_period": "2025-Q2",
                },
                "summary": {"industry_hiring_trend": "stable"},
            },
            "lending_context": {"local_market": None, "industry_lending": None},
        }
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get(
            "0000320193",
            statement="all",
            per_page=2,
            include="labor_context,lending_context",
        )
        assert isinstance(resp, EnrichedMultiStatementPaginatedResponse)
        # SDK-33: envelope-root enrichment is now typed (labor_context /
        # lending_context are declared attributes on the response class).
        assert resp.labor_context is not None
        assert resp.lending_context is not None
        # Per-element enrichment stays None — the S2 lock still holds.
        for period in resp.data:
            assert period.labor_context is None
            assert period.lending_context is None
        client.close()

    @respx.mock
    def test_dispatch_single_statement_path_unaffected(self, api_key: str) -> None:
        """Regression: `statement='income'` + no `per_page` still returns the single-statement shape."""
        respx.get(f"{BASE}/v1/us/sec/companies/0000320193/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get("0000320193", statement="income", year=2024)
        assert isinstance(resp, EnrichedFinancialDataResponse)
        assert isinstance(resp.data, FinancialStatementResponse)
        client.close()


class TestFinancialsByIdentifier:
    """SDK-40: ``identifier=`` accepts ticker for both ``get`` and ``time_series``."""

    @respx.mock
    def test_get_by_ticker(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/AAPL/financials").mock(
            return_value=httpx.Response(200, json=FINANCIAL_STATEMENT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        resp = client.financials.get(identifier="AAPL", statement="income")

        assert route.called
        assert isinstance(resp, EnrichedFinancialDataResponse)
        assert resp.data.company.cik == "0000320193"
        client.close()

    @respx.mock
    def test_time_series_by_ticker(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/companies/AAPL/financials/revenue").mock(
            return_value=httpx.Response(200, json=TIME_SERIES_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.financials.time_series(identifier="AAPL", metric="revenue")

        assert route.called
        client.close()
