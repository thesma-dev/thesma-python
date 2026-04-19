"""Tests for the Screener resource."""

from __future__ import annotations

import httpx
import pytest
import respx

from thesma._types import PaginatedResponse
from thesma.client import AsyncThesmaClient, ThesmaClient
from thesma.errors import BadRequestError, ThesmaError

BASE = "https://api.thesma.dev"

SCREENER_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "company_tier": "sp500",
            "exchange": "NASDAQ",
            "domicile": "us",
            "fiscal_year": 2024,
            "financials": {
                "revenue": 383285000000,
                "net_income": 96995000000,
                "eps_diluted": 6.08,
                "common_shares_outstanding": 15550061000,
                "total_equity": 62146000000,
                "dividends_paid": -15025000000,
                "institutional_ownership_pct": 62.5,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


SCREENER_JSON_NULL_FINANCIALS = {
    "data": [
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": None,
                "net_income": None,
                "eps_diluted": None,
                "common_shares_outstanding": None,
                "total_equity": None,
                "dividends_paid": None,
                "institutional_ownership_pct": None,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
        },
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


SCREENER_LAUS_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": 383285000000,
                "net_income": 96995000000,
                "eps_diluted": 6.08,
                "common_shares_outstanding": 15550061000,
                "total_equity": 62146000000,
                "dividends_paid": -15025000000,
                "institutional_ownership_pct": 62.5,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
            "labor_context": {
                "industry_hiring_trend": "stable",
                "industry_employment_growth_yoy": 1.2,
                "industry_wage_growth_yoy": 3.5,
                "hq_county_wage_growth_yoy": 4.1,
                "comp_to_market_ratio": None,
                "industry_quits_rate": None,
                "industry_openings_rate": None,
                "labour_market_tightness": None,
                "local_unemployment_rate": 2.8,
                "local_unemployment_trend": "improving",
                "local_labor_force": 1050450,
                "data_freshness": {
                    "ces_period": "2025-11",
                    "qcew_period": "2025-Q2",
                    "jolts_period": "2025-10",
                    "laus_period": "2025-11",
                },
            },
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestScreenerScreen:
    @respx.mock
    def test_screen_with_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(tier="sp500", min_gross_margin=40)

        assert route.called
        request = route.calls.last.request
        assert "tier=sp500" in str(request.url)
        assert "min_gross_margin=40" in str(request.url)
        assert isinstance(result, PaginatedResponse)
        client.close()

    @respx.mock
    def test_screen_response_type(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert isinstance(result, PaginatedResponse)
        assert len(result.data) == 1
        assert result.data[0].cik == "0000320193"
        client.close()

    @respx.mock
    def test_screen_with_sic_single_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sic="7372")

        request = route.calls.last.request
        assert "sic=7372" in str(request.url)
        client.close()

    @respx.mock
    def test_screen_with_sic_multi_value(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sic=["7372", "3674"])

        request = route.calls.last.request
        url_str = str(request.url)
        assert "sic=7372" in url_str
        assert "sic=3674" in url_str
        client.close()

    @respx.mock
    def test_screen_sort_maps_to_api_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(sort_by="gross_margin", order="desc")

        request = route.calls.last.request
        assert "sort=gross_margin" in str(request.url)
        assert "order=desc" in str(request.url)
        client.close()


class TestScreenerEnhancements:
    @respx.mock
    def test_new_params_passed_to_api(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(
            max_net_income=50000,
            min_institutional_ownership_pct=30.0,
            insider_buying_days="90",
        )

        request = route.calls.last.request
        assert "max_net_income=50000" in str(request.url)
        assert "min_institutional_ownership_pct=30.0" in str(request.url)
        assert "insider_buying_days=90" in str(request.url)
        client.close()

    @respx.mock
    def test_max_net_income_zero_not_stripped(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_net_income=0)

        request = route.calls.last.request
        assert "max_net_income=0" in str(request.url)
        client.close()

    @respx.mock
    def test_insider_buying_days_without_has_insider_buying(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(insider_buying_days="30")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "insider_buying_days=30" in url_str
        assert "has_insider_buying" not in url_str
        client.close()

    @respx.mock
    def test_financials_deserialized(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        item = result.data[0]
        assert item.financials["revenue"] == 383285000000
        assert item.financials["eps_diluted"] == 6.08
        assert item.financials["institutional_ownership_pct"] == 62.5
        client.close()

    @respx.mock
    def test_financials_all_null(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON_NULL_FINANCIALS),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        item = result.data[0]
        assert item.financials["revenue"] is None
        assert item.financials["net_income"] is None
        assert item.financials["eps_diluted"] is None
        assert item.financials["common_shares_outstanding"] is None
        assert item.financials["total_equity"] is None
        assert item.financials["dividends_paid"] is None
        assert item.financials["institutional_ownership_pct"] is None
        client.close()

    @respx.mock
    def test_negative_dividends_paid(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert result.data[0].financials["dividends_paid"] == -15025000000
        client.close()

    @respx.mock
    def test_existing_ratios_still_accessible(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        assert result.data[0].ratios["gross_margin"] == 46.2
        client.close()

    @respx.mock
    def test_search_kwarg_passed_to_api(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(search="AAPL")

        assert "search=AAPL" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_search_lowercase_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(search="aapl")

        assert "search=aapl" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_search_combined_with_financial_filter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(search="GO", min_gross_margin=40.0)

        url_str = str(route.calls.last.request.url)
        assert "search=GO" in url_str
        assert "min_gross_margin=40" in url_str
        client.close()

    @respx.mock
    def test_search_none_omits_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_gross_margin=40.0)

        assert "search=" not in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_search_empty_string_ships_with_empty_value(self, api_key: str) -> None:
        """``search=""`` must round-trip as ``?search=`` — the SDK must
        not silently coerce empty strings to ``None``. The API treats
        empty/whitespace-only as "no filter", so this is harmless.
        """
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(search="")

        assert "search=" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_search_preserved_across_pagination(self, api_key: str) -> None:
        """Guards against a ``_fetch_page`` refactor silently dropping filters."""
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(search="AAPL", page=2, per_page=5)

        url_str = str(route.calls.last.request.url)
        assert "search=AAPL" in url_str
        assert "page=2" in url_str
        assert "per_page=5" in url_str

        # Second-page fetch must preserve the search filter. The private
        # ``_fetch_page`` hook is what ``next_page`` / ``auto_paging_iter``
        # call through — exercise it directly to avoid depending on the
        # mock returning ``total_pages > 1``.
        assert result._fetch_page is not None
        result._fetch_page(3)
        second_url = str(route.calls.last.request.url)
        assert "search=AAPL" in second_url
        client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_screen_passes_search(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        async with AsyncThesmaClient(api_key=api_key) as client:
            await client.screener.screen(search="AAPL")

        assert "search=AAPL" in str(route.calls.last.request.url)


class TestScreenerBlsFilters:
    @respx.mock
    def test_screener_bls_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(
            industry_hiring_trend="stable",
            min_industry_employment_growth=1.0,
            max_industry_employment_growth=5.0,
            min_industry_wage_growth=2.0,
            min_hq_county_wage_growth=1.5,
            min_comp_to_market_ratio=10.0,
        )

        url_str = str(route.calls.last.request.url)
        assert "industry_hiring_trend=stable" in url_str
        assert "min_industry_employment_growth=1.0" in url_str
        assert "max_industry_employment_growth=5.0" in url_str
        assert "min_industry_wage_growth=2.0" in url_str
        assert "min_hq_county_wage_growth=1.5" in url_str
        assert "min_comp_to_market_ratio=10.0" in url_str
        client.close()

    @respx.mock
    def test_screener_bls_filters_omitted_when_none(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen()

        url_str = str(route.calls.last.request.url)
        assert "industry_hiring_trend" not in url_str
        assert "min_industry_employment_growth" not in url_str
        assert "min_comp_to_market_ratio" not in url_str
        client.close()

    @respx.mock
    def test_screener_bls_filters_mixed_with_sec(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_revenue=1000000, industry_hiring_trend="accelerating")

        url_str = str(route.calls.last.request.url)
        assert "min_revenue=1000000" in url_str
        assert "industry_hiring_trend=accelerating" in url_str
        client.close()


class TestScreenerLausFilters:
    @respx.mock
    def test_screener_min_local_unemployment_rate(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_local_unemployment_rate=2.0)

        assert "min_local_unemployment_rate=2.0" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_max_local_unemployment_rate(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_local_unemployment_rate=4.0)

        assert "max_local_unemployment_rate=4.0" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_local_unemployment_trend_improving(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(local_unemployment_trend="improving")

        assert "local_unemployment_trend=improving" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_local_unemployment_trend_stable(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(local_unemployment_trend="stable")

        assert "local_unemployment_trend=stable" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_local_unemployment_trend_worsening(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(local_unemployment_trend="worsening")

        assert "local_unemployment_trend=worsening" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_min_local_labor_force(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_local_labor_force=500_000)

        url_str = str(route.calls.last.request.url)
        assert "min_local_labor_force=500000" in url_str
        # Ensure no scientific notation from float coercion
        assert "5e" not in url_str
        client.close()

    @respx.mock
    def test_screener_all_laus_filters_combined(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(
            min_local_unemployment_rate=2.0,
            max_local_unemployment_rate=4.0,
            local_unemployment_trend="improving",
            min_local_labor_force=500_000,
            min_revenue=1_000_000,
        )

        url_str = str(route.calls.last.request.url)
        assert "min_local_unemployment_rate=2.0" in url_str
        assert "max_local_unemployment_rate=4.0" in url_str
        assert "local_unemployment_trend=improving" in url_str
        assert "min_local_labor_force=500000" in url_str
        assert "min_revenue=1000000" in url_str
        client.close()

    @respx.mock
    def test_screener_laus_filters_omitted_when_none(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen()

        url_str = str(route.calls.last.request.url)
        assert "min_local_unemployment_rate" not in url_str
        assert "max_local_unemployment_rate" not in url_str
        assert "local_unemployment_trend" not in url_str
        assert "min_local_labor_force" not in url_str
        client.close()

    @respx.mock
    def test_screener_response_includes_laus_fields(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(min_local_unemployment_rate=2.0)

        labor_context = result.data[0].labor_context  # type: ignore[attr-defined]
        assert labor_context["local_unemployment_rate"] == 2.8
        assert labor_context["local_unemployment_trend"] == "improving"
        assert labor_context["local_labor_force"] == 1050450
        client.close()

    @respx.mock
    def test_screener_response_includes_laus_period_in_data_freshness(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        labor_context = result.data[0].labor_context  # type: ignore[attr-defined]
        assert labor_context["data_freshness"]["laus_period"] == "2025-11"
        client.close()

    @respx.mock
    def test_screener_invalid_trend_propagates_422(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(
                422,
                json={
                    "error": {
                        "message": "Invalid value for local_unemployment_trend",
                        "code": "validation_error",
                    }
                },
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(ThesmaError):
            client.screener.screen(local_unemployment_trend="foo")
        client.close()

    @respx.mock
    def test_screener_existing_data_freshness_assertions_still_pass(self, api_key: str) -> None:
        """Sanity: the pre-existing ``ces_period`` field still parses after regeneration."""
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_LAUS_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        labor_context = result.data[0].labor_context  # type: ignore[attr-defined]
        assert labor_context["data_freshness"]["ces_period"] == "2025-11"
        assert labor_context["data_freshness"]["qcew_period"] == "2025-Q2"
        assert labor_context["data_freshness"]["jolts_period"] == "2025-10"
        client.close()


class TestScreenerExchangeDomicile:
    @respx.mock
    def test_screener_with_exchange_single(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(exchange="nyse")

        assert "exchange=nyse" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_with_exchange_multiple(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(exchange=["nyse", "nasdaq"])

        url_str = str(route.calls.last.request.url)
        assert "exchange=nyse" in url_str
        assert "exchange=nasdaq" in url_str
        client.close()

    @respx.mock
    def test_screener_with_domicile(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(domicile="us")

        assert "domicile=us" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_exchange_domicile_combined_with_financial_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(exchange=["nyse"], domicile="us", tier="sp500", min_revenue=1_000_000)

        url_str = str(route.calls.last.request.url)
        assert "exchange=nyse" in url_str
        assert "domicile=us" in url_str
        assert "tier=sp500" in url_str
        assert "min_revenue=1000000" in url_str
        client.close()

    @respx.mock
    def test_screener_exchange_domicile_omitted_when_none(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen()

        url_str = str(route.calls.last.request.url)
        assert "exchange=" not in url_str
        assert "domicile=" not in url_str
        client.close()

    @respx.mock
    def test_screener_exchange_empty_list_omitted(self, api_key: str) -> None:
        """Empty list must be normalised to None so httpx does not attempt to serialise it."""
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(exchange=[])

        assert "exchange=" not in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_screener_response_carries_exchange_and_domicile(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        # ScreenerResultItem remains an extra="allow" stub post-regeneration;
        # access via the stub-attr pattern.
        assert result.data[0].exchange == "NASDAQ"  # type: ignore[attr-defined]
        assert result.data[0].domicile == "us"  # type: ignore[attr-defined]
        client.close()

    @respx.mock
    def test_screener_invalid_exchange_raises_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(
                400,
                json={"error": {"code": "invalid_parameter", "message": "Invalid exchange 'amex'..."}},
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.screener.screen(exchange="amex")
        client.close()


SCREENER_SBA_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": 383285000000,
                "net_income": 96995000000,
                "eps_diluted": 6.08,
                "common_shares_outstanding": 15550061000,
                "total_equity": 62146000000,
                "dividends_paid": -15025000000,
                "institutional_ownership_pct": 62.5,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
            "lending_context": {
                "local_sba_loan_count_4q": 520,
                "local_sba_lending_growth_yoy": 8.4,
                "industry_sba_lending_growth_yoy": 6.1,
                "industry_sba_charge_off_rate": 1.9,
            },
            "data_freshness": {"sba_period": "2025-Q4"},
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


SCREENER_DUAL_CONTEXT_JSON = {
    "data": [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "company_tier": "sp500",
            "fiscal_year": 2024,
            "financials": {
                "revenue": 383285000000,
                "net_income": 96995000000,
                "eps_diluted": 6.08,
                "common_shares_outstanding": 15550061000,
                "total_equity": 62146000000,
                "dividends_paid": -15025000000,
                "institutional_ownership_pct": 62.5,
            },
            "ratios": {
                "gross_margin": 46.2,
                "operating_margin": 31.5,
                "net_margin": 26.4,
                "debt_to_equity": 1.87,
            },
            "labor_context": {
                "industry_hiring_trend": "stable",
                "data_freshness": {
                    "ces_period": "2025-11",
                    "qcew_period": "2025-Q2",
                    "jolts_period": "2025-10",
                    "laus_period": "2025-11",
                },
            },
            "lending_context": {
                "local_sba_loan_count_4q": 520,
                "local_sba_lending_growth_yoy": 8.4,
                "industry_sba_lending_growth_yoy": 6.1,
                "industry_sba_charge_off_rate": 1.9,
            },
            "data_freshness": {"sba_period": "2025-Q4"},
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}


class TestScreenerSbaFilters:
    @respx.mock
    def test_min_local_sba_loan_count_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_local_sba_loan_count=100)

        assert "min_local_sba_loan_count=100" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_max_local_sba_loan_count_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_local_sba_loan_count=1000)

        assert "max_local_sba_loan_count=1000" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_min_local_sba_lending_growth_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_local_sba_lending_growth=5.0)

        url_str = str(route.calls.last.request.url)
        assert "min_local_sba_lending_growth=5" in url_str
        client.close()

    @respx.mock
    def test_max_local_sba_lending_growth_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_local_sba_lending_growth=20.0)

        assert "max_local_sba_lending_growth=20" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_min_industry_sba_lending_growth_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_industry_sba_lending_growth=3.5)

        assert "min_industry_sba_lending_growth=3.5" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_max_industry_sba_charge_off_rate_passes_through(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(max_industry_sba_charge_off_rate=10.0)

        assert "max_industry_sba_charge_off_rate=10" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_no_sba_filter_omits_all_six_params(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen()

        url_str = str(route.calls.last.request.url)
        for name in (
            "min_local_sba_loan_count",
            "max_local_sba_loan_count",
            "min_local_sba_lending_growth",
            "max_local_sba_lending_growth",
            "min_industry_sba_lending_growth",
            "max_industry_sba_charge_off_rate",
        ):
            assert name not in url_str
        client.close()

    @respx.mock
    def test_combined_sba_and_bls_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        client.screener.screen(min_local_sba_loan_count=100, min_industry_employment_growth=2.0)

        url_str = str(route.calls.last.request.url)
        assert "min_local_sba_loan_count=100" in url_str
        assert "min_industry_employment_growth=2" in url_str
        client.close()

    @respx.mock
    def test_screener_response_includes_lending_context(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(min_local_sba_loan_count=100)

        lending_context = result.data[0].lending_context  # type: ignore[attr-defined]
        assert lending_context["local_sba_loan_count_4q"] == 520
        assert lending_context["industry_sba_charge_off_rate"] == 1.9
        assert "data_freshness" not in lending_context
        client.close()

    @respx.mock
    def test_screener_response_includes_top_level_data_freshness_with_sba_period(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen()

        data_freshness = result.data[0].data_freshness  # type: ignore[attr-defined]
        assert data_freshness["sba_period"] == "2025-Q4"
        client.close()

    @respx.mock
    def test_include_lending_context_alone(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_SBA_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(include="lending_context")

        assert "include=lending_context" in str(route.calls.last.request.url)
        assert result.data[0].lending_context is not None  # type: ignore[attr-defined]
        client.close()

    @respx.mock
    def test_include_labor_and_lending_context_combined(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_DUAL_CONTEXT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(include="labor_context,lending_context")

        url_str = str(route.calls.last.request.url)
        assert "include=labor_context%2Clending_context" in url_str
        item = result.data[0]
        assert item.labor_context is not None  # type: ignore[attr-defined]
        assert item.lending_context is not None  # type: ignore[attr-defined]
        assert item.data_freshness is not None  # type: ignore[attr-defined]
        client.close()

    @respx.mock
    def test_two_freshness_objects_coexist(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sec/screener").mock(
            return_value=httpx.Response(200, json=SCREENER_DUAL_CONTEXT_JSON),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.screener.screen(include="labor_context,lending_context")

        item = result.data[0]
        assert item.labor_context["data_freshness"]["ces_period"] == "2025-11"  # type: ignore[attr-defined]
        assert item.data_freshness["sba_period"] == "2025-Q4"  # type: ignore[attr-defined]
        client.close()
