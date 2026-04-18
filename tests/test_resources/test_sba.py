"""Tests for the SBA resource."""

from __future__ import annotations

import httpx
import pytest
import respx

from thesma._types import DataResponse, PaginatedResponse
from thesma.client import ThesmaClient
from thesma.errors import BadRequestError, NotFoundError

BASE = "https://api.thesma.dev"


COUNTY_LENDING_RESPONSE = {
    "data": [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "county_fips": "06037",
            "loan_count": 142,
            "total_amount": 38_500_000.0,
            "avg_amount": 271_127.0,
            "median_amount": None,
            "guaranteed_amount": 28_000_000.0,
            "avg_guarantee_pct": 72.7,
            "jobs_supported": 1820,
            "charge_off_count": 3,
            "charge_off_rate": 2.11,
            "charge_off_amount": 410_000.0,
            "naics_code": None,
            "naics_match_level": None,
            "source": "SBA",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

STATE_LENDING_RESPONSE = {
    "data": [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "state_fips": "06",
            "loan_count": 1420,
            "total_amount": 385_000_000.0,
            "avg_amount": 271_127.0,
            "median_amount": None,
            "guaranteed_amount": 280_000_000.0,
            "avg_guarantee_pct": 72.7,
            "jobs_supported": 18200,
            "charge_off_count": 34,
            "charge_off_rate": 2.39,
            "charge_off_amount": 4_100_000.0,
            "naics_code": None,
            "naics_match_level": None,
            "source": "SBA",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

INDUSTRY_LENDING_RESPONSE = {
    "data": [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "naics_code": "541211",
            "naics_match_level": "6-digit",
            "geo": "national",
            "state_fips": None,
            "county_fips": None,
            "loan_count": 920,
            "total_amount": 210_000_000.0,
            "avg_amount": 228_260.0,
            "median_amount": 175_000.0,
            "guaranteed_amount": 150_000_000.0,
            "avg_guarantee_pct": 71.4,
            "jobs_supported": 9200,
            "charge_off_count": 12,
            "charge_off_rate": 1.30,
            "charge_off_amount": 1_500_000.0,
            "source": "SBA",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

LENDER_LIST_RESPONSE = {
    "data": [
        {
            "lender_id": 42,
            "display_name": "Live Oak Banking Co",
            "city": "Wilmington",
            "state": "NC",
            "loan_count": 521,
            "total_amount": 412_000_000.0,
            "avg_amount": 791_170.0,
            "market_share_pct": 5.4,
            "source": "SBA",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

LENDER_DETAIL_RESPONSE = {
    "data": {
        "lender_id": 42,
        "display_name": "Live Oak Banking Co",
        "city": "Wilmington",
        "state": "NC",
        "source": "manual_top_100",
        "first_seen_at": "2010-01-15",
        "last_seen_at": "2025-09-30",
        "history": [
            {
                "year": 2025,
                "quarter": 3,
                "period": "2025-Q3",
                "loan_count": 521,
                "total_amount": 412_000_000.0,
                "avg_amount": 791_170.0,
                "source": "SBA",
            }
        ],
    }
}

CHARACTERISTICS_RESPONSE = {
    "data": {
        "year": 2025,
        "quarter": 3,
        "period": "2025-Q3",
        "total_loans": 28341,
        "filter_scope": {"state": None, "county": None, "industry": None},
        "loan_size_buckets": [{"label": "<50K", "loan_count": 5824, "total_amount": 178_000_000.0, "pct": 20.55}],
        "term_length_buckets": [],
        "interest_rate_histogram": [],
        "sub_programme_mix": [],
        "business_type_mix": [],
        "revolving_vs_term": [],
    }
}

OUTCOMES_RESPONSE = {
    "data": [
        {
            "vintage_year": 2018,
            "loans_in_vintage": 14_087,
            "charged_off_count": 412,
            "charge_off_rate_pct": 2.92,
            "gross_charge_off_amount": 88_500_000.0,
            "avg_time_to_chargeoff_months": 41.3,
            "active_loan_count": 8421,
            "vintage_maturity": "mature",
            "source": "SBA",
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

METRIC_LIST_RESPONSE = {
    "data": [
        {
            "canonical_name": "loan_count_4q",
            "display_name": "Trailing 4Q loan count",
            "description": "Total SBA 7(a) loans approved in the trailing four quarters.",
            "category": "volume",
            "unit": "count",
            "update_cadence": "quarterly",
            "typical_lag_months": 1,
        }
    ],
    "pagination": {"page": 1, "per_page": 25, "total": 1, "total_pages": 1},
}

METRIC_DETAIL_RESPONSE = {
    "data": {
        "canonical_name": "loan_count_4q",
        "display_name": "Trailing 4Q loan count",
        "description": "Total SBA 7(a) loans approved in the trailing four quarters.",
        "category": "volume",
        "unit": "count",
        "update_cadence": "quarterly",
        "typical_lag_months": 1,
        "data_availability": {"min": 2010, "max": 2025},
        "related_endpoints": ["/v1/us/sba/counties/{fips}/lending"],
    }
}


class TestCountyLending:
    @respx.mock
    def test_basic_call(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/counties/06037/lending").mock(
            return_value=httpx.Response(200, json=COUNTY_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.county_lending("06037")

        assert route.called
        assert isinstance(result, PaginatedResponse)
        assert result.data[0].county_fips == "06037"
        assert result.pagination.total == 1
        client.close()

    @respx.mock
    def test_with_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/counties/06037/lending").mock(
            return_value=httpx.Response(200, json=COUNTY_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.county_lending(
            "06037",
            industry="722511",
            from_period="2024-Q1",
            to_period="2025-Q3",
            page=2,
            per_page=50,
        )

        url_str = str(route.calls.last.request.url)
        assert "industry=722511" in url_str
        assert "from=2024-Q1" in url_str
        assert "to=2025-Q3" in url_str
        assert "page=2" in url_str
        assert "per_page=50" in url_str
        assert "from_period" not in url_str
        client.close()

    @respx.mock
    def test_404_propagates(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/counties/99999/lending").mock(
            return_value=httpx.Response(404, json={"error": {"message": "not found", "code": "not_found"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(NotFoundError):
            client.sba.county_lending("99999")
        client.close()

    @respx.mock
    def test_400_propagates_for_invalid_fips_format(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/counties/abc/lending").mock(
            return_value=httpx.Response(400, json={"error": {"message": "invalid fips", "code": "bad_request"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.county_lending("abc")
        client.close()

    @respx.mock
    def test_naics_cascade_returns_match_level(self, api_key: str) -> None:
        payload = {
            "data": [
                {
                    **COUNTY_LENDING_RESPONSE["data"][0],
                    "naics_code": "5412",
                    "naics_match_level": "4-digit",
                }
            ],
            "pagination": COUNTY_LENDING_RESPONSE["pagination"],
        }
        respx.get(f"{BASE}/v1/us/sba/counties/06037/lending").mock(
            return_value=httpx.Response(200, json=payload),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.county_lending("06037", industry="541211")
        assert result.data[0].naics_match_level == "4-digit"
        client.close()


class TestStateLending:
    @respx.mock
    def test_basic_call(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/states/06/lending").mock(
            return_value=httpx.Response(200, json=STATE_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.state_lending("06")

        assert route.called
        assert result.data[0].state_fips == "06"
        assert result.data[0].loan_count == 1420
        client.close()

    @respx.mock
    def test_state_fips_00_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/states/00/lending").mock(
            return_value=httpx.Response(
                400,
                json={"error": {"message": "use /industries endpoint for national", "code": "bad_request"}},
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.state_lending("00")
        client.close()


class TestIndustryLending:
    @respx.mock
    def test_geo_national_default_omits_param(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/industries/541211/lending").mock(
            return_value=httpx.Response(200, json=INDUSTRY_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.industry_lending("541211")

        url_str = str(route.calls.last.request.url)
        assert "geo=" not in url_str
        client.close()

    @respx.mock
    def test_geo_state_with_state_fips(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/industries/541211/lending").mock(
            return_value=httpx.Response(200, json=INDUSTRY_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.industry_lending("541211", geo="state", state="06")

        url_str = str(route.calls.last.request.url)
        assert "geo=state" in url_str
        assert "state=06" in url_str
        client.close()

    @respx.mock
    def test_geo_county_with_county_fips(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/industries/541211/lending").mock(
            return_value=httpx.Response(200, json=INDUSTRY_LENDING_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.industry_lending("541211", geo="county", county="06037")

        url_str = str(route.calls.last.request.url)
        assert "geo=county" in url_str
        assert "county=06037" in url_str
        client.close()


class TestLenders:
    @respx.mock
    def test_default_sort_loan_count(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lenders").mock(
            return_value=httpx.Response(200, json=LENDER_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lenders()

        url_str = str(route.calls.last.request.url)
        assert "sort=loan_count" in url_str
        client.close()

    @respx.mock
    def test_sort_by_total_amount(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lenders").mock(
            return_value=httpx.Response(200, json=LENDER_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lenders(sort="total_amount")

        assert "sort=total_amount" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_invalid_sort_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lenders").mock(
            return_value=httpx.Response(400, json={"error": {"message": "invalid sort", "code": "bad_request"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.lenders(sort="bogus")
        client.close()

    @respx.mock
    def test_with_state_filter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lenders").mock(
            return_value=httpx.Response(200, json=LENDER_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lenders(state="06")

        assert "state=06" in str(route.calls.last.request.url)
        client.close()


class TestLender:
    @respx.mock
    def test_lender_by_id(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lenders/42").mock(
            return_value=httpx.Response(200, json=LENDER_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.lender(42)

        assert route.called
        assert isinstance(result, DataResponse)
        assert result.data.lender_id == 42
        assert result.data.display_name == "Live Oak Banking Co"
        assert result.data.history[0].quarter == 3
        client.close()

    @respx.mock
    def test_lender_404_propagates(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lenders/9999").mock(
            return_value=httpx.Response(404, json={"error": {"message": "not found", "code": "not_found"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(NotFoundError):
            client.sba.lender(9999)
        client.close()

    @respx.mock
    def test_lender_with_period_filter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lenders/42").mock(
            return_value=httpx.Response(200, json=LENDER_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lender(42, from_period="2024-Q1")

        assert "from=2024-Q1" in str(route.calls.last.request.url)
        client.close()


class TestLendingCharacteristics:
    @respx.mock
    def test_basic_call_with_year_and_quarter(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lending/characteristics").mock(
            return_value=httpx.Response(200, json=CHARACTERISTICS_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.lending_characteristics(year=2025, quarter=3)

        assert result.data.total_loans == 28341
        assert result.data.loan_size_buckets[0].label == "<50K"
        client.close()

    @respx.mock
    def test_missing_year_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lending/characteristics").mock(
            return_value=httpx.Response(400, json={"error": {"message": "year required", "code": "bad_request"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.lending_characteristics(quarter=3)
        client.close()

    @respx.mock
    def test_with_state_and_industry_filters(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lending/characteristics").mock(
            return_value=httpx.Response(200, json=CHARACTERISTICS_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lending_characteristics(year=2025, quarter=3, state="06", industry="722511")

        url_str = str(route.calls.last.request.url)
        assert "year=2025" in url_str
        assert "quarter=3" in url_str
        assert "state=06" in url_str
        assert "industry=722511" in url_str
        client.close()


class TestLendingOutcomes:
    @respx.mock
    def test_basic_call_with_vintage_from(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lending/outcomes").mock(
            return_value=httpx.Response(200, json=OUTCOMES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.lending_outcomes(vintage_from=2018)

        assert result.data[0].vintage_year == 2018
        assert result.data[0].vintage_maturity == "mature"
        client.close()

    @respx.mock
    def test_with_vintage_range(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/lending/outcomes").mock(
            return_value=httpx.Response(200, json=OUTCOMES_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.lending_outcomes(vintage_from=2015, vintage_to=2020)

        url_str = str(route.calls.last.request.url)
        assert "vintage_from=2015" in url_str
        assert "vintage_to=2020" in url_str
        client.close()

    @respx.mock
    def test_vintage_range_too_wide_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lending/outcomes").mock(
            return_value=httpx.Response(400, json={"error": {"message": "range > 10 years", "code": "bad_request"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.lending_outcomes(vintage_from=2010, vintage_to=2025)
        client.close()

    @respx.mock
    def test_missing_vintage_from_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/lending/outcomes").mock(
            return_value=httpx.Response(
                400, json={"error": {"message": "vintage_from required", "code": "bad_request"}}
            ),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.lending_outcomes()
        client.close()


class TestMetrics:
    @respx.mock
    def test_basic_list(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/metrics").mock(
            return_value=httpx.Response(200, json=METRIC_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.metrics()

        assert result.pagination.total == 1
        assert result.data[0].canonical_name == "loan_count_4q"
        client.close()

    @respx.mock
    def test_with_category_filter(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/metrics").mock(
            return_value=httpx.Response(200, json=METRIC_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.metrics(category="volume")

        assert "category=volume" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_with_search(self, api_key: str) -> None:
        route = respx.get(f"{BASE}/v1/us/sba/metrics").mock(
            return_value=httpx.Response(200, json=METRIC_LIST_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        client.sba.metrics(search="loan")

        assert "search=loan" in str(route.calls.last.request.url)
        client.close()

    @respx.mock
    def test_search_below_min_length_propagates_400(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/metrics").mock(
            return_value=httpx.Response(400, json={"error": {"message": "min_length=2", "code": "bad_request"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(BadRequestError):
            client.sba.metrics(search="a")
        client.close()


class TestMetric:
    @respx.mock
    def test_metric_by_name(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/metrics/loan_count_4q").mock(
            return_value=httpx.Response(200, json=METRIC_DETAIL_RESPONSE),
        )
        client = ThesmaClient(api_key=api_key)
        result = client.sba.metric("loan_count_4q")

        assert result.data.canonical_name == "loan_count_4q"
        assert result.data.data_availability.max == 2025
        client.close()

    @respx.mock
    def test_metric_unknown_name_404(self, api_key: str) -> None:
        respx.get(f"{BASE}/v1/us/sba/metrics/bogus").mock(
            return_value=httpx.Response(404, json={"error": {"message": "not found", "code": "not_found"}}),
        )
        client = ThesmaClient(api_key=api_key)
        with pytest.raises(NotFoundError):
            client.sba.metric("bogus")
        client.close()
