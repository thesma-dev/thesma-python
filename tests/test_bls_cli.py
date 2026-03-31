"""Tests for the BLS CLI commands."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from thesma.cli.main import cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_bls_mock_client() -> MagicMock:
    """Create a mock ThesmaClient with BLS responses."""
    client = MagicMock()

    # industries() -> PaginatedResponse with .data as list of dicts
    industries_response = MagicMock()
    industries_response.data = [
        {"naics_code": "52", "title": "Finance and Insurance", "level": 2},
    ]
    client.bls.industries.return_value = industries_response

    # industry() -> DataResponse with .data as mock model
    industry_detail = MagicMock()
    industry_detail.model_dump.return_value = {
        "naics_code": "522110",
        "title": "Commercial Banking",
        "level": 6,
        "parent_naics": "52211",
        "has_ces_data": True,
        "has_qcew_data": True,
        "has_oews_data": True,
        "children": [{"naics_code": "522111", "title": "Sub-sector", "level": 7}],
        "data_availability": {
            "ces_years": {"min": 2010, "max": 2026},
            "qcew_years": {"min": 2010, "max": 2025},
            "oews_years": {"min": 2015, "max": 2024},
        },
    }
    industry_response = MagicMock()
    industry_response.data = industry_detail
    client.bls.industry.return_value = industry_response

    # employment() -> PaginatedResponse with .data as list of dicts
    employment_response = MagicMock()
    employment_response.data = [
        {
            "period": "2024-01",
            "all_employees_thousands": 2100.5,
            "avg_hourly_earnings": 35.20,
            "avg_weekly_earnings": 1408.0,
        },
    ]
    client.bls.employment.return_value = employment_response

    # employment_latest() -> DataResponse with .data as mock model
    employment_latest_detail = MagicMock()
    employment_latest_detail.model_dump.return_value = {
        "year": 2026,
        "month": 2,
        "period": "2026-02",
        "all_employees_thousands": 2150.3,
        "avg_hourly_earnings": 36.50,
        "avg_weekly_earnings": 1460.0,
        "adjustment": "sa",
        "footnote_code": None,
        "naics_code": "522110",
        "match_precision": "exact",
        "employment_yoy_pct": 2.3,
        "earnings_yoy_pct": 3.7,
    }
    employment_latest_response = MagicMock()
    employment_latest_response.data = employment_latest_detail
    client.bls.employment_latest.return_value = employment_latest_response

    # --- SDK-14 mocks ---

    # county_employment() -> PaginatedResponse with .data as list of dicts
    county_employment_response = MagicMock()
    county_employment_response.data = [
        {
            "year": 2024,
            "quarter": 3,
            "month1_employment": 1200000,
            "month2_employment": 1210000,
            "month3_employment": 1205000,
            "employment_yoy_pct": 1.26,
        },
    ]
    client.bls.county_employment.return_value = county_employment_response

    # county_wages() -> DataResponse with .data as mock model
    county_wages_detail = MagicMock()
    county_wages_detail.model_dump.return_value = {
        "area_fips": "12086",
        "avg_weekly_wage": 1450,
        "wage_yoy_pct": 3.57,
        "location_quotient_wages": 0.98,
    }
    county_wages_response = MagicMock()
    county_wages_response.data = county_wages_detail
    client.bls.county_wages.return_value = county_wages_response

    # occupations() -> PaginatedResponse with .data as list of dicts
    occupations_response = MagicMock()
    occupations_response.data = [
        {"soc_code": "15-1252", "title": "Software Developers", "major_group": "15-0000"},
    ]
    client.bls.occupations.return_value = occupations_response

    # occupation() -> DataResponse with .data as mock model
    occupation_detail = MagicMock()
    occupation_detail.model_dump.return_value = {
        "soc_code": "15-1252",
        "title": "Software Developers",
        "major_group": "15-0000",
        "is_detailed": True,
        "data_availability": {"years": {"min": 2022, "max": 2023}, "geographies": ["national", "state", "metro"]},
        "related_occupations": [{"soc_code": "15-1253", "title": "Software QA Analysts"}],
    }
    occupation_response = MagicMock()
    occupation_response.data = occupation_detail
    client.bls.occupation.return_value = occupation_response

    # occupation_wages() -> PaginatedResponse with .data as list of dicts
    occupation_wages_response = MagicMock()
    occupation_wages_response.data = [
        {
            "area_name": "U.S.",
            "reference_year": 2023,
            "median_annual_wage": 206980,
            "mean_annual_wage": 239660,
            "employment": 200540,
        },
    ]
    client.bls.occupation_wages.return_value = occupation_wages_response

    # metrics() -> PaginatedResponse with .data as list of dicts
    metrics_response = MagicMock()
    metrics_response.data = [
        {
            "canonical_name": "total_employment",
            "display_name": "Total Employment",
            "category": "employment",
            "source_dataset": "ces",
        },
    ]
    client.bls.metrics.return_value = metrics_response

    # metric() -> DataResponse with .data as mock model
    metric_detail = MagicMock()
    metric_detail.model_dump.return_value = {
        "canonical_name": "total_employment",
        "display_name": "Total Employment",
        "description": "Total nonfarm employment",
        "category": "employment",
        "unit": "persons",
        "source_dataset": "ces",
        "data_availability": {"min": 2020, "max": 2025},
        "related_endpoints": ["/v1/us/bls/industries/{naics}/employment"],
    }
    metric_response = MagicMock()
    metric_response.data = metric_detail
    client.bls.metric.return_value = metric_response

    return client


def _invoke(runner: CliRunner, args: list[str], mock_client: MagicMock) -> object:
    """Invoke CLI with mocked ThesmaClient."""
    full_args = ["--api-key", "th_test_key", *args]
    with patch("thesma.client.ThesmaClient", return_value=mock_client):
        return runner.invoke(cli, full_args)


class TestBlsCli:
    def test_industries_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "industries"], mock_client)
        assert result.exit_code == 0
        assert "Finance and Insurance" in result.output

    def test_industry_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "industry", "522110"], mock_client)
        assert result.exit_code == 0
        assert "Commercial Banking" in result.output

    def test_employment_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "employment", "522110", "--from", "2024-01", "--to", "2024-12"], mock_client)
        assert result.exit_code == 0
        assert "2024-01" in result.output

    def test_employment_latest_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "employment-latest", "522110"], mock_client)
        assert result.exit_code == 0
        assert "2150.3" in result.output


class TestBlsCliSdk14:
    def test_county_employment_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "county-employment", "12086"], mock_client)
        assert result.exit_code == 0
        assert "1200000" in result.output

    def test_county_wages_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "county-wages", "12086"], mock_client)
        assert result.exit_code == 0
        assert "avg_weekly_wage" in result.output

    def test_occupations_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "occupations", "--search", "software"], mock_client)
        assert result.exit_code == 0
        assert "Software Developers" in result.output

    def test_occupation_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "occupation", "15-1252"], mock_client)
        assert result.exit_code == 0
        assert "Software Developers" in result.output

    def test_occupation_wages_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "occupation-wages", "11-1011", "--industry", "522110"], mock_client)
        assert result.exit_code == 0
        assert "206980" in result.output

    def test_metrics_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "metrics", "--category", "employment", "--source", "ces"], mock_client)
        assert result.exit_code == 0
        assert "total_employment" in result.output

    def test_metric_command(self, runner: CliRunner) -> None:
        mock_client = _make_bls_mock_client()
        result = _invoke(runner, ["bls", "metric", "total_employment:ces"], mock_client)
        assert result.exit_code == 0
        assert "Total Employment" in result.output
