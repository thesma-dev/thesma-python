"""Tests for the SBA CLI commands."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from thesma.cli.main import cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_sba_mock_client() -> MagicMock:
    """Create a mock ThesmaClient with every SBA method pre-stubbed."""
    client = MagicMock()

    county_lending_response = MagicMock()
    county_lending_response.data = [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "county_fips": "06037",
            "loan_count": 142,
            "total_amount": 38_500_000.0,
            "avg_amount": 271_127.0,
            "charge_off_rate": 2.11,
            "naics_match_level": None,
        }
    ]
    client.sba.county_lending.return_value = county_lending_response

    state_lending_response = MagicMock()
    state_lending_response.data = [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "state_fips": "06",
            "loan_count": 1420,
            "total_amount": 385_000_000.0,
            "avg_amount": 271_127.0,
            "charge_off_rate": 2.39,
        }
    ]
    client.sba.state_lending.return_value = state_lending_response

    industry_lending_response = MagicMock()
    industry_lending_response.data = [
        {
            "year": 2025,
            "quarter": 3,
            "period": "2025-Q3",
            "geo": "national",
            "state_fips": None,
            "county_fips": None,
            "loan_count": 920,
            "total_amount": 210_000_000.0,
            "charge_off_rate": 1.30,
        }
    ]
    client.sba.industry_lending.return_value = industry_lending_response

    lenders_response = MagicMock()
    lenders_response.data = [
        {
            "lender_id": 42,
            "display_name": "Live Oak Banking Co",
            "city": "Wilmington",
            "state": "NC",
            "loan_count": 521,
            "total_amount": 412_000_000.0,
            "avg_amount": 791_170.0,
            "market_share_pct": 5.4,
        }
    ]
    client.sba.lenders.return_value = lenders_response

    lender_detail = MagicMock()
    lender_detail.model_dump.return_value = {
        "lender_id": 42,
        "display_name": "Live Oak Banking Co",
        "city": "Wilmington",
        "state": "NC",
        "first_seen_at": "2010-01-15",
        "last_seen_at": "2025-09-30",
        "history": [{"year": 2025, "quarter": 3, "loan_count": 521}],
    }
    lender_response = MagicMock()
    lender_response.data = lender_detail
    client.sba.lender.return_value = lender_response

    characteristics_detail = MagicMock()
    characteristics_detail.model_dump.return_value = {
        "year": 2025,
        "quarter": 3,
        "period": "2025-Q3",
        "total_loans": 28341,
        "loan_size_buckets": [{"label": "<50K", "loan_count": 5824}],
    }
    characteristics_response = MagicMock()
    characteristics_response.data = characteristics_detail
    client.sba.lending_characteristics.return_value = characteristics_response

    outcomes_response = MagicMock()
    outcomes_response.data = [
        {
            "vintage_year": 2018,
            "loans_in_vintage": 14_087,
            "charged_off_count": 412,
            "charge_off_rate_pct": 2.92,
            "active_loan_count": 8421,
            "vintage_maturity": "mature",
        }
    ]
    client.sba.lending_outcomes.return_value = outcomes_response

    metrics_response = MagicMock()
    metrics_response.data = [
        {
            "canonical_name": "loan_count_4q",
            "display_name": "Trailing 4Q loan count",
            "category": "volume",
            "unit": "count",
            "update_cadence": "quarterly",
        }
    ]
    client.sba.metrics.return_value = metrics_response

    metric_detail = MagicMock()
    metric_detail.model_dump.return_value = {
        "canonical_name": "loan_count_4q",
        "display_name": "Trailing 4Q loan count",
        "description": "Total SBA 7(a) loans approved in the trailing four quarters.",
        "category": "volume",
        "unit": "count",
        "update_cadence": "quarterly",
    }
    metric_response = MagicMock()
    metric_response.data = metric_detail
    client.sba.metric.return_value = metric_response

    return client


def _invoke(runner: CliRunner, args: list[str], mock_client: MagicMock, fmt: str | None = None) -> object:
    """Invoke CLI with mocked ThesmaClient."""
    prefix = ["--api-key", "th_test_key"]
    if fmt is not None:
        prefix.extend(["--format", fmt])
    full_args = [*prefix, *args]
    with patch("thesma.client.ThesmaClient", return_value=mock_client):
        return runner.invoke(cli, full_args)


class TestSbaCountyLendingCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "county-lending", "06037"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.county_lending.call_args.args[0] == "06037"
        assert "06037" in result.output or "142" in result.output

    def test_with_filters(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(
            runner,
            [
                "sba",
                "county-lending",
                "06037",
                "--industry",
                "722511",
                "--from",
                "2024-Q1",
                "--to",
                "2025-Q3",
                "--page",
                "2",
                "--per-page",
                "50",
            ],
            mock_client,
        )
        assert result.exit_code == 0, result.output
        call = mock_client.sba.county_lending.call_args
        assert call.kwargs["industry"] == "722511"
        assert call.kwargs["from_period"] == "2024-Q1"
        assert call.kwargs["to_period"] == "2025-Q3"
        assert call.kwargs["page"] == 2
        assert call.kwargs["per_page"] == 50

    def test_json_format(self, runner: CliRunner) -> None:
        import json as _json

        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "county-lending", "06037"], mock_client, fmt="json")
        assert result.exit_code == 0, result.output
        parsed = _json.loads(result.output)
        assert isinstance(parsed, list)


class TestSbaStateLendingCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "state-lending", "06"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.state_lending.call_args.args[0] == "06"


class TestSbaIndustryLendingCommand:
    def test_geo_national_default_omits_kwarg(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "industry-lending", "541211"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.industry_lending.call_args.kwargs.get("geo") is None

    def test_geo_state_with_state_option(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(
            runner,
            ["sba", "industry-lending", "541211", "--geo", "state", "--state", "06"],
            mock_client,
        )
        assert result.exit_code == 0, result.output
        call = mock_client.sba.industry_lending.call_args
        assert call.kwargs["geo"] == "state"
        assert call.kwargs["state"] == "06"


class TestSbaLendersCommand:
    def test_default_sort(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "lenders"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.lenders.call_args.kwargs.get("sort") == "loan_count"

    def test_sort_total_amount(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "lenders", "--sort", "total_amount"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.lenders.call_args.kwargs["sort"] == "total_amount"


class TestSbaLenderCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "lender", "42"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.lender.call_args.args[0] == 42
        assert isinstance(mock_client.sba.lender.call_args.args[0], int)


class TestSbaLendingCharacteristicsCommand:
    def test_required_year_quarter(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(
            runner,
            ["sba", "lending-characteristics", "--year", "2025", "--quarter", "3"],
            mock_client,
        )
        assert result.exit_code == 0, result.output
        call = mock_client.sba.lending_characteristics.call_args
        assert call.kwargs["year"] == 2025
        assert call.kwargs["quarter"] == 3

    def test_missing_year_returns_nonzero(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "lending-characteristics", "--quarter", "3"], mock_client)
        assert result.exit_code != 0


class TestSbaLendingOutcomesCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "lending-outcomes", "--vintage-from", "2018"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.lending_outcomes.call_args.kwargs["vintage_from"] == 2018

    def test_with_range_and_filters(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(
            runner,
            [
                "sba",
                "lending-outcomes",
                "--vintage-from",
                "2015",
                "--vintage-to",
                "2020",
                "--state",
                "06",
                "--industry",
                "541211",
            ],
            mock_client,
        )
        assert result.exit_code == 0, result.output
        call = mock_client.sba.lending_outcomes.call_args
        assert call.kwargs["vintage_from"] == 2015
        assert call.kwargs["vintage_to"] == 2020
        assert call.kwargs["state"] == "06"
        assert call.kwargs["industry"] == "541211"


class TestSbaMetricsCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "metrics"], mock_client)
        assert result.exit_code == 0, result.output
        assert "loan_count_4q" in result.output

    def test_with_category(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "metrics", "--category", "volume"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.metrics.call_args.kwargs["category"] == "volume"


class TestSbaMetricCommand:
    def test_basic(self, runner: CliRunner) -> None:
        mock_client = _make_sba_mock_client()
        result = _invoke(runner, ["sba", "metric", "loan_count_4q"], mock_client)
        assert result.exit_code == 0, result.output
        assert mock_client.sba.metric.call_args.args[0] == "loan_count_4q"


class TestSbaGroupHelp:
    def test_sba_help_lists_all_commands(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["sba", "--help"])
        assert result.exit_code == 0
        for cmd in [
            "county-lending",
            "state-lending",
            "industry-lending",
            "lenders",
            "lender",
            "lending-characteristics",
            "lending-outcomes",
            "metrics",
            "metric",
        ]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"
