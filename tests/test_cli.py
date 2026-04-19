"""Tests for the Thesma CLI."""

from __future__ import annotations

import csv
import io
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from thesma._version import __version__
from thesma.cli.main import cli
from thesma.errors import NotFoundError


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_mock_client() -> MagicMock:
    """Create a mock ThesmaClient with pre-configured responses.

    Uses plain dicts for data items so the formatters (_to_dict, format_json, etc.)
    can process them without needing real Pydantic model instances.
    """
    client = MagicMock()

    # companies.list() returns PaginatedResponse with .data as list of dicts
    companies_response = MagicMock()
    companies_response.data = [
        {
            "ticker": "AAPL",
            "cik": "0000320193",
            "name": "Apple Inc.",
            "company_tier": "sp500",
            "exchange": "NASDAQ",
            "domicile": "us",
        },
    ]
    client.companies.list.return_value = companies_response

    # companies.get() returns DataResponse with .data as a mock model
    # The companies get command uses result.data.model_dump(mode="json") for table/csv,
    # and for json format passes the whole result to format_json.
    company_detail = MagicMock()
    company_detail.model_dump.return_value = {
        "cik": "0000320193",
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "company_tier": "sp500",
        "exchange": "NASDAQ",
        "domicile": "us",
    }
    get_response = MagicMock()
    get_response.data = company_detail
    client.companies.get.return_value = get_response

    # census.compare() returns ComparisonResponse with .data.places as list of dicts
    compare_response = MagicMock()
    compare_response.data.places = [
        {"fips": "35620", "name": "New York-Newark-Jersey City", "value": 45000.0, "moe": 500.0},
        {"fips": "31080", "name": "Los Angeles-Long Beach-Anaheim", "value": 42000.0, "moe": 450.0},
    ]
    client.census.compare.return_value = compare_response

    # screener.screen() returns PaginatedResponse with .data as list of dicts
    screener_response = MagicMock()
    screener_response.data = [
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "company_tier": "sp500",
            "exchange": "NASDAQ",
            "domicile": "us",
            "fiscal_year": 2024,
        },
    ]
    client.screener.screen.return_value = screener_response

    return client


def _invoke(
    runner: CliRunner,
    args: list[str],
    mock_client: MagicMock | None = None,
    fmt: str | None = None,
) -> object:
    """Invoke CLI with optional mocked ThesmaClient and format.

    Group-level options (--api-key, --format) are placed before subcommand args.
    """
    prefix: list[str] = []
    if mock_client is not None:
        prefix.extend(["--api-key", "th_test_key"])
    if fmt is not None:
        prefix.extend(["--format", fmt])

    full_args = [*prefix, *args]

    if mock_client is not None:
        with patch("thesma.client.ThesmaClient", return_value=mock_client):
            return runner.invoke(cli, full_args)
    return runner.invoke(cli, full_args)


# --- Version and help ---


class TestVersionAndHelp:
    def test_version_flag(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output

    def test_help_shows_all_resource_groups(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        expected_groups = [
            "companies",
            "financials",
            "filings",
            "ratios",
            "screener",
            "insider-trades",
            "holdings",
            "events",
            "census",
        ]
        for group in expected_groups:
            assert group in result.output, f"Missing command group: {group}"


# --- Companies commands ---


class TestCompaniesListJson:
    def test_outputs_valid_json(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list", "--tier", "sp500"], mock_client, fmt="json")
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        assert len(parsed) == 1
        assert parsed[0]["ticker"] == "AAPL"

    def test_passes_tier_to_sdk(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        _invoke(runner, ["companies", "list", "--tier", "sp500"], mock_client)
        mock_client.companies.list.assert_called_once()
        call_kwargs = mock_client.companies.list.call_args
        assert call_kwargs.kwargs.get("tier") == "sp500" or call_kwargs[1].get("tier") == "sp500"

    def test_cli_companies_list_exchange(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["companies", "list", "--exchange", "nyse", "--exchange", "nasdaq"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.companies.list.call_args
        assert call_kwargs.kwargs.get("exchange") == ["nyse", "nasdaq"]

    def test_cli_companies_list_domicile(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list", "--domicile", "us"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.companies.list.call_args
        assert call_kwargs.kwargs.get("domicile") == "us"

    def test_cli_companies_list_case_insensitive_exchange(self, runner: CliRunner) -> None:
        """Click's ``Choice(case_sensitive=False)`` accepts any case and
        normalises the stored value to the canonical choice (lowercase here).
        The API also normalises, so the round-trip is lossless either way.
        """
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list", "--exchange", "NYSE"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.companies.list.call_args
        assert call_kwargs.kwargs.get("exchange") == ["nyse"]

    def test_cli_companies_list_invalid_exchange_rejected(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list", "--exchange", "amex"], mock_client)
        assert result.exit_code != 0
        mock_client.companies.list.assert_not_called()

    def test_cli_companies_list_help_shows_new_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["companies", "list", "--help"])
        assert result.exit_code == 0
        assert "--exchange" in result.output
        assert "--domicile" in result.output


class TestCompaniesListCsv:
    def test_outputs_valid_csv_with_headers(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list"], mock_client, fmt="csv")
        assert result.exit_code == 0
        reader = csv.DictReader(io.StringIO(result.output))
        assert reader.fieldnames is not None
        assert "ticker" in reader.fieldnames
        assert "cik" in reader.fieldnames
        assert "name" in reader.fieldnames
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"


class TestCompaniesListTable:
    def test_outputs_tabulated_output(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list"], mock_client, fmt="table")
        assert result.exit_code == 0
        assert "ticker" in result.output
        assert "AAPL" in result.output
        assert "Apple Inc." in result.output

    def test_cli_companies_list_default_columns_include_exchange_domicile(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "list"], mock_client, fmt="table")
        assert result.exit_code == 0
        assert "exchange" in result.output
        assert "domicile" in result.output
        assert "NASDAQ" in result.output
        assert "us" in result.output


class TestCompaniesGet:
    def test_calls_correct_sdk_method(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["companies", "get", "0000320193"], mock_client, fmt="json")
        assert result.exit_code == 0
        mock_client.companies.get.assert_called_once_with("0000320193", include=None)

    def test_cli_companies_get_include(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner, ["companies", "get", "0000320193", "--include", "labor_context"], mock_client, fmt="json"
        )
        assert result.exit_code == 0
        mock_client.companies.get.assert_called_once_with("0000320193", include="labor_context")


# --- Error handling ---


class TestMissingApiKey:
    def test_shows_error_not_traceback(self, runner: CliRunner) -> None:
        """Missing --api-key and no THESMA_API_KEY env var should show a clean error."""
        result = runner.invoke(cli, ["companies", "list"], env={"THESMA_API_KEY": ""})
        # Should not succeed
        assert result.exit_code != 0
        # Should not show a Python traceback
        assert "Traceback" not in result.output


class TestApiError:
    def test_404_shows_clean_error(self, runner: CliRunner) -> None:
        """API error (404) should show clean error message with non-zero exit code."""
        mock_client = MagicMock()
        mock_client.companies.get.side_effect = NotFoundError("Company not found", status_code=404)
        result = _invoke(runner, ["companies", "get", "9999999999"], mock_client)
        assert result.exit_code != 0


# --- Census compare ---


class TestCensusCompare:
    def test_passes_fips_list_correctly(self, runner: CliRunner) -> None:
        """thesma census compare median_income --fips 35620 --fips 31080 passes list to SDK."""
        mock_client = _make_mock_client()
        _invoke(
            runner,
            ["census", "compare", "median_income", "--fips", "35620", "--fips", "31080"],
            mock_client,
        )
        mock_client.census.compare.assert_called_once()
        call_kwargs = mock_client.census.compare.call_args
        # First positional arg is the metric
        assert call_kwargs.args[0] == "median_income"
        # fips should be a list
        fips_arg = call_kwargs.kwargs.get("fips") or call_kwargs[1].get("fips")
        assert fips_arg == ["35620", "31080"]


# --- Screener screen ---


class TestScreenerScreen:
    def test_screener_screen_passes_max_net_income(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--max-net-income", "0"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("max_net_income") == 0.0

    def test_screener_screen_passes_min_institutional_ownership_pct(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--min-institutional-ownership-pct", "50"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("min_institutional_ownership_pct") == 50.0

    def test_screener_screen_passes_insider_buying_days(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--has-insider-buying", "true", "--insider-buying-days", "30"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("has_insider_buying") is True
        assert call_kwargs.kwargs.get("insider_buying_days") == "30"

    def test_screener_screen_insider_buying_days_invalid_rejected(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--insider-buying-days", "42"], mock_client)
        assert result.exit_code != 0

    def test_screener_screen_has_institutional_increase(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--has-institutional-increase", "true"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("has_institutional_increase") is True

    def test_cli_screener_bls_options(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            [
                "screener",
                "screen",
                "--industry-hiring-trend",
                "stable",
                "--min-comp-to-market-ratio",
                "50",
            ],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("industry_hiring_trend") == "stable"
        assert call_kwargs.kwargs.get("min_comp_to_market_ratio") == 50.0

    def test_cli_screener_help_shows_bls_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        assert "--industry-hiring-trend" in result.output
        assert "--min-comp-to-market-ratio" in result.output
        assert "--min-hq-county-wage-growth" in result.output

    def test_cli_screener_min_local_unemployment_rate(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--min-local-unemployment-rate", "2.0"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("min_local_unemployment_rate") == 2.0

    def test_cli_screener_local_unemployment_trend(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--local-unemployment-trend", "improving"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("local_unemployment_trend") == "improving"

    def test_cli_screener_local_unemployment_trend_invalid_value(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--local-unemployment-trend", "foo"],
            mock_client,
        )
        assert result.exit_code != 0
        # Click's Choice rejects the value before any API call is made.
        assert mock_client.screener.screen.call_count == 0

    def test_cli_screener_min_local_labor_force_int(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--min-local-labor-force", "500000"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("min_local_labor_force") == 500000
        assert isinstance(call_kwargs.kwargs.get("min_local_labor_force"), int)

    def test_cli_screener_help_shows_laus_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        assert "--min-local-unemployment-rate" in result.output
        assert "--max-local-unemployment-rate" in result.output
        assert "--local-unemployment-trend" in result.output
        assert "--min-local-labor-force" in result.output

    def test_cli_screener_all_laus_options_combined(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            [
                "screener",
                "screen",
                "--min-local-unemployment-rate",
                "2.0",
                "--max-local-unemployment-rate",
                "4.0",
                "--local-unemployment-trend",
                "improving",
                "--min-local-labor-force",
                "500000",
                "--tier",
                "russell1000",
            ],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("min_local_unemployment_rate") == 2.0
        assert call_kwargs.kwargs.get("max_local_unemployment_rate") == 4.0
        assert call_kwargs.kwargs.get("local_unemployment_trend") == "improving"
        assert call_kwargs.kwargs.get("min_local_labor_force") == 500000
        assert call_kwargs.kwargs.get("tier") == "russell1000"

    def test_cli_screener_jolts_filters(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            [
                "screener",
                "screen",
                "--min-industry-quits-rate",
                "2.0",
                "--max-industry-quits-rate",
                "4.0",
                "--min-industry-openings-rate",
                "3.0",
                "--max-industry-openings-rate",
                "6.0",
            ],
            mock_client,
        )
        assert result.exit_code == 0
        assert "AAPL" in result.output
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("min_industry_quits_rate") == 2.0
        assert call_kwargs.kwargs.get("max_industry_quits_rate") == 4.0
        assert call_kwargs.kwargs.get("min_industry_openings_rate") == 3.0
        assert call_kwargs.kwargs.get("max_industry_openings_rate") == 6.0

    def test_cli_screener_help_shows_jolts_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        assert "--min-industry-quits-rate" in result.output
        assert "--max-industry-quits-rate" in result.output
        assert "--min-industry-openings-rate" in result.output
        assert "--max-industry-openings-rate" in result.output

    def test_cli_screener_screen_exchange(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--exchange", "nyse", "--exchange", "nasdaq"],
            mock_client,
        )
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("exchange") == ["nyse", "nasdaq"]

    def test_cli_screener_screen_case_insensitive_exchange(self, runner: CliRunner) -> None:
        """Click's ``Choice(case_sensitive=False)`` accepts any case and
        normalises to the canonical choice value (lowercase here).
        """
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--exchange", "NASDAQ"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("exchange") == ["nasdaq"]

    def test_cli_screener_screen_domicile(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--domicile", "us"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("domicile") == "us"

    def test_cli_screener_screen_invalid_domicile_rejected(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--domicile", "uk"], mock_client)
        assert result.exit_code != 0
        mock_client.screener.screen.assert_not_called()

    def test_cli_screener_screen_help_shows_new_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        assert "--exchange" in result.output
        assert "--domicile" in result.output

    def test_cli_screener_min_local_sba_loan_count(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--min-local-sba-loan-count", "100"], mock_client)
        assert result.exit_code == 0
        kwargs = mock_client.screener.screen.call_args.kwargs
        assert kwargs["min_local_sba_loan_count"] == 100
        assert isinstance(kwargs["min_local_sba_loan_count"], int)

    def test_cli_screener_max_local_sba_loan_count(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--max-local-sba-loan-count", "1000"], mock_client)
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["max_local_sba_loan_count"] == 1000

    def test_cli_screener_min_local_sba_lending_growth(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--min-local-sba-lending-growth", "5.0"], mock_client)
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["min_local_sba_lending_growth"] == 5.0

    def test_cli_screener_max_local_sba_lending_growth(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--max-local-sba-lending-growth", "20.0"], mock_client)
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["max_local_sba_lending_growth"] == 20.0

    def test_cli_screener_min_industry_sba_lending_growth(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--min-industry-sba-lending-growth", "3.5"],
            mock_client,
        )
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["min_industry_sba_lending_growth"] == 3.5

    def test_cli_screener_max_industry_sba_charge_off_rate(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--max-industry-sba-charge-off-rate", "10.0"],
            mock_client,
        )
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["max_industry_sba_charge_off_rate"] == 10.0

    def test_cli_screener_combined_sba_options(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            [
                "screener",
                "screen",
                "--min-local-sba-loan-count",
                "100",
                "--max-local-sba-loan-count",
                "1000",
                "--min-local-sba-lending-growth",
                "5.0",
                "--max-local-sba-lending-growth",
                "20.0",
                "--min-industry-sba-lending-growth",
                "3.5",
                "--max-industry-sba-charge-off-rate",
                "10.0",
            ],
            mock_client,
        )
        assert result.exit_code == 0
        kwargs = mock_client.screener.screen.call_args.kwargs
        assert kwargs["min_local_sba_loan_count"] == 100
        assert kwargs["max_local_sba_loan_count"] == 1000
        assert kwargs["min_local_sba_lending_growth"] == 5.0
        assert kwargs["max_local_sba_lending_growth"] == 20.0
        assert kwargs["min_industry_sba_lending_growth"] == 3.5
        assert kwargs["max_industry_sba_charge_off_rate"] == 10.0

    def test_cli_screener_help_shows_sba_options(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        for option in (
            "--min-local-sba-loan-count",
            "--max-local-sba-loan-count",
            "--min-local-sba-lending-growth",
            "--max-local-sba-lending-growth",
            "--min-industry-sba-lending-growth",
            "--max-industry-sba-charge-off-rate",
            "--include",
        ):
            assert option in result.output, f"Missing option: {option}"

    def test_cli_screener_include_lending_context_option(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--include", "lending_context"], mock_client)
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["include"] == "lending_context"

    def test_cli_screener_include_combined_option(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(
            runner,
            ["screener", "screen", "--include", "labor_context,lending_context"],
            mock_client,
        )
        assert result.exit_code == 0
        assert mock_client.screener.screen.call_args.kwargs["include"] == "labor_context,lending_context"

    def test_screener_screen_passes_search(self, runner: CliRunner) -> None:
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen", "--search", "AAPL"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("search") == "AAPL"

    def test_cli_screener_help_shows_search_option(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["screener", "screen", "--help"])
        assert result.exit_code == 0
        assert "--search" in result.output

    def test_cli_screener_search_default_is_none(self, runner: CliRunner) -> None:
        """No ``--search`` flag must forward ``None``, not an empty string."""
        mock_client = _make_mock_client()
        result = _invoke(runner, ["screener", "screen"], mock_client)
        assert result.exit_code == 0
        call_kwargs = mock_client.screener.screen.call_args
        assert call_kwargs.kwargs.get("search") is None


# --- Holdings CLI ---


class TestHoldingsCli:
    def test_holdings_funds_search_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.holdings.funds.return_value = response
        result = _invoke(runner, ["holdings", "funds", "--search", "Vanguard"], mock_client)
        assert result.exit_code == 0
        mock_client.holdings.funds.assert_called_once()
        call_kwargs = mock_client.holdings.funds.call_args
        assert call_kwargs.kwargs.get("search") == "Vanguard"


# --- Insider trades CLI ---


class TestInsiderTradesCli:
    def test_insider_trades_list_from_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.insider_trades.list.return_value = response
        result = _invoke(runner, ["insider-trades", "list", "0000320193", "--from", "2024-01-01"], mock_client)
        assert result.exit_code == 0
        mock_client.insider_trades.list.assert_called_once()
        call_kwargs = mock_client.insider_trades.list.call_args
        assert call_kwargs.kwargs.get("from_date") == "2024-01-01"

    def test_insider_trades_list_to_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.insider_trades.list.return_value = response
        result = _invoke(runner, ["insider-trades", "list", "0000320193", "--to", "2024-12-31"], mock_client)
        assert result.exit_code == 0
        mock_client.insider_trades.list.assert_called_once()
        call_kwargs = mock_client.insider_trades.list.call_args
        assert call_kwargs.kwargs.get("to_date") == "2024-12-31"

    def test_insider_trades_list_person_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.insider_trades.list.return_value = response
        result = _invoke(runner, ["insider-trades", "list", "0000320193", "--person", "Jane"], mock_client)
        assert result.exit_code == 0
        mock_client.insider_trades.list.assert_called_once()
        call_kwargs = mock_client.insider_trades.list.call_args
        assert call_kwargs.kwargs.get("person") == "Jane"


# --- Events CLI ---


class TestEventsCli:
    def test_events_list_from_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.events.list.return_value = response
        result = _invoke(runner, ["events", "list", "0000320193", "--from", "2024-01-01"], mock_client)
        assert result.exit_code == 0
        mock_client.events.list.assert_called_once()
        call_kwargs = mock_client.events.list.call_args
        assert call_kwargs.kwargs.get("from_date") == "2024-01-01"

    def test_events_list_to_flag(self, runner: CliRunner) -> None:
        mock_client = MagicMock()
        response = MagicMock()
        response.data = []
        mock_client.events.list.return_value = response
        result = _invoke(runner, ["events", "list", "0000320193", "--to", "2024-12-31"], mock_client)
        assert result.exit_code == 0
        mock_client.events.list.assert_called_once()
        call_kwargs = mock_client.events.list.call_args
        assert call_kwargs.kwargs.get("to_date") == "2024-12-31"
