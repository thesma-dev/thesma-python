"""Tests for the export CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
from click.testing import CliRunner

from thesma._export import ExportResult
from thesma.cli.main import cli

BASE = "https://api.thesma.dev"
API_KEY = "th_test_000000000000000000000000"

JSONL_COMPANIES = '{"cik":"320193","ticker":"AAPL","name":"Apple Inc"}\n{"__export_complete":true}\n'
JSONL_NO_SENTINEL = '{"cik":"320193","ticker":"AAPL","name":"Apple Inc"}\n'
CSV_COMPANIES = "cik,ticker,name\n320193,AAPL,Apple Inc\n"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestExportCli:
    @respx.mock
    def test_export_to_file(self, runner: CliRunner, tmp_path: Path) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        out_path = str(tmp_path / "out.jsonl")
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        assert result.exit_code == 0
        assert Path(out_path).exists()
        content = Path(out_path).read_text()
        assert "320193" in content

    @respx.mock
    def test_export_to_stdout_jsonl(self, runner: CliRunner) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies"])

        assert result.exit_code == 0
        assert "320193" in result.output
        assert "AAPL" in result.output
        # Sentinel should NOT appear in stdout
        assert "__export_complete" not in result.output

    @respx.mock
    def test_export_csv_to_stdout(self, runner: CliRunner) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=CSV_COMPANIES.encode()),
        )
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--format", "csv"])

        assert result.exit_code == 0
        assert "cik" in result.output
        assert "ticker" in result.output
        assert "320193" in result.output

    @respx.mock
    def test_export_incomplete_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_NO_SENTINEL.encode()),
        )
        out_path = str(tmp_path / "out.jsonl")
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        # CliRunner with mix_stderr=True (default) captures stderr in output
        assert "incomplete" in result.output.lower()

    def test_holdings_help_mentions_fund(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["export", "holdings", "--help"])
        assert result.exit_code == 0
        assert "fund" in result.output.lower()

    @respx.mock
    def test_403_shows_error(self, runner: CliRunner) -> None:
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(
                403,
                json={"detail": "Plan insufficient", "code": "plan_insufficient"},
            ),
        )
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies"])

        assert result.exit_code != 0

    def test_all_subcommands_registered(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["export", "--help"])
        assert result.exit_code == 0
        expected = [
            "companies",
            "financials",
            "insider-trades",
            "events",
            "ratios",
            "holdings",
            "compensation",
            "beneficial-ownership",
        ]
        for cmd in expected:
            assert cmd in result.output, f"Missing export subcommand: {cmd}"


class TestExportCliResume:
    def test_export_max_retries_option(self, runner: CliRunner, tmp_path: Path) -> None:
        """Invoke CLI with --max-retries 2 --output <path>. Patch the underlying export method
        to return ExportResult(complete=True, retries=1, ...). Assert exit code 0."""
        out_path = str(tmp_path / "out.jsonl")
        mock_result = ExportResult(path=out_path, rows=100, complete=True, format="jsonl", retries=1)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(
                cli, ["--api-key", API_KEY, "export", "companies", "--max-retries", "2", "--output", out_path]
            )

        assert result.exit_code == 0
        assert "complete" in result.output.lower()

    def test_export_max_retries_default(self, runner: CliRunner, tmp_path: Path) -> None:
        """Invoke CLI with --output and no --max-retries. Verify export method called with max_resume_retries=3."""
        out_path = str(tmp_path / "out.jsonl")
        mock_result = ExportResult(path=out_path, rows=50, complete=True, format="jsonl", retries=0)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        assert result.exit_code == 0
        mock_client.export.companies.assert_called_once()
        call_kwargs = mock_client.export.companies.call_args[1]
        assert call_kwargs["max_resume_retries"] == 3

    def test_export_incomplete_with_retries_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        """Patch export method to return ExportResult(complete=False, retries=3).
        Assert the warning message includes retry info."""
        out_path = str(tmp_path / "out.jsonl")
        mock_result = ExportResult(path=out_path, rows=50, complete=False, format="jsonl", retries=3)

        with patch("thesma.cli.commands.export.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.export.companies.return_value = mock_result

            result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--output", out_path])

        assert "3 retries" in result.output
        assert "4 total attempts" in result.output

    @respx.mock
    def test_export_stdout_mode_ignores_max_retries(self, runner: CliRunner) -> None:
        """Invoke CLI without --output but with --max-retries 5.
        Verify the command still uses ExportStream iteration (stdout mode unchanged)."""
        respx.get(f"{BASE}/v1/us/sec/export/companies").mock(
            return_value=httpx.Response(200, content=JSONL_COMPANIES.encode()),
        )
        result = runner.invoke(cli, ["--api-key", API_KEY, "export", "companies", "--max-retries", "5"])

        assert result.exit_code == 0
        assert "320193" in result.output
