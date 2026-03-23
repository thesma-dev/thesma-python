"""Thesma CLI entry point."""

from __future__ import annotations

import signal
import sys

import click

from thesma._version import __version__
from thesma.cli.commands.census import census_group
from thesma.cli.commands.companies import companies_group
from thesma.cli.commands.events import events_group
from thesma.cli.commands.export import export_group
from thesma.cli.commands.filings import filings_group
from thesma.cli.commands.financials import financials_group
from thesma.cli.commands.holdings import holdings_group
from thesma.cli.commands.insider_trades import insider_trades_group
from thesma.cli.commands.ratios import ratios_group
from thesma.cli.commands.screener import screener_group
from thesma.errors import ThesmaError

# Handle SIGPIPE cleanly for piped output (e.g. thesma companies list | head -5)
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


@click.group()
@click.version_option(version=__version__, prog_name="thesma")
@click.option("--api-key", envvar="THESMA_API_KEY", default=None, help="Thesma API key (or set THESMA_API_KEY).")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "csv"], case_sensitive=False),
    default="table",
    help="Output format.",
)
@click.option("--base-url", default="https://api.thesma.dev", help="API base URL.")
@click.pass_context
def cli(ctx: click.Context, api_key: str | None, output_format: str, base_url: str) -> None:
    """Thesma CLI — access SEC EDGAR data from your terminal."""
    ctx.ensure_object(dict)
    ctx.obj["format"] = output_format
    ctx.obj["api_key"] = api_key
    ctx.obj["base_url"] = base_url

    # Lazy client creation: only create when a subcommand actually runs
    if api_key is not None:
        from thesma.client import ThesmaClient

        ctx.obj["client"] = ThesmaClient(api_key=api_key, base_url=base_url)


# Register all command groups
cli.add_command(companies_group)
cli.add_command(financials_group)
cli.add_command(filings_group)
cli.add_command(ratios_group)
cli.add_command(screener_group)
cli.add_command(insider_trades_group)
cli.add_command(holdings_group)
cli.add_command(events_group)
cli.add_command(census_group)
cli.add_command(export_group)


def main() -> None:
    """CLI entry point with error handling."""
    try:
        cli(standalone_mode=False)
    except click.exceptions.Abort:
        pass
    except click.ClickException as exc:
        exc.show()
        sys.exit(exc.exit_code)
    except ThesmaError as exc:
        click.echo(f"Error: {exc.message}", err=True)
        sys.exit(1)
    except BrokenPipeError:
        # Clean exit when piped to head, etc.
        sys.stderr.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
