"""CLI commands for the ratios resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client


@click.group("ratios")
def ratios_group() -> None:
    """Financial ratios for SEC companies."""


@ratios_group.command("get")
@click.argument("cik")
@click.option("--period", default=None, help="Period type (annual, quarterly).")
@click.option("--year", default=None, type=int, help="Fiscal year.")
@click.pass_context
def ratios_get(ctx: click.Context, cik: str, period: str | None, year: int | None) -> None:
    """Get financial ratios for a company."""
    client = get_client(ctx)
    result = client.ratios.get(cik, period=period, year=year)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        ratios_data = result.data.ratios.model_dump(mode="json")
        rows = [{"ratio": k, "value": v} for k, v in ratios_data.items()]
        output(rows, fmt, ("ratio", "value"))
