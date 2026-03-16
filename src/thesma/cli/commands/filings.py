"""CLI commands for the filings resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

FILING_LIST_COLUMNS = ("accession_number", "filing_type", "filed_at", "period_of_report")


@click.group("filings")
def filings_group() -> None:
    """List and look up SEC filings."""


@filings_group.command("list")
@click.argument("cik")
@click.option("--type", "filing_type", default=None, help="Filter by filing type (10-K, 10-Q, 8-K, etc.).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def filings_list(ctx: click.Context, cik: str, filing_type: str | None, page: int, per_page: int) -> None:
    """List filings for a company."""
    client = get_client(ctx)
    result = client.filings.list(cik, filing_type=filing_type, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], FILING_LIST_COLUMNS)


@filings_group.command("get")
@click.argument("accession_number")
@click.pass_context
def filings_get(ctx: click.Context, accession_number: str) -> None:
    """Get a single filing by accession number."""
    client = get_client(ctx)
    result = client.filings.get(accession_number)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))
