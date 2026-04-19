"""CLI commands for the companies resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

COMPANY_LIST_COLUMNS = ("ticker", "cik", "name", "company_tier", "exchange", "domicile")


@click.group("companies")
def companies_group() -> None:
    """List and look up SEC-registered companies."""


@companies_group.command("list")
@click.option("--ticker", default=None, help="Filter by ticker symbol.")
@click.option("--search", default=None, help="Filter by company name (substring) or ticker (prefix), case-insensitive.")
@click.option("--sic", multiple=True, help="Filter by SIC code(s). Repeat for multiple.")
@click.option("--tier", default=None, help="Filter by index tier (sp500, russell1000, etc.).")
@click.option(
    "--exchange",
    multiple=True,
    type=click.Choice(["nyse", "nasdaq"], case_sensitive=False),
    help="Filter by stock exchange (nyse, nasdaq). Repeat for multiple.",
)
@click.option(
    "--domicile",
    default=None,
    type=click.Choice(["us", "adr"], case_sensitive=False),
    help="Filter by company domicile (us or adr).",
)
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def companies_list(
    ctx: click.Context,
    ticker: str | None,
    search: str | None,
    sic: tuple[str, ...],
    tier: str | None,
    exchange: tuple[str, ...],
    domicile: str | None,
    page: int,
    per_page: int,
) -> None:
    """List companies with optional filters."""
    client = get_client(ctx)
    result = client.companies.list(
        ticker=ticker,
        search=search,
        sic=sic if sic else None,
        tier=tier,
        exchange=list(exchange) if exchange else None,
        domicile=domicile,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], COMPANY_LIST_COLUMNS)


@companies_group.command("get")
@click.argument("cik")
@click.option("--include", default=None, help="Include enrichment (e.g. labor_context).")
@click.pass_context
def companies_get(ctx: click.Context, cik: str, include: str | None) -> None:
    """Get a single company by CIK."""
    client = get_client(ctx)
    result = client.companies.get(cik, include=include)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))
