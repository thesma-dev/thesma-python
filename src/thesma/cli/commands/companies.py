"""CLI commands for the companies resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

COMPANY_LIST_COLUMNS = ("ticker", "cik", "name", "company_tier")


@click.group("companies")
def companies_group() -> None:
    """List and look up SEC-registered companies."""


@companies_group.command("list")
@click.option("--ticker", default=None, help="Filter by ticker symbol.")
@click.option("--search", default=None, help="Search by company name.")
@click.option("--tier", default=None, help="Filter by index tier (sp500, russell1000, etc.).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def companies_list(
    ctx: click.Context, ticker: str | None, search: str | None, tier: str | None, page: int, per_page: int
) -> None:
    """List companies with optional filters."""
    client = get_client(ctx)
    result = client.companies.list(ticker=ticker, search=search, tier=tier, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], COMPANY_LIST_COLUMNS)


@companies_group.command("get")
@click.argument("cik")
@click.pass_context
def companies_get(ctx: click.Context, cik: str) -> None:
    """Get a single company by CIK."""
    client = get_client(ctx)
    result = client.companies.get(cik)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))
