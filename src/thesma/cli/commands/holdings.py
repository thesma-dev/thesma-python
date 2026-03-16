"""CLI commands for the holdings resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

HOLDER_COLUMNS = ("fund_name", "shares", "market_value")
FUND_COLUMNS = ("cik", "name", "holdings_url")


@click.group("holdings")
def holdings_group() -> None:
    """Institutional holdings and fund data."""


@holdings_group.command("holders")
@click.argument("cik")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def holdings_holders(ctx: click.Context, cik: str, page: int, per_page: int) -> None:
    """List institutional holders for a company."""
    client = get_client(ctx)
    result = client.holdings.holders(cik, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], HOLDER_COLUMNS)


@holdings_group.command("funds")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def holdings_funds(ctx: click.Context, page: int, per_page: int) -> None:
    """List institutional funds."""
    client = get_client(ctx)
    result = client.holdings.funds(page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], FUND_COLUMNS)
