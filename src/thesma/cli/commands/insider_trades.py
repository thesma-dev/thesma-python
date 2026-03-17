"""CLI commands for the insider-trades resource."""

from __future__ import annotations

from typing import Any

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

INSIDER_TRADE_COLUMNS = ("transaction_date", "person_name", "type", "shares", "total_value")


@click.group("insider-trades")
def insider_trades_group() -> None:
    """SEC Form 4 insider trade data."""


@insider_trades_group.command("list")
@click.argument("cik")
@click.option("--from", "from_date", default=None, help="Filter trades on or after this date (YYYY-MM-DD).")
@click.option("--person", default=None, help="Filter by person name (partial match).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def insider_trades_list(
    ctx: click.Context, cik: str, from_date: str | None, person: str | None, page: int, per_page: int
) -> None:
    """List insider trades for a company."""
    client = get_client(ctx)
    result = client.insider_trades.list(cik, from_date=from_date, person=person, page=page, per_page=per_page)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result.data, fmt, INSIDER_TRADE_COLUMNS)
    else:
        rows: list[dict[str, Any]] = []
        for item in result.data:
            d = item.model_dump(mode="json")
            d["person_name"] = d.get("person", {}).get("name", "")
            rows.append(d)
        output(rows, fmt, INSIDER_TRADE_COLUMNS)
