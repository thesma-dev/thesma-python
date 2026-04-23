"""CLI commands for the financials resource."""

from __future__ import annotations

from typing import Any

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client


@click.group("financials")
def financials_group() -> None:
    """Financial statements and field reference."""


@financials_group.command("get")
@click.argument("cik")
@click.option(
    "--statement",
    default=None,
    help="Statement type (income, balance-sheet, cash-flow, all). Use 'all' for all three in one call.",
)
@click.option("--period", default=None, help="Period type (annual, quarterly).")
@click.option("--year", default=None, type=int, help="Fiscal year.")
@click.option(
    "--per-page",
    "per_page",
    default=None,
    type=int,
    help="Return N most-recent statements as a list (1-20). Mutually exclusive with --year/--quarter.",
)
@click.pass_context
def financials_get(
    ctx: click.Context,
    cik: str,
    statement: str | None,
    period: str | None,
    year: int | None,
    per_page: int | None,
) -> None:
    """Get financial statements for a company."""
    client = get_client(ctx)
    result = client.financials.get(cik, statement=statement, period=period, year=year, per_page=per_page)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    elif per_page is not None or statement == "all":
        # Paginated / multi-statement shapes — dump JSON for table-mode users too; the
        # legacy single-statement line-items table only applies to the single-statement shape.
        output(result, "json", ())
    else:
        line_items = result.data.line_items
        rows = [{"label": k, "value": v, "unit": result.data.currency} for k, v in line_items.items()]
        output(rows, fmt, ("label", "value", "unit"))


@financials_group.command("fields")
@click.pass_context
def financials_fields(ctx: click.Context) -> None:
    """List canonical financial statement fields."""
    client = get_client(ctx)
    result = client.financials.fields()
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        rows: list[dict[str, Any]] = []
        for stmt_name in ("income", "balance_sheet", "cash_flow"):
            stmt_fields = getattr(result.data, stmt_name)
            for field in stmt_fields.fields:
                rows.append({"statement": stmt_name, "name": field.name, "description": field.description})
        output(rows, fmt, ("statement", "name", "description"))
