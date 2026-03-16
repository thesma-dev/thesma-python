"""CLI commands for the screener resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

SCREENER_COLUMNS = ("ticker", "name", "company_tier", "fiscal_year")


@click.group("screener")
def screener_group() -> None:
    """Screen companies by financial thresholds."""


@screener_group.command("screen")
@click.option("--tier", default=None, help="Filter by index tier.")
@click.option("--min-gross-margin", default=None, type=float, help="Minimum gross margin (%).")
@click.option("--min-operating-margin", default=None, type=float, help="Minimum operating margin (%).")
@click.option("--min-net-margin", default=None, type=float, help="Minimum net margin (%).")
@click.option("--min-revenue", default=None, type=float, help="Minimum revenue.")
@click.option("--min-return-on-equity", default=None, type=float, help="Minimum return on equity (%).")
@click.option("--max-debt-to-equity", default=None, type=float, help="Maximum debt to equity ratio.")
@click.option("--max-net-income", default=None, type=float, help="Maximum net income (USD). Use 0 for loss-making.")
@click.option("--min-institutional-ownership-pct", default=None, type=float, help="Minimum institutional ownership (%%).")
@click.option("--has-insider-buying", default=None, type=bool, help="Filter for companies with recent insider buying.")
@click.option("--insider-buying-days", default=None, type=click.Choice(["7", "14", "30", "60", "90", "180", "365"]), help="Insider buying lookback days. Requires --has-insider-buying.")
@click.option("--has-institutional-increase", default=None, type=bool, help="Filter for companies with recent institutional ownership increase.")
@click.option("--sort-by", default=None, help="Sort by field.")
@click.option("--order", default=None, help="Sort order (asc, desc).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def screener_screen(
    ctx: click.Context,
    tier: str | None,
    min_gross_margin: float | None,
    min_operating_margin: float | None,
    min_net_margin: float | None,
    min_revenue: float | None,
    min_return_on_equity: float | None,
    max_debt_to_equity: float | None,
    max_net_income: float | None,
    min_institutional_ownership_pct: float | None,
    has_insider_buying: bool | None,
    insider_buying_days: str | None,
    has_institutional_increase: bool | None,
    sort_by: str | None,
    order: str | None,
    page: int,
    per_page: int,
) -> None:
    """Screen companies by financial ratio thresholds."""
    client = get_client(ctx)
    result = client.screener.screen(
        tier=tier,
        min_gross_margin=min_gross_margin,
        min_operating_margin=min_operating_margin,
        min_net_margin=min_net_margin,
        min_revenue=min_revenue,
        min_return_on_equity=min_return_on_equity,
        max_debt_to_equity=max_debt_to_equity,
        max_net_income=max_net_income,
        min_institutional_ownership_pct=min_institutional_ownership_pct,
        has_insider_buying=has_insider_buying,
        insider_buying_days=insider_buying_days,
        has_institutional_increase=has_institutional_increase,
        sort_by=sort_by,
        order=order,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], SCREENER_COLUMNS)
