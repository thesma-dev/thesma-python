"""CLI commands for the screener resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

SCREENER_COLUMNS = ("ticker", "name", "company_tier", "exchange", "domicile", "fiscal_year")


@click.group("screener")
def screener_group() -> None:
    """Screen companies by financial thresholds."""


@screener_group.command("screen")
@click.option("--sic", multiple=True, help="Filter by SIC code(s). Repeat for multiple.")
@click.option("--tier", default=None, help="Filter by index tier.")
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
@click.option(
    "--search",
    default=None,
    help="Filter by company name (substring) or ticker (prefix), case-insensitive.",
)
@click.option("--min-gross-margin", default=None, type=float, help="Minimum gross margin (%).")
@click.option("--min-operating-margin", default=None, type=float, help="Minimum operating margin (%).")
@click.option("--min-net-margin", default=None, type=float, help="Minimum net margin (%).")
@click.option("--min-revenue", default=None, type=float, help="Minimum revenue.")
@click.option("--min-return-on-equity", default=None, type=float, help="Minimum return on equity (%).")
@click.option("--max-debt-to-equity", default=None, type=float, help="Maximum debt to equity ratio.")
@click.option("--max-net-income", default=None, type=float, help="Maximum net income (USD). Use 0 for loss-making.")
@click.option("--min-institutional-ownership-pct", default=None, type=float, help="Min institutional ownership (%%).")
@click.option("--has-insider-buying", default=None, type=bool, help="Filter for insider buying.")
@click.option(
    "--insider-buying-days",
    default=None,
    type=click.Choice(["7", "14", "30", "60", "90", "180", "365"]),
    help="Insider buying lookback days. Requires --has-insider-buying.",
)
@click.option("--has-institutional-increase", default=None, type=bool, help="Filter for institutional increase.")
@click.option(
    "--industry-hiring-trend",
    default=None,
    type=click.Choice(["accelerating", "decelerating", "stable", "declining"], case_sensitive=False),
    help="Filter by industry hiring trend.",
)
@click.option("--min-industry-employment-growth", default=None, type=float, help="Min industry employment growth %%.")
@click.option("--max-industry-employment-growth", default=None, type=float, help="Max industry employment growth %%.")
@click.option("--min-industry-wage-growth", default=None, type=float, help="Min industry wage growth %%.")
@click.option("--min-hq-county-wage-growth", default=None, type=float, help="Min HQ county wage growth %%.")
@click.option("--min-comp-to-market-ratio", default=None, type=float, help="Min CEO comp-to-market ratio.")
@click.option("--min-industry-quits-rate", default=None, type=float, help="Min industry quits rate (%%).")
@click.option("--max-industry-quits-rate", default=None, type=float, help="Max industry quits rate (%%).")
@click.option("--min-industry-openings-rate", default=None, type=float, help="Min industry job openings rate (%%).")
@click.option("--max-industry-openings-rate", default=None, type=float, help="Max industry job openings rate (%%).")
@click.option(
    "--min-local-unemployment-rate",
    default=None,
    type=float,
    help="Minimum HQ county unemployment rate (%%).",
)
@click.option(
    "--max-local-unemployment-rate",
    default=None,
    type=float,
    help="Maximum HQ county unemployment rate (%%).",
)
@click.option(
    "--local-unemployment-trend",
    default=None,
    type=click.Choice(["improving", "stable", "worsening"], case_sensitive=False),
    help="Filter by HQ county unemployment trend.",
)
@click.option(
    "--min-local-labor-force",
    default=None,
    type=int,
    help="Minimum HQ county labour force size.",
)
@click.option(
    "--min-local-sba-loan-count",
    default=None,
    type=int,
    help="Min trailing 4Q SBA loan count in HQ county.",
)
@click.option(
    "--max-local-sba-loan-count",
    default=None,
    type=int,
    help="Max trailing 4Q SBA loan count in HQ county.",
)
@click.option(
    "--min-local-sba-lending-growth",
    default=None,
    type=float,
    help="Min YoY %% change in HQ county SBA lending.",
)
@click.option(
    "--max-local-sba-lending-growth",
    default=None,
    type=float,
    help="Max YoY %% change in HQ county SBA lending.",
)
@click.option(
    "--min-industry-sba-lending-growth",
    default=None,
    type=float,
    help="Min YoY %% change in NAICS national SBA lending.",
)
@click.option(
    "--max-industry-sba-charge-off-rate",
    default=None,
    type=float,
    help="Max SBA charge-off rate (%%) for NAICS nationally.",
)
@click.option(
    "--include",
    default=None,
    help="Comma-separated enrichments (e.g. 'labor_context', 'lending_context', 'labor_context,lending_context').",
)
@click.option("--sort-by", default=None, help="Sort by field.")
@click.option("--order", default=None, help="Sort order (asc, desc).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def screener_screen(
    ctx: click.Context,
    sic: tuple[str, ...],
    tier: str | None,
    exchange: tuple[str, ...],
    domicile: str | None,
    search: str | None,
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
    industry_hiring_trend: str | None,
    min_industry_employment_growth: float | None,
    max_industry_employment_growth: float | None,
    min_industry_wage_growth: float | None,
    min_hq_county_wage_growth: float | None,
    min_comp_to_market_ratio: float | None,
    min_industry_quits_rate: float | None,
    max_industry_quits_rate: float | None,
    min_industry_openings_rate: float | None,
    max_industry_openings_rate: float | None,
    min_local_unemployment_rate: float | None,
    max_local_unemployment_rate: float | None,
    local_unemployment_trend: str | None,
    min_local_labor_force: int | None,
    min_local_sba_loan_count: int | None,
    max_local_sba_loan_count: int | None,
    min_local_sba_lending_growth: float | None,
    max_local_sba_lending_growth: float | None,
    min_industry_sba_lending_growth: float | None,
    max_industry_sba_charge_off_rate: float | None,
    include: str | None,
    sort_by: str | None,
    order: str | None,
    page: int,
    per_page: int,
) -> None:
    """Screen companies by financial ratio thresholds."""
    client = get_client(ctx)
    result = client.screener.screen(
        sic=sic if sic else None,
        tier=tier,
        exchange=list(exchange) if exchange else None,
        domicile=domicile,
        search=search,
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
        industry_hiring_trend=industry_hiring_trend,
        min_industry_employment_growth=min_industry_employment_growth,
        max_industry_employment_growth=max_industry_employment_growth,
        min_industry_wage_growth=min_industry_wage_growth,
        min_hq_county_wage_growth=min_hq_county_wage_growth,
        min_comp_to_market_ratio=min_comp_to_market_ratio,
        min_industry_quits_rate=min_industry_quits_rate,
        max_industry_quits_rate=max_industry_quits_rate,
        min_industry_openings_rate=min_industry_openings_rate,
        max_industry_openings_rate=max_industry_openings_rate,
        min_local_unemployment_rate=min_local_unemployment_rate,
        max_local_unemployment_rate=max_local_unemployment_rate,
        local_unemployment_trend=local_unemployment_trend,
        min_local_labor_force=min_local_labor_force,
        min_local_sba_loan_count=min_local_sba_loan_count,
        max_local_sba_loan_count=max_local_sba_loan_count,
        min_local_sba_lending_growth=min_local_sba_lending_growth,
        max_local_sba_lending_growth=max_local_sba_lending_growth,
        min_industry_sba_lending_growth=min_industry_sba_lending_growth,
        max_industry_sba_charge_off_rate=max_industry_sba_charge_off_rate,
        include=include,
        sort_by=sort_by,
        order=order,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], SCREENER_COLUMNS)
