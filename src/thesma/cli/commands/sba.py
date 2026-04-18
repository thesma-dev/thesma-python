"""CLI commands for the SBA resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

COUNTY_LENDING_COLUMNS = (
    "year",
    "quarter",
    "period",
    "loan_count",
    "total_amount",
    "avg_amount",
    "charge_off_rate",
    "naics_match_level",
)
STATE_LENDING_COLUMNS = (
    "year",
    "quarter",
    "period",
    "loan_count",
    "total_amount",
    "avg_amount",
    "charge_off_rate",
)
INDUSTRY_LENDING_COLUMNS = (
    "year",
    "quarter",
    "period",
    "geo",
    "state_fips",
    "county_fips",
    "loan_count",
    "total_amount",
    "charge_off_rate",
)
LENDER_LIST_COLUMNS = (
    "lender_id",
    "display_name",
    "city",
    "state",
    "loan_count",
    "total_amount",
    "avg_amount",
    "market_share_pct",
)
VINTAGE_OUTCOME_COLUMNS = (
    "vintage_year",
    "loans_in_vintage",
    "charged_off_count",
    "charge_off_rate_pct",
    "active_loan_count",
    "vintage_maturity",
)
METRIC_LIST_COLUMNS = (
    "canonical_name",
    "display_name",
    "category",
    "unit",
    "update_cadence",
)


@click.group("sba")
def sba_group() -> None:
    """US Small Business Administration 7(a) loan data."""


@sba_group.command("county-lending")
@click.argument("fips")
@click.option("--industry", default=None, help="NAICS code (2-6 digits).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.option("--from", "from_period", default=None, help="Start period (YYYY-Qq).")
@click.option("--to", "to_period", default=None, help="End period (YYYY-Qq).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_county_lending(
    ctx: click.Context,
    fips: str,
    industry: str | None,
    year: int | None,
    quarter: int | None,
    from_period: str | None,
    to_period: str | None,
    page: int,
    per_page: int,
) -> None:
    """Get quarterly SBA 7(a) lending aggregates for a county."""
    client = get_client(ctx)
    result = client.sba.county_lending(
        fips,
        industry=industry,
        year=year,
        quarter=quarter,
        from_period=from_period,
        to_period=to_period,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], COUNTY_LENDING_COLUMNS)


@sba_group.command("state-lending")
@click.argument("fips")
@click.option("--industry", default=None, help="NAICS code (2-6 digits).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.option("--from", "from_period", default=None, help="Start period (YYYY-Qq).")
@click.option("--to", "to_period", default=None, help="End period (YYYY-Qq).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_state_lending(
    ctx: click.Context,
    fips: str,
    industry: str | None,
    year: int | None,
    quarter: int | None,
    from_period: str | None,
    to_period: str | None,
    page: int,
    per_page: int,
) -> None:
    """Get quarterly SBA 7(a) lending aggregates for a state."""
    client = get_client(ctx)
    result = client.sba.state_lending(
        fips,
        industry=industry,
        year=year,
        quarter=quarter,
        from_period=from_period,
        to_period=to_period,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], STATE_LENDING_COLUMNS)


@sba_group.command("industry-lending")
@click.argument("naics")
@click.option(
    "--geo",
    default=None,
    type=click.Choice(["national", "state", "county"], case_sensitive=False),
    help="Geography level (defaults to national on the API side).",
)
@click.option("--state", default=None, help="2-digit FIPS (required when --geo=state).")
@click.option("--county", default=None, help="5-digit FIPS (required when --geo=county).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.option("--from", "from_period", default=None, help="Start period (YYYY-Qq).")
@click.option("--to", "to_period", default=None, help="End period (YYYY-Qq).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_industry_lending(
    ctx: click.Context,
    naics: str,
    geo: str | None,
    state: str | None,
    county: str | None,
    year: int | None,
    quarter: int | None,
    from_period: str | None,
    to_period: str | None,
    page: int,
    per_page: int,
) -> None:
    """Get quarterly SBA 7(a) lending aggregates for a NAICS industry."""
    client = get_client(ctx)
    result = client.sba.industry_lending(
        naics,
        geo=geo,
        state=state,
        county=county,
        year=year,
        quarter=quarter,
        from_period=from_period,
        to_period=to_period,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], INDUSTRY_LENDING_COLUMNS)


@sba_group.command("lenders")
@click.option("--state", default=None, help="2-digit FIPS to filter by.")
@click.option("--county", default=None, help="5-digit FIPS to filter by.")
@click.option("--industry", default=None, help="NAICS code (2-6 digits).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.option("--from", "from_period", default=None, help="Start period (YYYY-Qq).")
@click.option("--to", "to_period", default=None, help="End period (YYYY-Qq).")
@click.option(
    "--sort",
    default="loan_count",
    type=click.Choice(["loan_count", "total_amount", "avg_amount"], case_sensitive=False),
    help="Sort field.",
)
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_lenders(
    ctx: click.Context,
    state: str | None,
    county: str | None,
    industry: str | None,
    year: int | None,
    quarter: int | None,
    from_period: str | None,
    to_period: str | None,
    sort: str,
    page: int,
    per_page: int,
) -> None:
    """List SBA 7(a) lenders ranked by activity."""
    client = get_client(ctx)
    result = client.sba.lenders(
        state=state,
        county=county,
        industry=industry,
        year=year,
        quarter=quarter,
        from_period=from_period,
        to_period=to_period,
        sort=sort,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], LENDER_LIST_COLUMNS)


@sba_group.command("lender")
@click.argument("lender_id", type=int)
@click.option("--from", "from_period", default=None, help="Start period (YYYY-Qq).")
@click.option("--to", "to_period", default=None, help="End period (YYYY-Qq).")
@click.pass_context
def sba_lender(
    ctx: click.Context,
    lender_id: int,
    from_period: str | None,
    to_period: str | None,
) -> None:
    """Get detail and quarterly history for a single SBA lender."""
    client = get_client(ctx)
    result = client.sba.lender(lender_id, from_period=from_period, to_period=to_period)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))


@sba_group.command("lending-characteristics")
@click.option("--year", required=True, type=int, help="Data year (required).")
@click.option("--quarter", required=True, type=int, help="Quarter 1-4 (required).")
@click.option("--state", default=None, help="2-digit FIPS.")
@click.option("--county", default=None, help="5-digit FIPS.")
@click.option("--industry", default=None, help="NAICS code.")
@click.pass_context
def sba_lending_characteristics(
    ctx: click.Context,
    year: int,
    quarter: int,
    state: str | None,
    county: str | None,
    industry: str | None,
) -> None:
    """Get distributional breakdown of SBA 7(a) loans for one quarter."""
    client = get_client(ctx)
    result = client.sba.lending_characteristics(
        year=year,
        quarter=quarter,
        state=state,
        county=county,
        industry=industry,
    )
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))


@sba_group.command("lending-outcomes")
@click.option("--vintage-from", required=True, type=int, help="Earliest vintage year (required).")
@click.option("--vintage-to", default=None, type=int, help="Latest vintage year.")
@click.option("--state", default=None, help="2-digit FIPS.")
@click.option("--county", default=None, help="5-digit FIPS.")
@click.option("--industry", default=None, help="NAICS code.")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_lending_outcomes(
    ctx: click.Context,
    vintage_from: int,
    vintage_to: int | None,
    state: str | None,
    county: str | None,
    industry: str | None,
    page: int,
    per_page: int,
) -> None:
    """Get vintage-level charge-off outcomes for SBA 7(a) loans."""
    client = get_client(ctx)
    result = client.sba.lending_outcomes(
        vintage_from=vintage_from,
        vintage_to=vintage_to,
        state=state,
        county=county,
        industry=industry,
        page=page,
        per_page=per_page,
    )
    output(result.data, ctx.obj["format"], VINTAGE_OUTCOME_COLUMNS)


@sba_group.command("metrics")
@click.option(
    "--category",
    default=None,
    type=click.Choice(["volume", "outcomes", "characteristics"], case_sensitive=False),
    help="Filter by metric category.",
)
@click.option("--search", default=None, help="Search metric name/description (min 2 chars).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def sba_metrics(
    ctx: click.Context,
    category: str | None,
    search: str | None,
    page: int,
    per_page: int,
) -> None:
    """List SBA metric definitions."""
    client = get_client(ctx)
    result = client.sba.metrics(category=category, search=search, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], METRIC_LIST_COLUMNS)


@sba_group.command("metric")
@click.argument("metric")
@click.pass_context
def sba_metric(ctx: click.Context, metric: str) -> None:
    """Get detail for a single SBA metric by canonical name."""
    client = get_client(ctx)
    result = client.sba.metric(metric)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))
