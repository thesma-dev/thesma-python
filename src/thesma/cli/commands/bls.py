"""CLI commands for the BLS resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

INDUSTRY_LIST_COLUMNS = ("naics_code", "title", "level")
EMPLOYMENT_COLUMNS = ("period", "all_employees_thousands", "avg_hourly_earnings", "avg_weekly_earnings")
COUNTY_EMPLOYMENT_COLUMNS = (
    "year",
    "quarter",
    "month1_employment",
    "month2_employment",
    "month3_employment",
    "employment_yoy_pct",
)
OCCUPATION_LIST_COLUMNS = ("soc_code", "title", "major_group")
OCCUPATION_WAGES_COLUMNS = ("area_name", "reference_year", "median_annual_wage", "mean_annual_wage", "employment")
METRIC_LIST_COLUMNS = ("canonical_name", "display_name", "category", "source_dataset")


@click.group("bls")
def bls_group() -> None:
    """US Bureau of Labor Statistics labor market data."""


@bls_group.command("industries")
@click.option("--level", default=None, type=int, help="Filter by NAICS digit count (1-6).")
@click.option("--search", default=None, help="Case-insensitive title search (min 2 chars).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def bls_industries(ctx: click.Context, level: int | None, search: str | None, page: int, per_page: int) -> None:
    """List or search NAICS industries."""
    client = get_client(ctx)
    result = client.bls.industries(level=level, search=search, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], INDUSTRY_LIST_COLUMNS)


@bls_group.command("industry")
@click.argument("naics")
@click.pass_context
def bls_industry(ctx: click.Context, naics: str) -> None:
    """Get industry detail by NAICS code."""
    client = get_client(ctx)
    result = client.bls.industry(naics)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))


@bls_group.command("employment")
@click.argument("naics")
@click.option("--from", "from_date", default=None, help="Start date (YYYY-MM).")
@click.option("--to", "to_date", default=None, help="End date (YYYY-MM).")
@click.option(
    "--adjustment", default="sa", type=click.Choice(["sa", "nsa"], case_sensitive=False), help="Seasonal adjustment."
)
@click.option(
    "--geo",
    default="national",
    type=click.Choice(["national", "state", "metro"], case_sensitive=False),
    help="Geography level.",
)
@click.option("--state", default=None, help="2-digit FIPS code (required when geo=state).")
@click.option("--metro", default=None, help="5-digit CBSA code (required when geo=metro).")
@click.pass_context
def bls_employment(
    ctx: click.Context,
    naics: str,
    from_date: str | None,
    to_date: str | None,
    adjustment: str,
    geo: str,
    state: str | None,
    metro: str | None,
) -> None:
    """Get CES employment time series for an industry."""
    client = get_client(ctx)
    result = client.bls.employment(
        naics, from_date=from_date, to_date=to_date, adjustment=adjustment, geo=geo, state=state, metro=metro
    )
    output(result.data, ctx.obj["format"], EMPLOYMENT_COLUMNS)


@bls_group.command("employment-latest")
@click.argument("naics")
@click.option(
    "--adjustment", default="sa", type=click.Choice(["sa", "nsa"], case_sensitive=False), help="Seasonal adjustment."
)
@click.option(
    "--geo",
    default="national",
    type=click.Choice(["national", "state", "metro"], case_sensitive=False),
    help="Geography level.",
)
@click.option("--state", default=None, help="2-digit FIPS code (required when geo=state).")
@click.option("--metro", default=None, help="5-digit CBSA code (required when geo=metro).")
@click.pass_context
def bls_employment_latest(
    ctx: click.Context,
    naics: str,
    adjustment: str,
    geo: str,
    state: str | None,
    metro: str | None,
) -> None:
    """Get latest CES employment data for an industry."""
    client = get_client(ctx)
    result = client.bls.employment_latest(naics, adjustment=adjustment, geo=geo, state=state, metro=metro)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))


# --- County data (QCEW) ---


@bls_group.command("county-employment")
@click.argument("fips")
@click.option("--industry", default="10", help="NAICS code (default: all industries).")
@click.option("--ownership", default="private", help="Ownership type (private, federal, state_govt, ...).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def bls_county_employment(
    ctx: click.Context,
    fips: str,
    industry: str,
    ownership: str,
    year: int | None,
    quarter: int | None,
    page: int,
    per_page: int,
) -> None:
    """Get quarterly county employment data."""
    client = get_client(ctx)
    result = client.bls.county_employment(
        fips, industry=industry, ownership=ownership, year=year, quarter=quarter, page=page, per_page=per_page
    )
    output(result.data, ctx.obj["format"], COUNTY_EMPLOYMENT_COLUMNS)


@bls_group.command("county-wages")
@click.argument("fips")
@click.option("--industry", default="10", help="NAICS code (default: all industries).")
@click.option("--ownership", default="private", help="Ownership type (private, federal, state_govt, ...).")
@click.option("--year", default=None, type=int, help="Data year.")
@click.option("--quarter", default=None, type=int, help="Quarter (1-4).")
@click.pass_context
def bls_county_wages(
    ctx: click.Context,
    fips: str,
    industry: str,
    ownership: str,
    year: int | None,
    quarter: int | None,
) -> None:
    """Get latest-quarter county wage snapshot."""
    client = get_client(ctx)
    result = client.bls.county_wages(fips, industry=industry, ownership=ownership, year=year, quarter=quarter)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        if result.data is None:
            click.echo("No data available for the requested filters.")
        else:
            data = result.data.model_dump(mode="json")
            rows = [{"field": k, "value": v} for k, v in data.items()]
            output(rows, fmt, ("field", "value"))


# --- Occupation data (OEWS) ---


@bls_group.command("occupations")
@click.option("--search", default=None, help="Search occupation titles.")
@click.option("--group", default=None, help="Filter by SOC level (major, detailed).")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def bls_occupations(ctx: click.Context, search: str | None, group: str | None, page: int, per_page: int) -> None:
    """List or search SOC occupations."""
    client = get_client(ctx)
    result = client.bls.occupations(search=search, group=group, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], OCCUPATION_LIST_COLUMNS)


@bls_group.command("occupation")
@click.argument("soc")
@click.pass_context
def bls_occupation(ctx: click.Context, soc: str) -> None:
    """Get occupation detail by SOC code."""
    client = get_client(ctx)
    result = client.bls.occupation(soc)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))


@bls_group.command("occupation-wages")
@click.argument("soc")
@click.option("--industry", default=None, help="NAICS code (2-6 digits).")
@click.option(
    "--geo",
    default="national",
    type=click.Choice(["national", "state", "metro"], case_sensitive=False),
    help="Geography level.",
)
@click.option("--state", default=None, help="2-digit FIPS code.")
@click.option("--metro", default=None, help="5-digit CBSA code.")
@click.option("--year", default=None, type=int, help="Reference year.")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def bls_occupation_wages(
    ctx: click.Context,
    soc: str,
    industry: str | None,
    geo: str,
    state: str | None,
    metro: str | None,
    year: int | None,
    page: int,
    per_page: int,
) -> None:
    """Get OEWS wage data for an occupation."""
    client = get_client(ctx)
    result = client.bls.occupation_wages(
        soc, industry=industry, geo=geo, state=state, metro=metro, year=year, page=page, per_page=per_page
    )
    output(result.data, ctx.obj["format"], OCCUPATION_WAGES_COLUMNS)


# --- Metrics (reference data) ---


@bls_group.command("metrics")
@click.option("--category", default=None, help="Filter by category (employment, wages, derived).")
@click.option("--source", default=None, help="Filter by source dataset (ces, qcew, oews).")
@click.option("--search", default=None, help="Search metric name/description.")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def bls_metrics(
    ctx: click.Context, category: str | None, source: str | None, search: str | None, page: int, per_page: int
) -> None:
    """List available BLS metrics."""
    client = get_client(ctx)
    result = client.bls.metrics(category=category, source=source, search=search, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], METRIC_LIST_COLUMNS)


@bls_group.command("metric")
@click.argument("metric")
@click.pass_context
def bls_metric(ctx: click.Context, metric: str) -> None:
    """Get metric detail by composite key."""
    client = get_client(ctx)
    result = client.bls.metric(metric)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        data = result.data.model_dump(mode="json")
        rows = [{"field": k, "value": v} for k, v in data.items()]
        output(rows, fmt, ("field", "value"))
