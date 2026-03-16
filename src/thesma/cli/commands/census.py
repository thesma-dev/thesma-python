"""CLI commands for the census resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

METRIC_SUMMARY_COLUMNS = ("canonical_name", "display_name", "category", "unit")
PLACE_METRIC_COLUMNS = ("canonical_name", "display_name", "value", "unit")
COMPARE_COLUMNS = ("fips", "name", "value", "moe")


@click.group("census")
def census_group() -> None:
    """US Census demographic and economic data."""


@census_group.command("place")
@click.argument("fips")
@click.option("--metric", multiple=True, help="Metric(s) to include. Repeat for multiple.")
@click.pass_context
def census_place(ctx: click.Context, fips: str, metric: tuple[str, ...]) -> None:
    """Get all metrics for a place by FIPS code."""
    client = get_client(ctx)
    result = client.census.place(fips)
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        metrics_data = result.data.metrics
        if metric:
            metrics_data = [m for m in metrics_data if m.canonical_name in metric]
        output(metrics_data, fmt, PLACE_METRIC_COLUMNS)


@census_group.command("metrics")
@click.pass_context
def census_metrics(ctx: click.Context) -> None:
    """List all available Census metrics."""
    client = get_client(ctx)
    result = client.census.metrics()
    output(result.data, ctx.obj["format"], METRIC_SUMMARY_COLUMNS)


@census_group.command("compare")
@click.argument("metric")
@click.option("--fips", multiple=True, required=True, help="FIPS code(s) to compare. Repeat for multiple.")
@click.pass_context
def census_compare(ctx: click.Context, metric: str, fips: tuple[str, ...]) -> None:
    """Compare a metric across multiple places."""
    client = get_client(ctx)
    result = client.census.compare(metric, fips=list(fips))
    fmt = ctx.obj["format"]
    if fmt == "json":
        output(result, fmt, ())
    else:
        output(result.data.places, fmt, COMPARE_COLUMNS)
