"""CLI commands for the events resource."""

from __future__ import annotations

import click

from thesma.cli._formatters import output
from thesma.cli._utils import get_client

EVENT_LIST_COLUMNS = ("filed_at", "category", "company_name")
EVENT_CATEGORY_COLUMNS = ("name", "description", "filing_count")


@click.group("events")
def events_group() -> None:
    """SEC 8-K corporate event data."""


@events_group.command("list")
@click.argument("cik")
@click.option("--category", default=None, help="Filter by event category.")
@click.option("--page", default=1, type=int, help="Page number.")
@click.option("--per-page", default=25, type=int, help="Results per page.")
@click.pass_context
def events_list(ctx: click.Context, cik: str, category: str | None, page: int, per_page: int) -> None:
    """List events for a company."""
    client = get_client(ctx)
    result = client.events.list(cik, category=category, page=page, per_page=per_page)
    output(result.data, ctx.obj["format"], EVENT_LIST_COLUMNS)


@events_group.command("categories")
@click.pass_context
def events_categories(ctx: click.Context) -> None:
    """List all event categories."""
    client = get_client(ctx)
    result = client.events.categories()
    output(result.data, ctx.obj["format"], EVENT_CATEGORY_COLUMNS)
