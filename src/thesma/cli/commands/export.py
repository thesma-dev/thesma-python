"""CLI commands for bulk data exports."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import click

from thesma._export import ExportResult, ExportStream
from thesma.cli._utils import get_client


def _run_export(
    ctx: click.Context,
    resource_name: str,
    method_name: str,
    export_format: str,
    output: str | None,
    since: str | None,
    cik: str | None,
    ticker: str | None,
) -> None:
    """Shared implementation for all export subcommands."""
    client = get_client(ctx)
    export_method = getattr(client.export, method_name)

    if output is not None:
        # File download mode — use iterator internally for progress control
        resolved = str(Path(output).resolve())
        stream: ExportStream = export_method(format=export_format, since=since, cik=cik, ticker=ticker)
        is_tty = sys.stderr.isatty()
        rows = 0

        try:
            with open(resolved, "w", newline="") as f:
                if export_format == "csv":
                    first = True
                    with stream:
                        for row in stream:
                            if first:
                                # Write CSV header from first row's keys
                                file_writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                                file_writer.writeheader()
                                first = False
                            file_writer.writerow(row)
                            rows += 1
                            if is_tty and rows % 10_000 == 0:
                                click.echo(f"\rExporting {resource_name}... {rows:,} rows", nl=False, err=True)
                else:
                    with stream:
                        for row in stream:
                            f.write(json.dumps(row) + "\n")
                            rows += 1
                            if is_tty and rows % 10_000 == 0:
                                click.echo(f"\rExporting {resource_name}... {rows:,} rows", nl=False, err=True)
        except Exception:
            stream.close()
            raise

        result = ExportResult(
            path=resolved,
            rows=rows,
            complete=stream.complete,
            format=export_format,
        )

        if is_tty:
            click.echo("", err=True)  # newline after progress
        status = "complete" if result.complete else "incomplete"
        click.echo(f"Exported {result.rows:,} rows to {result.path} ({status})", err=True)

        if not result.complete:
            click.echo(
                "Warning: Export may be incomplete (server sentinel not found). Use --since to resume.",
                err=True,
            )
    else:
        # Stdout streaming mode
        stream = export_method(format=export_format, since=since, cik=cik, ticker=ticker)
        try:
            if export_format == "csv":
                first = True
                writer: csv.DictWriter[str] | None = None
                with stream:
                    for row in stream:
                        if first:
                            writer = csv.DictWriter(sys.stdout, fieldnames=list(row.keys()))
                            writer.writeheader()
                            first = False
                        if writer is not None:
                            writer.writerow(row)
            else:
                with stream:
                    for row in stream:
                        click.echo(json.dumps(row))
        except BrokenPipeError:
            pass


def _common_options(f: Any) -> Any:
    """Apply common export options to a Click command."""
    f = click.option("--output", "-o", default=None, help="Output file path. Streams to stdout if omitted.")(f)
    f = click.option(
        "--format",
        "-f",
        "export_format",
        type=click.Choice(["jsonl", "csv"], case_sensitive=False),
        default="jsonl",
        help="Export format (default: jsonl).",
    )(f)
    f = click.option("--since", default=None, help="Only include records after this ISO timestamp.")(f)
    f = click.option("--cik", default=None, help="Filter by CIK.")(f)
    f = click.option("--ticker", default=None, help="Filter by ticker symbol.")(f)
    f = click.pass_context(f)
    return f


def _holdings_options(f: Any) -> Any:
    """Apply export options with fund-specific help text for holdings."""
    f = click.option("--output", "-o", default=None, help="Output file path. Streams to stdout if omitted.")(f)
    f = click.option(
        "--format",
        "-f",
        "export_format",
        type=click.Choice(["jsonl", "csv"], case_sensitive=False),
        default="jsonl",
        help="Export format (default: jsonl).",
    )(f)
    f = click.option("--since", default=None, help="Only include records after this ISO timestamp.")(f)
    f = click.option("--cik", default=None, help="Filter by fund CIK (the 13F filer).")(f)
    f = click.option("--ticker", default=None, help="Filter by fund ticker (the 13F filer).")(f)
    f = click.pass_context(f)
    return f


@click.group("export")
def export_group() -> None:
    """Bulk-export complete datasets as JSONL or CSV."""


@export_group.command("companies")
@_common_options
def export_companies(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all companies."""
    _run_export(ctx, "companies", "companies", export_format, output, since, cik, ticker)


@export_group.command("financials")
@_common_options
def export_financials(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all financial data."""
    _run_export(ctx, "financials", "financials", export_format, output, since, cik, ticker)


@export_group.command("insider-trades")
@_common_options
def export_insider_trades(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all insider trade data."""
    _run_export(ctx, "insider-trades", "insider_trades", export_format, output, since, cik, ticker)


@export_group.command("events")
@_common_options
def export_events(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all corporate events."""
    _run_export(ctx, "events", "events", export_format, output, since, cik, ticker)


@export_group.command("ratios")
@_common_options
def export_ratios(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all financial ratios."""
    _run_export(ctx, "ratios", "ratios", export_format, output, since, cik, ticker)


@export_group.command("holdings")
@_holdings_options
def export_holdings(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all institutional holdings."""
    _run_export(ctx, "holdings", "holdings", export_format, output, since, cik, ticker)


@export_group.command("compensation")
@_common_options
def export_compensation(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all executive compensation data."""
    _run_export(ctx, "compensation", "compensation", export_format, output, since, cik, ticker)


@export_group.command("beneficial-ownership")
@_common_options
def export_beneficial_ownership(
    ctx: click.Context, output: str | None, export_format: str, since: str | None, cik: str | None, ticker: str | None
) -> None:
    """Export all beneficial ownership data."""
    _run_export(ctx, "beneficial_ownership", "beneficial_ownership", export_format, output, since, cik, ticker)
