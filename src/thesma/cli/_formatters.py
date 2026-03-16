"""Output formatters for the Thesma CLI."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Sequence
from typing import Any

import click
from pydantic import BaseModel


def _to_dict(item: Any) -> dict[str, Any]:
    """Convert an item to a dict, handling Pydantic models."""
    if isinstance(item, BaseModel):
        return item.model_dump(mode="json")
    if isinstance(item, dict):
        return item
    return dict(item)


def format_table(data: Sequence[Any], columns: Sequence[str]) -> str:
    """Format data as a table using tabulate with ``simple`` style."""
    from tabulate import tabulate

    rows: list[list[Any]] = []
    for item in data:
        d = _to_dict(item)
        rows.append([d.get(col) for col in columns])
    return str(tabulate(rows, headers=columns, tablefmt="simple"))


def format_json(data: Any) -> str:
    """Serialize data to JSON with indent=2."""
    if isinstance(data, BaseModel):
        return json.dumps(data.model_dump(mode="json"), indent=2)
    if isinstance(data, list):
        items = [item.model_dump(mode="json") if isinstance(item, BaseModel) else item for item in data]
        return json.dumps(items, indent=2)
    return json.dumps(data, indent=2, default=str)


def format_csv(data: Sequence[Any], columns: Sequence[str]) -> str:
    """Format data as CSV with headers."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(columns), extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for item in data:
        writer.writerow(_to_dict(item))
    return buf.getvalue()


def output(data: Any, fmt: str, columns: Sequence[str]) -> None:
    """Dispatch to the right formatter and echo the result."""
    if fmt == "json":
        click.echo(format_json(data))
    elif fmt == "csv":
        if isinstance(data, list):
            click.echo(format_csv(data, columns))
        else:
            click.echo(format_csv([data], columns))
    else:
        if isinstance(data, list):
            click.echo(format_table(data, columns))
        else:
            click.echo(format_table([data], columns))
