"""Shared CLI utilities."""

from __future__ import annotations

from typing import Any

import click


def get_client(ctx: click.Context) -> Any:
    """Get the client from the Click context, raising a clean error if missing."""
    client = ctx.obj.get("client")
    if client is None:
        click.echo("Error: Missing API key. Set THESMA_API_KEY or pass --api-key.", err=True)
        ctx.exit(1)
    return client
