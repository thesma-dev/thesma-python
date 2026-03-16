"""Thesma CLI entry point.

Placeholder — real implementation in SDK-08.
"""

from __future__ import annotations

import click

from thesma._version import __version__


@click.group()
@click.version_option(version=__version__, prog_name="thesma")
def cli() -> None:
    """Thesma CLI — access SEC EDGAR data from your terminal."""
