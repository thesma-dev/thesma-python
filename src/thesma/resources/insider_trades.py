"""Insider trades resource — SEC Form 4 transaction data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import InsiderTradeAggregateListItem, InsiderTradeListItem
from thesma._types import PaginatedResponse


class InsiderTrades:
    """Resource for insider trade endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        identifier: str,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        person: str | None = None,
        trade_type: str | None = None,
        min_value: int | None = None,
        flat: bool = False,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[InsiderTradeAggregateListItem] | PaginatedResponse[InsiderTradeListItem]:
        """List insider trades for a company.

        ``GET /v1/us/sec/companies/{identifier}/insider-trades``

        ``identifier`` accepts a CIK or ticker (case-insensitive, with
        ``TickerAlias`` fallback).

        By default, rows are aggregated transaction events — same-day 10b5-1
        tranches collapse into one row keyed on
        ``(person, date, type, security, ownership)``. Each row carries
        ``price_range`` (min/max across slices), ``slice_count``, and
        weighted-average ``price_per_share``. Pass ``flat=True`` to get
        the pre-T5 per-slice row shape.

        When combined with ``min_value``, aggregate mode filters against
        the post-SUM aggregate ``total_value``; flat mode filters
        per-slice. The API handles the WHERE-vs-HAVING dispatch.
        """
        params: dict[str, Any] = {
            "from": from_date,
            "to": to_date,
            "person": person,
            "type": trade_type,
            "min_value": min_value,
            # Only forward ``flat`` when the caller opts in — keeps the wire
            # clean (``?flat=false`` is accepted by the API but noisy, and
            # sending it conflates the default path with an explicit ask).
            "flat": True if flat else None,
            "page": page,
            "per_page": per_page,
        }
        response_model = (
            PaginatedResponse[InsiderTradeListItem] if flat else PaginatedResponse[InsiderTradeAggregateListItem]
        )
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/insider-trades",
            params=params,
            response_model=response_model,
        )

    def list_all(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        person: str | None = None,
        trade_type: str | None = None,
        min_value: int | None = None,
        flat: bool = False,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[InsiderTradeAggregateListItem] | PaginatedResponse[InsiderTradeListItem]:
        """List insider trades across all companies.

        ``GET /v1/us/sec/insider-trades``

        Same aggregation semantics as ``list()``: aggregate-first by default,
        ``flat=True`` to get per-slice rows. ``min_value`` filters against
        aggregate ``total_value`` (aggregate mode) or per-slice value
        (flat mode).
        """
        params: dict[str, Any] = {
            "from": from_date,
            "to": to_date,
            "person": person,
            "type": trade_type,
            "min_value": min_value,
            "flat": True if flat else None,
            "page": page,
            "per_page": per_page,
        }
        response_model = (
            PaginatedResponse[InsiderTradeListItem] if flat else PaginatedResponse[InsiderTradeAggregateListItem]
        )
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/insider-trades",
            params=params,
            response_model=response_model,
        )
