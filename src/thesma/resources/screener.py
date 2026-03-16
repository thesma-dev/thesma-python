"""Screener resource — screen companies by financial thresholds."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import ScreenerResultItem
from thesma._types import PaginatedResponse


class Screener:
    """Resource for ``/v1/us/sec/screener`` endpoint."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def screen(
        self,
        *,
        min_revenue: float | None = None,
        min_net_income: float | None = None,
        min_gross_margin: float | None = None,
        max_gross_margin: float | None = None,
        min_operating_margin: float | None = None,
        min_net_margin: float | None = None,
        min_return_on_equity: float | None = None,
        min_return_on_assets: float | None = None,
        max_debt_to_equity: float | None = None,
        min_current_ratio: float | None = None,
        min_interest_coverage: float | None = None,
        min_revenue_growth: float | None = None,
        min_eps_growth: float | None = None,
        tier: str | None = None,
        sic: str | None = None,
        has_insider_buying: bool | None = None,
        has_institutional_increase: bool | None = None,
        sort_by: str | None = None,
        order: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[ScreenerResultItem]:
        """Screen companies by financial ratio thresholds.

        ``GET /v1/us/sec/screener``
        """
        params: dict[str, Any] = {
            "min_revenue": min_revenue,
            "min_net_income": min_net_income,
            "min_gross_margin": min_gross_margin,
            "max_gross_margin": max_gross_margin,
            "min_operating_margin": min_operating_margin,
            "min_net_margin": min_net_margin,
            "min_return_on_equity": min_return_on_equity,
            "min_return_on_assets": min_return_on_assets,
            "max_debt_to_equity": max_debt_to_equity,
            "min_current_ratio": min_current_ratio,
            "min_interest_coverage": min_interest_coverage,
            "min_revenue_growth": min_revenue_growth,
            "min_eps_growth": min_eps_growth,
            "tier": tier,
            "sic": sic,
            "has_insider_buying": has_insider_buying,
            "has_institutional_increase": has_institutional_increase,
            "sort": sort_by,
            "order": order,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/screener",
            params=params,
            response_model=PaginatedResponse[ScreenerResultItem],
        )
