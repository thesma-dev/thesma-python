"""Screener resource — screen companies by financial thresholds."""

from __future__ import annotations

import math
from typing import Any

from thesma._generated.models import ScreenerResultItem
from thesma._types import PaginatedResponse

# Source-of-truth list: T-216-screener-scale-mismatch-reject.md:48-75.
# When the server-side list changes, update this frozenset in the same
# release (grep the T-216 archive file at implementation time).
_PERCENTAGE_SCALE_PARAMS: frozenset[str] = frozenset(
    {
        # Financial ratios (8 entries)
        "min_gross_margin",
        "max_gross_margin",
        "min_operating_margin",
        "min_net_margin",
        "min_return_on_equity",
        "min_return_on_assets",
        "min_revenue_growth",  # maps to revenue_growth_yoy server-side
        "min_eps_growth",  # maps to eps_growth_yoy server-side
        # Institutional ownership (1 entry)
        "min_institutional_ownership_pct",
        # BLS / labor-market percentage fields (10 entries)
        "min_industry_employment_growth",
        "max_industry_employment_growth",
        "min_industry_wage_growth",
        "min_hq_county_wage_growth",
        "min_industry_quits_rate",
        "max_industry_quits_rate",
        "min_industry_openings_rate",
        "max_industry_openings_rate",
        "min_local_unemployment_rate",
        "max_local_unemployment_rate",
        # SBA lending percentage fields (4 entries)
        "min_local_sba_lending_growth",
        "max_local_sba_lending_growth",
        "min_industry_sba_lending_growth",
        "max_industry_sba_charge_off_rate",
    }
)
# Total: 8 + 1 + 10 + 4 = 23 entries.


def _validate_percentage_scale(name: str, value: float) -> None:
    """Mirror T-216's server-side rejection client-side.

    Raises ValueError on:
    - non-numeric types (guards TypeError from math.isfinite on strings);
    - non-finite values (NaN, +inf, -inf);
    - values in the ambiguous 0 < x < 1.0 band.

    Accepts 0 (documented "no minimum" sentinel), negative values,
    and all values >= 1.0.
    """
    # Guard: non-numeric inputs would raise TypeError from math.isfinite.
    # The SDK method signatures are type-hinted `float | None` but Python
    # does not enforce at runtime; a caller passing "0.2" (string from
    # a config file, env var, or DataFrame cell) otherwise hits a
    # TypeError with the unhelpful "must be real number, not str".
    # `bool` is a subclass of `int`, so exclude it explicitly — booleans
    # as percentage values are always caller bugs.
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(
            f"{name}={value!r} is not numeric. Pass an int or float (use percentage scale: 20 for 20%, not 0.20)."
        )
    # Non-finite: keep the scale hint OUT of this branch — infinity is
    # not a decimal-fraction problem, it's a range problem. Conflating
    # the two confuses debugging.
    if not math.isfinite(value):
        raise ValueError(f"{name}={value} is not a finite number.")
    # The actual scale-ambiguity gate that T-216 mirrors.
    if 0 < value < 1.0:
        raise ValueError(
            f"{name}={value} is in the ambiguous 0-1 range. Use "
            f"percentage scale (e.g. 20 for 20%), not decimal "
            f"fractions (0.20). Pass 0 for 'no minimum', or a "
            f"value >= 1.0."
        )


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
        in_index: bool | None = None,
        sic: str | list[str] | None = None,
        exchange: str | list[str] | None = None,
        domicile: str | None = None,
        search: str | None = None,
        taxonomy: str | None = None,
        currency: str | None = None,
        has_insider_buying: bool | None = None,
        has_institutional_increase: bool | None = None,
        max_net_income: float | None = None,
        min_institutional_ownership_pct: float | None = None,
        insider_buying_days: str | None = None,
        industry_hiring_trend: str | None = None,
        min_industry_employment_growth: float | None = None,
        max_industry_employment_growth: float | None = None,
        min_industry_wage_growth: float | None = None,
        min_hq_county_wage_growth: float | None = None,
        min_comp_to_market_ratio: float | None = None,
        min_industry_quits_rate: float | None = None,
        max_industry_quits_rate: float | None = None,
        min_industry_openings_rate: float | None = None,
        max_industry_openings_rate: float | None = None,
        min_local_unemployment_rate: float | None = None,
        max_local_unemployment_rate: float | None = None,
        local_unemployment_trend: str | None = None,
        min_local_labor_force: int | None = None,
        min_local_sba_loan_count: int | None = None,
        max_local_sba_loan_count: int | None = None,
        min_local_sba_lending_growth: float | None = None,
        max_local_sba_lending_growth: float | None = None,
        min_industry_sba_lending_growth: float | None = None,
        max_industry_sba_charge_off_rate: float | None = None,
        include: str | None = None,
        sort_by: str | None = None,
        order: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[ScreenerResultItem]:
        """Screen companies by financial ratio thresholds.

        ``GET /v1/us/sec/screener``

        **Scale conventions.** Percentage-scale filter params (e.g.
        ``min_operating_margin``, ``min_return_on_equity``, the ``*_growth``
        and ``*_rate`` fields) use the integer-percent convention: pass
        ``20`` for "20%", not ``0.20``. Values in the ambiguous
        ``0 < x < 1.0`` range raise ``ValueError`` client-side before the
        HTTP request (mirrors the T-216 server-side 400). Pass ``0`` as the
        "no minimum" sentinel; negative values and ``>= 1.0`` pass through.
        True-ratio filters (``min_current_ratio``, ``max_debt_to_equity``,
        ``min_interest_coverage``, ``min_comp_to_market_ratio``) are NOT
        validated — ``0.5`` is a legitimate value there. When multiple
        percentage-scale params violate in the same call, the first
        violating param is reported alphabetically; ``max_*`` params sort
        before ``min_*`` (``a < i``), so ``max_gross_margin=0.3,
        min_operating_margin=0.2`` surfaces the ``max_gross_margin`` error.

        The four LAUS filters ``min_local_unemployment_rate``,
        ``max_local_unemployment_rate``, ``local_unemployment_trend``, and
        ``min_local_labor_force`` screen on HQ-county local labour market
        data from BLS LAUS. Trend values are ``"improving"``, ``"stable"``,
        or ``"worsening"``.

        .. note::

           ``local_unemployment_trend`` is **case-sensitive** at the
           resource layer — direct Python callers must pass lowercase
           strings (``"improving"``, ``"stable"``, ``"worsening"``) or the
           API will return 422. The ``thesma screener screen`` CLI command
           normalises case via ``click.Choice(case_sensitive=False)``
           before calling this method, but that normalisation does not
           apply here. When ``local_unemployment_trend`` is set, companies
           with no YoY LAUS data (and therefore a null trend) are excluded
           from results — this matches the ``industry_hiring_trend``
           behaviour.

        ``exchange`` accepts a single string (``"nyse"``) or list
        (``["nyse", "nasdaq"]``). ``domicile`` is a single value
        (``"us"`` or ``"adr"``). Unlike ``local_unemployment_trend``,
        both filters are **case-insensitive** — the API normalises case
        before querying, so callers may pass any case.

        ``taxonomy`` filters by the filing taxonomy of the company's
        most-recent parsed statement — accepted values are ``"us-gaap"``
        and ``"ifrs-full"``; other inputs return 400 as
        ``BadRequestError``. ``currency`` filters by the presentation
        currency — case-insensitive 3-letter ISO-4217 code (``"USD"``,
        ``"EUR"``, ``"JPY"``…); unknown codes return 400. Companies with
        no parsed financials are excluded from results on either filter.

        The six SBA filters (``min_local_sba_loan_count``,
        ``max_local_sba_loan_count``, ``min_local_sba_lending_growth``,
        ``max_local_sba_lending_growth``,
        ``min_industry_sba_lending_growth``,
        ``max_industry_sba_charge_off_rate``) screen on HQ-county and
        NAICS-industry SBA 7(a) lending signals. Loan counts are integer
        trailing-4Q totals; growth values are YoY percentages (floats);
        the charge-off rate is a percentage (float). When any SBA filter
        is applied or ``include="lending_context"`` is passed, each
        ``ScreenerResultItem`` gains a flat ``lending_context`` summary
        (no nested ``data_freshness``) — SBA freshness lives in the new
        top-level ``data_freshness`` object on each result item, next to
        ``labor_context.data_freshness`` (which remains nested for BLS).

        Post-S3, ``labor_context`` on each ``ScreenerResultItem`` is the
        unified nested shape with sub-objects ``industry``, ``local_market``,
        ``turnover``, ``compensation_benchmark``, ``summary``, and
        ``data_freshness``. The derived classification fields (previously
        flat on ``ScreenerResultItem.labor_context``) moved to
        ``labor_context.summary.industry_hiring_trend`` /
        ``.local_unemployment_trend`` / ``.comp_to_market_ratio`` /
        ``.labour_market_tightness``. Consumers accessing the flat shape
        before SDK 0.10 must migrate to the nested path.

        ``search`` filters by company name substring OR ticker prefix,
        case-insensitive. The value is passed through to the API verbatim
        — the server trims whitespace, escapes SQL LIKE wildcards, and
        silently skips the ticker branch for companies with a null
        ticker. Known v1 limitations inherited from the API: separators
        differ between the filing record and common market conventions
        (``BRK-B`` on EDGAR vs ``BRK.B`` on Yahoo Finance) so matches are
        separator-sensitive, and there is no ticker alias resolution.
        Because the screener inner-joins against annual ratios, any
        match whose latest annual ratio row has been filtered out (e.g.
        an insufficient fiscal-year window) is silently excluded from
        the result set — use ``companies.list(search=...)`` if you need
        pure company discovery without the ratio-availability filter.
        """
        if isinstance(exchange, list) and not exchange:
            exchange = None
        # Client-side mirror of T-216 server-side scale-ambiguity rejection.
        # Build the validation dict from explicit kwarg references — NOT via
        # `locals()`. locals() returns `self` plus all other local variables,
        # typed as `dict[str, Any]` (mypy strict flags every lookup as Any).
        # Explicit construction is typed, cheap, and doesn't leak scope.
        _scale_values: dict[str, float | None] = {
            "min_gross_margin": min_gross_margin,
            "max_gross_margin": max_gross_margin,
            "min_operating_margin": min_operating_margin,
            "min_net_margin": min_net_margin,
            "min_return_on_equity": min_return_on_equity,
            "min_return_on_assets": min_return_on_assets,
            "min_revenue_growth": min_revenue_growth,
            "min_eps_growth": min_eps_growth,
            "min_institutional_ownership_pct": min_institutional_ownership_pct,
            "min_industry_employment_growth": min_industry_employment_growth,
            "max_industry_employment_growth": max_industry_employment_growth,
            "min_industry_wage_growth": min_industry_wage_growth,
            "min_hq_county_wage_growth": min_hq_county_wage_growth,
            "min_industry_quits_rate": min_industry_quits_rate,
            "max_industry_quits_rate": max_industry_quits_rate,
            "min_industry_openings_rate": min_industry_openings_rate,
            "max_industry_openings_rate": max_industry_openings_rate,
            "min_local_unemployment_rate": min_local_unemployment_rate,
            "max_local_unemployment_rate": max_local_unemployment_rate,
            "min_local_sba_lending_growth": min_local_sba_lending_growth,
            "max_local_sba_lending_growth": max_local_sba_lending_growth,
            "min_industry_sba_lending_growth": min_industry_sba_lending_growth,
            "max_industry_sba_charge_off_rate": max_industry_sba_charge_off_rate,
        }
        # Sort deterministically so multi-violation inputs produce reproducible
        # error messages. Alphabetical sort puts `max_*` params before `min_*`
        # (a < i), so `max_gross_margin=0.3` reports before any concurrently-
        # passed `min_*` decimal-fraction violation.
        for _name in sorted(_scale_values):
            _value = _scale_values[_name]
            if _value is not None:
                _validate_percentage_scale(_name, _value)
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
            "in_index": in_index,
            "sic": sic,
            "exchange": exchange,
            "domicile": domicile,
            "search": search,
            "taxonomy": taxonomy,
            "currency": currency,
            "has_insider_buying": has_insider_buying,
            "has_institutional_increase": has_institutional_increase,
            "max_net_income": max_net_income,
            "min_institutional_ownership_pct": min_institutional_ownership_pct,
            "insider_buying_days": insider_buying_days,
            "industry_hiring_trend": industry_hiring_trend,
            "min_industry_employment_growth": min_industry_employment_growth,
            "max_industry_employment_growth": max_industry_employment_growth,
            "min_industry_wage_growth": min_industry_wage_growth,
            "min_hq_county_wage_growth": min_hq_county_wage_growth,
            "min_comp_to_market_ratio": min_comp_to_market_ratio,
            "min_industry_quits_rate": min_industry_quits_rate,
            "max_industry_quits_rate": max_industry_quits_rate,
            "min_industry_openings_rate": min_industry_openings_rate,
            "max_industry_openings_rate": max_industry_openings_rate,
            "min_local_unemployment_rate": min_local_unemployment_rate,
            "max_local_unemployment_rate": max_local_unemployment_rate,
            "local_unemployment_trend": local_unemployment_trend,
            "min_local_labor_force": min_local_labor_force,
            "min_local_sba_loan_count": min_local_sba_loan_count,
            "max_local_sba_loan_count": max_local_sba_loan_count,
            "min_local_sba_lending_growth": min_local_sba_lending_growth,
            "max_local_sba_lending_growth": max_local_sba_lending_growth,
            "min_industry_sba_lending_growth": min_industry_sba_lending_growth,
            "max_industry_sba_charge_off_rate": max_industry_sba_charge_off_rate,
            "include": include,
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
