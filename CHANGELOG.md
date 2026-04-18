# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0.12] - 2026-04-18

### Added
- Screener: 6 new SBA filters on `screener.screen()` — `min_local_sba_loan_count`, `max_local_sba_loan_count`, `min_local_sba_lending_growth`, `max_local_sba_lending_growth`, `min_industry_sba_lending_growth`, `max_industry_sba_charge_off_rate`. Matching CLI options on `thesma screener screen` with kebab-case spellings.
- Screener CLI: new `--include` option on `thesma screener screen` accepting comma-separated values (`labor_context`, `lending_context`, or both). Previously only the Python kwarg was exposed.
- Screener response: `ScreenerResultItem.lending_context` (flat `LendingContextSummary` — `local_sba_loan_count_4q`, `local_sba_lending_growth_yoy`, `industry_sba_lending_growth_yoy`, `industry_sba_charge_off_rate`) and a new top-level `ScreenerResultItem.data_freshness` carrying `sba_period`. BLS `labor_context.data_freshness` remains nested.
- Company enrichment: `companies.get(cik, include="lending_context")` returns `data.lending_context` (`LendingContext` with `local_market` and `industry_lending` sub-objects). Combined `include="labor_context,lending_context"` returns both enrichment surfaces independently.
- Financials enrichment: `financials.get(cik, include="lending_context")` forwards the query parameter; the enrichment object is dropped by `FinancialStatementResponse` (`extra="ignore"`) until the envelope is hoisted — matches the existing `labor_context` limitation.
- New model classes: `LendingContext`, `LocalLendingMarket`, `IndustryLending`, `LendingContextSummary`, plus `DataFreshness.sba_period` (additive 5th period field).

## [0.9.0.11] - 2026-04-18

### Added
- New top-level `client.sba` resource wrapping the 9 SBA 7(a) standalone endpoints: `county_lending`, `state_lending`, `industry_lending`, `lenders`, `lender`, `lending_characteristics`, `lending_outcomes`, `metrics`, `metric`.
- New CLI command group `thesma sba` with 9 subcommands mirroring the resource methods (`county-lending`, `state-lending`, `industry-lending`, `lenders`, `lender`, `lending-characteristics`, `lending-outcomes`, `metrics`, `metric`).
- Response models (from OpenAPI regeneration): `CountyLendingPoint`, `StateLendingPoint`, `IndustryLendingPoint`, `LenderSummary`, `LenderDetail`, `LenderQuarterPoint`, `CharacteristicsDistribution`, `BucketCount`, `CategoryCount`, `VintageOutcomePoint`, `SbaMetricSummary`, `SbaMetricDetail`.

## [0.9.0.10] - 2026-04-18

### Added
- `companies.list()`: new `exchange` (str or list) and `domicile` (str) filter parameters, matching the UE-03 API expansion. Exposed as `--exchange` (repeatable) and `--domicile` on `thesma companies list`.
- `screener.screen()`: same `exchange` and `domicile` filter parameters. Exposed as `--exchange` (repeatable) and `--domicile` on `thesma screener screen`.
- Response: `CompanyListItem.exchange`, `CompanyListItem.domicile` (typed `Literal` fields from OpenAPI regeneration). Screener results and `companies.get()` also carry these fields via the existing `extra="allow"` stub pattern (untyped access).
- CLI table output: `thesma companies list` and `thesma screener screen` now render `exchange` and `domicile` columns by default.

## [0.9.0.9] - 2026-04-14

### Added
- Screener: 4 new BLS LAUS filters — `min_local_unemployment_rate`, `max_local_unemployment_rate`, `local_unemployment_trend`, `min_local_labor_force` (and matching CLI options on `thesma screener screen`)
- Screener response: `labor_context.local_unemployment_rate`, `labor_context.local_unemployment_trend`, `labor_context.local_labor_force`, `labor_context.data_freshness.laus_period`
- Company enrichment: `labor_context.local_market` extended with 8 LAUS fields (`unemployment_rate`, `unemployment_rate_yoy_change`, `labor_force`, `labor_force_yoy_change_pct`, `laus_data_period`, `laus_data_lag_weeks`, `match_level`, `seasonal_adjustment`)

### Changed (BREAKING)
- `LocalMarketContext.source` type changed from `str` (always `"QCEW"`) to `Optional[str]` with values `"QCEW"`, `"LAUS+QCEW"`, `"LAUS"`, or `None`. Consumers doing `local_market.source == "QCEW"` will silently miss LAUS-enriched responses. Migration: switch to `if local_market.source and "QCEW" in local_market.source:` or accept any non-None value.
- `LocalMarketContext.data_period`, `data_lag_months`, `match_precision` are now `Optional` (may be `None` when only LAUS data contributed and no QCEW data exists for the county). Add null checks in consumer code.
- `data_freshness` dict in screener responses now has a 4th key `laus_period`. Consumers doing exact dict equality will need to update their assertions to include `"laus_period": None` (or the seeded value).
