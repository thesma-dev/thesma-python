# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added
- Screener: 4 new BLS LAUS filters — `min_local_unemployment_rate`, `max_local_unemployment_rate`, `local_unemployment_trend`, `min_local_labor_force` (and matching CLI options on `thesma screener screen`)
- Screener response: `labor_context.local_unemployment_rate`, `labor_context.local_unemployment_trend`, `labor_context.local_labor_force`, `labor_context.data_freshness.laus_period`
- Company enrichment: `labor_context.local_market` extended with 8 LAUS fields (`unemployment_rate`, `unemployment_rate_yoy_change`, `labor_force`, `labor_force_yoy_change_pct`, `laus_data_period`, `laus_data_lag_weeks`, `match_level`, `seasonal_adjustment`)

### Changed (BREAKING)
- `LocalMarketContext.source` type changed from `str` (always `"QCEW"`) to `Optional[str]` with values `"QCEW"`, `"LAUS+QCEW"`, `"LAUS"`, or `None`. Consumers doing `local_market.source == "QCEW"` will silently miss LAUS-enriched responses. Migration: switch to `if local_market.source and "QCEW" in local_market.source:` or accept any non-None value.
- `LocalMarketContext.data_period`, `data_lag_months`, `match_precision` are now `Optional` (may be `None` when only LAUS data contributed and no QCEW data exists for the county). Add null checks in consumer code.
- `data_freshness` dict in screener responses now has a 4th key `laus_period`. Consumers doing exact dict equality will need to update their assertions to include `"laus_period": None` (or the seeded value).
