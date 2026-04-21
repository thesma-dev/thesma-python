# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0.15] - 2026-04-21

### Added
- `companies.list(taxonomy=..., currency=...)` — two new filter kwargs matching query-parameter additions on the `/v1/us/sec/companies` endpoint. `taxonomy` accepts `"us-gaap"` or `"ifrs-full"` (server-validated; unknown values return 400 as `BadRequestError`); `currency` accepts a case-insensitive ISO-4217 code (`"USD"`, `"EUR"`, `"JPY"`, …). Companies with no parsed financials are excluded from filtered results.
- `screener.screen(taxonomy=..., currency=...)` — same two kwargs on the screener resource, for filtering screener output by the company's most-recent statement taxonomy and/or presentation currency.
- CLI: `thesma companies list --taxonomy` (`click.Choice(["us-gaap","ifrs-full"], case_sensitive=False)`) and `--currency` (free-form ISO-4217 string). Same options on `thesma screener screen`.

## [0.9.0.14] - 2026-04-21

### Added
- `FinancialStatementResponse.taxonomy` typed attribute (`str`) — US-GAAP and IFRS filings now surface their detected XBRL taxonomy (`"us-gaap"`, `"ifrs-full"`, or other/empty for the small residual cohort that could not be classified). The SDK deliberately keeps the type as `str` (not `Literal`) so future taxonomy-version strings do not break consumer code with `ValidationError`.
- `FinancialStatementResponse.reporting_notes` typed attribute (`ReportingNotes | None`) — presentation format (`"by_function"` / `"by_nature"` / `"unknown"`), IFRS-18 applicability, amendment / ambiguity detection flags, and an optional nested `presentation_format_detection_note` for ambiguous cases. The Python attribute is `reporting_notes` while the wire-level JSON key is `_reporting_notes`; Pydantic's `Field(alias=...)` plus a new `model_config = ConfigDict(populate_by_name=True)` block on `FinancialStatementResponse` makes both construction forms work (`FinancialStatementResponse(reporting_notes=...)` and `FinancialStatementResponse(**{"_reporting_notes": ...})`).
- New model classes: `ReportingNotes` (7 fields — 2 required, 5 optional) and `PresentationFormatDetectionNote` (3 required `list[str]` fields).
- README: coverage tagline now explicitly mentions IFRS alongside US-GAAP, with a new SPOT / EUR quickstart block and a "Typed responses" subsection documenting the hoisted typed attributes.

### Changed
- `FinancialStatementResponse` now declares `model_config = ConfigDict(populate_by_name=True)` — consumers instantiating the model directly (e.g. in test fixtures) can now pass either `reporting_notes=` (Python attribute) or `_reporting_notes=` (wire alias). `.model_dump()` emits the wire key by default; use `.model_dump(by_alias=False)` for the Python attribute form. Additive change; existing consumers that only read attributes are unaffected.
- IFRS 20-F filings (Spotify, Nu Holdings, ASML, and other US-listed IFRS reporters) now return native-currency financials with a correctly-detected `currency` and `taxonomy` — the govdata-api-side IFRS-01 / IFRS-07 work that this SDK release surfaces end-to-end.

## [0.9.0.13] - 2026-04-19

### Added
- Screener: new `search` kwarg on `screener.screen()` — filters by company name substring OR ticker prefix, case-insensitive. Matches the `companies.list(search=...)` semantics expanded in the govdata-api T-186 + T-190 changes. Exposed on the CLI as `thesma screener screen --search`.
- Companies: `companies.list(search=...)` docstring now documents the expanded name-or-ticker semantics (substring match on name OR prefix match on ticker, both case-insensitive, with null tickers silently skipped). The signature was already wired; only the documentation changed.
- CLI: `thesma companies list --search` help text updated to reflect the expanded semantics (previously read "Search by company name.").

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
