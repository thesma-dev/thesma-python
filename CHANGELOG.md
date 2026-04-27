# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.1.1] - 2026-04-27

### Added
- New typed exception `TierRequiredError` (subclass of new `PaymentRequiredError`,
  in turn a subclass of `ThesmaError`) for 402 responses with
  `error.code == "tier_required"`. Carries typed `current_tier` and
  `required_tier` attributes so callers can render upgrade CTAs without
  parsing the message string. Triggered by govdata-api `0.11.1`'s S17 gate
  on `/v1/us/sec/screener?include=...` for Free / Starter callers.
- New typed exception `PaymentRequiredError` for 402 responses generally.
  Catches both `tier_required` (subclassed) and `plan_cap_exceeded` (the
  webhook plan-cap path on `POST /v1/webhooks` for free-tier callers, which
  previously fell through to the generic `ThesmaError`). Existing
  `except ThesmaError` clauses continue to catch both — `PaymentRequiredError`
  is a subclass.

### Fixed
- `_parse_error_body` now correctly extracts `code` and `message` from the
  api's nested-error response shape (`{"error": {"code": "...", ...}}`).
  Previously the helper read top-level keys only and returned `error_code=None`
  for every 4xx and 429 response in production. The class-level dispatch
  (400 → `BadRequestError` etc.) was unaffected because that's keyed on
  status code. **`ExportInProgressError` (the 429 + `error_code == "export_in_progress"`
  discriminator) was effectively dead code in production** — it can now fire
  correctly. The helper falls back to top-level keys for proxy errors and
  forward-compat. SDK-38's flat-shape test mocks did not catch this; the
  expanded test fixtures in `tests/test_errors.py` exercise both shapes.

## [0.11.1.0] - 2026-04-26

### Added
- `client.webhooks` resource restored. govdata-api `0.11.1` (T-219) opened
  webhook-management routes to API-key authentication in addition to portal
  JWT, which makes a programmatic webhook surface workable from the SDK.
  The resource exposes 10 methods covering the full post-S-14 webhook
  surface: `list_event_types()`, `list()`, `create()`, `get()`, `update()`,
  `delete()`, `list_deliveries()`, `rotate_secret()`, `send_test()`,
  `replay_delivery()`. SDK-36 (`0.11.0.0`) had removed an earlier, broken
  version of this resource that 403'd against the live api; the restored
  version is built against the current OpenAPI surface and works under
  API-key auth.
- Webhook calls under API-key auth consume the key's burst + daily
  rate-limit budget (under JWT auth they bypassed rate limits per
  `middleware/rate_limit.py` shape). Customers driving heavy webhook
  lifecycle traffic from the SDK should account for the additional quota
  usage.

## [0.11.0.1] - 2026-04-26

### Added
- `client.sections.search()` now accepts five additional optional filters
  matching the underlying api: `cik`, `filing_type`, `section_type`, `year`,
  and `min_similarity`. All five are pass-through and server-validated;
  `min_similarity` defaults to `0.3` server-side when omitted. Exposes the
  `/v1/us/sec/sections/search?cik=...&filing_type=...&section_type=...&year=...&min_similarity=...`
  query surface that has been live on the api since launch but was not
  reachable through the SDK.

## [0.11.0.0] - 2026-04-25

### Breaking
- `client.webhooks` resource removed. Webhook management endpoints on the
  api (`/v1/webhooks/*`) use JWT authentication; the SDK uses `X-API-Key`
  exclusively. The previous `client.webhooks.list()`, `create()`, `get()`,
  `update()`, `delete()`, and `deliveries()` methods returned `403
  Forbidden` against the live api for every caller — the resource was
  dead code that misrepresented the SDK's surface. Customers needing
  programmatic webhook management should call the api endpoints directly
  with a portal JWT; see https://docs.thesma.dev/webhooks for the
  supported flow.

### Changed
- Generated models regenerated from api OpenAPI 0.11.0. The post-S-14
  webhook schemas (`WebhookEventTypeResponse`, `WebhookSecretRotateResponse`,
  `WebhookTestResponse`, `WebhookReplayResponse`, plus new fields on
  `WebhookResponse` and `WebhookDeliveryResponse`) are still importable
  from `thesma._generated.models` for consumers building their own
  JWT-based integrations — but no SDK resource wraps them.

## [0.10.1.2] - 2026-04-24

### Changed
- `client.screener.screen()` now validates percentage-scale filter params
  client-side before constructing the HTTP request. Values in the ambiguous
  `0 < x < 1.0` range raise `ValueError` with a clear "use integer percent"
  message, matching the server-side validation shipped in govdata-api T-216
  on 2026-04-25. Non-finite values (NaN, ±inf) also raise `ValueError`.
  Behavioural note: customers passing decimal fractions (`0.2` for "20%")
  previously received an `ApiError` from a server 400; they now receive a
  `ValueError` synchronously before the HTTP round-trip. Exception-type
  change only — the fix itself is a developer-experience improvement.
- Screener method docstring gains a "Scale conventions" paragraph
  documenting the integer-percent convention + the `0 = no minimum` sentinel.

## [0.10.1.1] - 2026-04-24

### Changed
- `client.companies.get(cik, include="events")` now returns a populated `events` slot on `result.data.events` (list of ≤10 recent 8-K filings). Pre-SDK-34 the API returned 400 with a "events expansion temporarily unavailable" message, which the SDK's docstring documented. govdata-api T-215 shipped the expander on 2026-04-24; this SDK release removes the stale caveat from the `Companies.get()` docstring.
- Internal: two unit tests that mocked the 400-dispatch response are replaced with 200-path assertions on the new populated slot. No public-API signature change.

## [0.10.1.0] - 2026-04-24

SDK-33: enrichment-envelope typed hoist. Spec-regen picks up API `0.10.1` (`LocalMarketContext` county fields went nullable — forward-compatible). Eight new hand-correction patches replace the opaque `Enriched*` stubs with declared typed fields so consumers get `result.labor_context`, `result.lending_context`, `result.enrichment_warnings`, and the 7 S1 expander slots as typed attributes instead of `model_extra` dict lookups.

### Changed
- `client.financials.get()` now returns `EnrichedFinancialDataResponse` (was `DataResponse[FinancialStatementResponse]`) on the single-statement non-paginated path. `client.companies.get()` now returns `EnrichedCompanyDataResponse` (was `DataResponse[EnrichedCompanyData]`). `client.compensation.get()` now returns `EnrichedCompensationDataResponse` (was `DataResponse[CompensationResponse]`). Source-compatible for untyped consumers that reach for `.data.<field>`; mypy-strict consumers who narrowed on the old return types need to update annotations.
- Generic `DataResponse[T]` now declares `warnings_` (alias `_warnings`) and `enrichment_warnings` (alias `_enrichment_warnings`) as optional typed fields, plus `model_config=ConfigDict(populate_by_name=True, extra="allow")`. Any endpoint still routing through `DataResponse[T]` gains typed access to those envelope-metadata siblings.

### Fixed
- `client.financials.get(..., include="labor_context")` / `include="lending_context"` now populate the enrichment payload on `result.labor_context` / `result.lending_context`. Pre-SDK-33, the URL parameter was forwarded correctly but the enrichment payload was silently dropped by `DataResponse[FinancialStatementResponse]` (open since SDK-19 / SDK-22). Customers using BLS labor enrichment on single-statement financials calls should see non-`None` values where they previously saw `None`.
- `_enrichment_warnings` now surfaces as typed `list[EnrichmentWarning]` on all 5 enriched envelopes (companies detail, financials single-statement, financials `statement=all`, financials `statement=all` paginated, compensation). Pre-SDK-33 it was only accessible via `.model_extra.get("_enrichment_warnings")`.
- `EnrichedCompanyData` S1 expander slots (`financials`, `ratios`, `events`, `insider_trades`, `holders`, `compensation`, `board`) now land as typed `Any | None` attributes; pre-SDK-33 they sat in `.model_extra`. Base company fields (`cik`, `name`, `ticker`, etc.) continue to pass through via `extra="allow"` because the SDK has no `CompanyResponse` codegen class.

### Internal
- Renamed `scripts/regenerate.py::_SDK24_PATCHES` → `_SDK_PATCHES` to reflect the broader scope (SDK-24 and SDK-33 patches both live here now). Added 8 new hand-correction patches for the 6 `Enriched*` envelope stubs plus `FinancialStatementListItem` and `MultiStatementListItem`.

## [0.10.0.0] - 2026-04-23

Release bundle for the govdata-api Wave 1 + S1 shape landing (API `0.10.0`, merged as `f80b6c6`). Eight SDK prompts propagate together: SDK-25 (URL renames + HATEOAS), SDK-26 (`per_page` on financials), SDK-27 (`statement=all`), SDK-28 (unified LaborContext), SDK-29 (holders temporal fields), SDK-30 (insider-trades aggregation), SDK-31 (enrichment warnings), SDK-32 (`include=` composition).

### Breaking
- URL path renames on two SDK methods (same Python interface, different underlying HTTP path): `client.compensation.get()` now targets `/compensation` (was `/executive-compensation`); `client.holdings.holders()` now targets `/holders` (was `/institutional-holders`). SDK consumers using the typed methods see no change; consumers hitting URLs manually must update.
- `LaborContextSummary` reshaped: the pre-S3 flat 12-field shape is replaced with a 4-field derived-summary shape (`industry_hiring_trend`, `local_unemployment_trend`, `comp_to_market_ratio`, `labour_market_tightness`). Consumers who accessed fields via `row.labor_context.industry_hiring_trend` on screener rows must switch to `row.labor_context.summary.industry_hiring_trend`. Fields formerly under `LaborContextSummary` that mapped to raw sub-objects (industry employment/wage, local unemployment/labor force) are accessible via the existing `LaborContext` sub-objects (`labor_context.industry.*`, `labor_context.local_market.*`).
- `client.insider_trades.list()` and `.list_all()` default behaviour changed: rows are now aggregated transaction events (one row per person/date/type/security/ownership 5-part key) instead of per-slice Form 4 rows. New fields `price_range` (min/max across slices), `slice_count`, and weighted-average `price_per_share` appear on each aggregate row. Consumers relying on per-slice rows must pass `flat=True` to preserve pre-T5 behaviour.

### Added
- `CompanyListItem.detail_url` — typed `str` field pointing at `/companies/{cik}` (from the post-S4 HATEOAS additions).
- `EnrichedCompanyData` response now carries 11 absolute `*_url` HATEOAS fields (`filings_url`, `financials_url`, `ratios_url`, `events_url`, `insider_trades_url`, `insider_holdings_url`, `holders_url`, `compensation_url`, `board_url`, `proxy_votes_url`, `beneficial_ownership_url`). Accessed via typed attributes where codegen emitted them, or via `.model_extra` for the envelope passthrough path.
- `client.financials.get(cik, per_page=N)` returns up to N most-recent financial statements in one call (e.g. `per_page=5` for a 5-year trend). Response is `PaginatedResponse[FinancialStatementListItem]`; omit `per_page` to keep the pre-IFRS-09 single-statement shape. Mutually exclusive with `year` and `quarter`.
- `client.financials.get(cik, statement="all")` returns all three statements (income, balance_sheet, cash_flow) in one call under a `MultiStatementResponse` shape. Combined with `per_page=N`, returns N periods each with all three statements. Enrichment (`labor_context` / `lending_context`) sits at envelope root on the paginated shape, NOT per-element — reflecting the current-snapshot semantics of the enrichment builders.
- `LaborContext.summary` — typed sub-object with 4 derived classification fields (`industry_hiring_trend`, `local_unemployment_trend`, `comp_to_market_ratio`, `labour_market_tightness`).
- `LaborContext.data_freshness` — typed sub-object with 6 period anchors (`ces_period`, `qcew_period`, `jolts_period`, `laus_period`, `oews_period`, `sec_exec_comp_snapshot_date`).
- `/companies/{cik}?include=labor_context` now returns a populated `compensation_benchmark` sub-object (API-side bug fix where this was previously omitted).
- `HolderListItem` and `FundHoldingListItem` now include `report_quarter` (e.g. `"2025-Q3"`) and `filed_at` (UTC `datetime`) fields on every row. Consumers rendering 13F data now have the temporal anchors needed to display "as-of" dates without an extra lookup.
- `flat: bool = False` kwarg on `InsiderTrades.list()` and `.list_all()`. `flat=True` returns per-slice Form 4 rows; default returns aggregated transaction events.
- `min_value: int | None` kwarg on both insider-trades methods (aggregate-mode filter on the post-SUM total; flat-mode filter per-slice — matches API semantics).
- `person: str | None` and `trade_type: str | None` kwargs on `InsiderTrades.list_all()` for signature parity with `list()`.
- New typed models: `InsiderTradeAggregateListItem`, `PriceRange`, `EnrichmentWarning`, `MultiStatementResponse`, `FinancialStatementBody`, `MultiStatementListItem`, `EnrichedMultiStatementResponse`, `EnrichedMultiStatementPaginatedResponse`, `FinancialStatementListItem`.
- `_enrichment_warnings` envelope field (typed via `EnrichmentWarning`: `field`, `reason`, `message`) on 5 response envelopes (companies detail, financials single-statement, financials `statement=all`, financials `statement=all` paginated, compensation). Accessible via `response.model_extra.get("_enrichment_warnings")` on the passthrough-typed envelopes. A 2-second hard timeout protects financials + companies endpoints from slow BLS / SBA queries; customers see `labor_context: null` + a typed warning instead of a 500.
- `client.companies.get(cik, include="...")` accepts 9 values (up from 2): `labor_context`, `lending_context`, `financials`, `ratios`, `events`, `insider_trades`, `holders`, `compensation`, `board`. Composing a company-detail page now takes one API call instead of eight — concurrent fan-out, 2-3s per-expander timeout, partial-failure typed error slots. `events` ships disabled in this release pending API-side performance work; `include="events"` (alone or in combination) returns `BadRequestError`. A follow-up SDK release will enable the expander.

### Changed
- All URL-carrying response fields are now returned as absolute URLs (`https://api.thesma.dev/...` in prod) driven by the `API_PUBLIC_BASE_URL` env var on the API side. Consumers can follow HATEOAS links directly with `httpx.get(resp.data.financials_url)` without manual base-URL joining.
- `EnrichedCompanyData` envelope carries inline sub-resource payloads (not just URL links) when expanders are requested via `include=...`. Non-requested slots continue to carry `*_url` HATEOAS links per the S4 convention.

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
