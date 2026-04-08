**Status:** Ready
**Depends on:** None
**Target repo:** thesma-python

---

## Section 1+2: Technical Spec

### Context

The MCP tool layer reported three missing query parameters where the SDK does not expose filtering that the underlying REST API supports. The MCP TL had to fall back to raw `client.request()` calls to work around the gaps.

The three issues:

1. **`Holdings.funds()` missing `search`** -- The API supports `GET /v1/us/sec/funds?search=...` for name-based fund lookup, but the SDK method only accepted `page` and `per_page`.
2. **`Events.list()` / `Events.list_all()` missing `to_date`** -- Only `from_date` was exposed. The API accepts a `to` query parameter for end-date filtering, but the SDK silently dropped it.
3. **`InsiderTrades.list()` / `InsiderTrades.list_all()` missing `to_date`** -- Same pattern as events. No end-date filtering available through the SDK.

Impact: MCP tools that accept `to_date` or `search` parameters could not pass them through the SDK. Users calling the SDK directly also lacked these filters.

### Root cause

When the resource methods were originally written, the `search` parameter on the funds endpoint and the `to` parameter on events/insider-trades endpoints were not included in the method signatures or `params` dicts. The API supported them, but the SDK never wired them through.

The omission was consistent across all three layers (resource method, CLI command, tests) -- this wasn't a wiring bug, the parameters were simply never added.

### Fix

Add the missing parameters at every layer. No behavioral change to existing calls -- all new parameters are optional with `None` defaults and are stripped from the query string when not provided (existing `_strip_none` behavior).

| Endpoint | Parameter | Query key | Type |
|---|---|---|---|
| `Holdings.funds()` | `search` | `?search=` | `str \| None` |
| `Events.list()` | `to_date` | `?to=` | `str \| None` |
| `Events.list_all()` | `to_date` | `?to=` | `str \| None` |
| `InsiderTrades.list()` | `to_date` | `?to=` | `str \| None` |
| `InsiderTrades.list_all()` | `to_date` | `?to=` | `str \| None` |

Before: `client.holdings.funds()` -- only pagination.
After: `client.holdings.funds(search="Vanguard")` -- filters by name.

Before: `client.events.list(cik, from_date="2024-01-01")` -- no end-date bound.
After: `client.events.list(cik, from_date="2024-01-01", to_date="2024-12-31")` -- bounded range.

### Modifies

**Resource methods (add parameter to signature + params dict):**

- `src/thesma/resources/holdings.py` -- Add `search: str | None = None` to `funds()`, include `"search": search` in params dict.
- `src/thesma/resources/events.py` -- Add `to_date: str | None = None` to both `list()` and `list_all()`, include `"to": to_date` in params dict.
- `src/thesma/resources/insider_trades.py` -- Add `to_date: str | None = None` to both `list()` and `list_all()`, include `"to": to_date` in params dict.

**CLI commands (add `--to` / `--search` Click options, pass through to SDK):**

- `src/thesma/cli/commands/events.py` -- Add `@click.option("--to", "to_date", ...)` to `events_list`, pass `to_date=to_date` to `client.events.list()`.
- `src/thesma/cli/commands/insider_trades.py` -- Add `@click.option("--to", "to_date", ...)` to `insider_trades_list`, pass `to_date=to_date` to `client.insider_trades.list()`.

**Tests (add positive + negative test for each new parameter):**

- `tests/test_resources/test_holdings.py` -- Add `test_funds_with_search` (asserts `search=Vanguard` in URL) and `test_funds_none_search_omitted` (asserts `search=` not in URL) to `TestFunds`.
- `tests/test_resources/test_events.py` -- Add `test_list_with_to_date` and `test_list_none_to_date_omitted` to `TestEventsList`. Add `test_list_all_with_to_date` and `test_list_all_none_to_date_omitted` to `TestEventsListAll`.
- `tests/test_resources/test_insider_trades.py` -- Add `test_list_with_to_date` and `test_list_none_to_date_omitted` to `TestInsiderTradesList`. Add `test_list_all_with_to_date` and `test_list_all_none_to_date_omitted` to `TestInsiderTradesListAll`.
- `tests/test_cli.py` -- Add `test_insider_trades_list_to_flag` to `TestInsiderTradesCli`. Add `test_events_list_to_flag` to `TestEventsCli`.

### Do NOT change

- `src/thesma/_generated/models.py` -- Auto-generated, never hand-edit.
- Any existing method signatures or default values -- all changes are additive.
- The `list_all` CLI commands for events and insider trades -- these do not exist as CLI commands (only `list` is exposed via CLI).
- The existing `holdings funds` CLI subcommand (`src/thesma/cli/commands/holdings.py`) -- the `--search` option is not added to this CLI command in this change; only the resource method is extended.

### Architecture decisions

None. Pure additive parameter pass-through following the established pattern: keyword-only arg with `None` default -> included in `params` dict -> stripped by `_strip_none` when `None`.

---

## Section 3: Verification Spec

### Unit tests

**Resource-level tests** (respx mocks, assert query string contains/omits parameter):

Each new parameter gets two tests following the existing pattern:
- **Positive**: Call with the parameter set, assert the query string contains it (e.g., `assert "search=Vanguard" in str(request.url)`).
- **Negative**: Call without the parameter, assert the query string does not contain it (e.g., `assert "search=" not in str(request.url)`).

| File | Class | New tests |
|---|---|---|
| `tests/test_resources/test_holdings.py` | `TestFunds` | `test_funds_with_search`, `test_funds_none_search_omitted` |
| `tests/test_resources/test_events.py` | `TestEventsList` | `test_list_with_to_date`, `test_list_none_to_date_omitted` |
| `tests/test_resources/test_events.py` | `TestEventsListAll` | `test_list_all_with_to_date`, `test_list_all_none_to_date_omitted` |
| `tests/test_resources/test_insider_trades.py` | `TestInsiderTradesList` | `test_list_with_to_date`, `test_list_none_to_date_omitted` |
| `tests/test_resources/test_insider_trades.py` | `TestInsiderTradesListAll` | `test_list_all_with_to_date`, `test_list_all_none_to_date_omitted` |

**CLI-level tests** (MagicMock client, assert kwargs passed through):

| File | Class | New tests |
|---|---|---|
| `tests/test_cli.py` | `TestInsiderTradesCli` | `test_insider_trades_list_to_flag` |
| `tests/test_cli.py` | `TestEventsCli` | `test_events_list_to_flag` |

### Regression tests

The positive/negative pairs above serve as regression tests. They verify:
- New parameters reach the API when provided.
- New parameters are omitted from the query string when `None` (no accidental `?to=None` pollution).
- Existing parameters (`from_date`, `category`, `person`, `page`, `per_page`) are unaffected -- covered by pre-existing tests that continue to pass.

### Integration tests

Not applicable. No API contract changes -- the REST API already supports these parameters. The SDK is catching up.

### Production verification

After release:
1. `client.holdings.funds(search="Vanguard")` returns filtered fund list (not full paginated list).
2. `client.events.list("0000320193", from_date="2024-01-01", to_date="2024-06-30")` returns only events within the date range.
3. `client.insider_trades.list("0000320193", to_date="2024-12-31")` returns only trades on or before the end date.
4. CLI: `thesma events list 0000320193 --from 2024-01-01 --to 2024-06-30` produces bounded results.
5. CLI: `thesma insider-trades list 0000320193 --to 2024-12-31` produces bounded results.

### Test data notes

All resource tests reuse the existing fixture JSON constants (`PAGINATED_FUNDS_JSON`, `PAGINATED_EVENTS_JSON`, `PAGINATED_TRADES_JSON`). No new fixtures needed -- the tests only verify query string construction, not response parsing.
