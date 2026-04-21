# Thesma Python SDK

[![PyPI version](https://img.shields.io/pypi/v/thesma.svg)](https://pypi.org/project/thesma/)
[![CI](https://github.com/thesma-dev/thesma-python/actions/workflows/ci.yml/badge.svg)](https://github.com/thesma-dev/thesma-python/actions/workflows/ci.yml)

Python SDK for the [Thesma API](https://thesma.dev) -- developer-friendly access to US-GAAP and IFRS financial data from SEC EDGAR — every US-listed public company on NYSE and NASDAQ.

## Installation

```bash
pip install thesma
```

## Quickstart

```python
from thesma import ThesmaClient

client = ThesmaClient(api_key="gd_live_...")

# List companies
companies = client.companies.list()
for company in companies:
    print(company.ticker, company.name)

# Get financial statements
financials = client.financials.get("0000320193", statement="income", period="annual")
```

```python
# IFRS filers return native-currency financials with taxonomy metadata
spot = client.financials.get("0001639920", statement="income", period="annual")
print(spot.data.taxonomy)                         # "ifrs-full"
print(spot.data.currency)                         # "EUR"
print(spot.data.reporting_notes.presentation_format)  # "by_nature" | "by_function" | "unknown"
print(spot.data.reporting_notes.ifrs_18_applied)  # True | False
```

## Async usage

```python
from thesma import AsyncThesmaClient

async with AsyncThesmaClient(api_key="gd_live_...") as client:
    companies = await client.companies.list()
```

## CLI

```bash
export THESMA_API_KEY=gd_live_...
thesma companies list
thesma financials list AAPL --period annual --format json
```

## Typed responses

`response.taxonomy`, `response.currency`, and `response.reporting_notes.presentation_format`
are declared as typed attributes on `FinancialStatementResponse` — callers get IDE
autocomplete and mypy-checked access instead of reaching into `.model_extra` for
the IFRS-01 fields.

## Documentation

Full documentation is available at [docs.thesma.dev](https://docs.thesma.dev).

## License

MIT
