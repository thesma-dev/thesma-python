# Thesma Python SDK

[![PyPI version](https://img.shields.io/pypi/v/thesma.svg)](https://pypi.org/project/thesma/)
[![CI](https://github.com/thesma-dev/thesma-python/actions/workflows/ci.yml/badge.svg)](https://github.com/thesma-dev/thesma-python/actions/workflows/ci.yml)

Python SDK for the [Thesma API](https://thesma.dev) -- developer-friendly access to SEC EDGAR financial data.

## Installation

```bash
pip install thesma
```

## Quickstart

```python
from thesma import ThesmaClient

client = ThesmaClient(api_key="th_live_...")

# List companies
companies = client.companies.list()
for company in companies:
    print(company.ticker, company.name)

# Get financial statements
financials = client.financials.list("AAPL", period="annual")
```

## Async usage

```python
from thesma import AsyncThesmaClient

async with AsyncThesmaClient(api_key="th_live_...") as client:
    companies = await client.companies.list()
```

## CLI

```bash
export THESMA_API_KEY=th_live_...
thesma companies list
thesma financials list AAPL --period annual --format json
```

## Documentation

Full API documentation is available at [api.thesma.dev/docs](https://api.thesma.dev/docs).

## License

MIT
