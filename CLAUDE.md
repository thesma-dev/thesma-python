# CLAUDE.md

## Project

Thesma Python SDK -- typed Python client for the Thesma REST API (SEC EDGAR financial data).

## Stack

- Python 3.9+, httpx, Pydantic v2, Click, Hatch (PEP 621)
- `from __future__ import annotations` in every module

## Key directories

| Directory | Purpose |
|---|---|
| `src/thesma/` | SDK package (client, resources, CLI) |
| `src/thesma/_generated/` | Auto-generated Pydantic models (DO NOT EDIT) |
| `src/thesma/resources/` | Resource namespaces (companies, filings, etc.) |
| `src/thesma/cli/` | Click CLI entry point and commands |
| `tests/` | pytest test suite |
| `scripts/` | Development scripts (model regeneration) |

## Development

```bash
python -m venv .venv
source .venv/bin/activate
make install       # pip install -e ".[dev]"
make lint          # ruff check + format --check
make typecheck     # mypy src/
make test          # pytest
make format        # ruff format + check --fix
```

## Conventions

- Ruff for linting and formatting (line length 120)
- mypy strict mode
- `src/` layout -- all imports start with `thesma.`
- `from __future__ import annotations` in every `.py` file
- Generated code in `_generated/` is excluded from linting

## Version

- 4-part version: `{API_MAJOR}.{API_MINOR}.{API_PATCH}.{SDK_BUILD}`
- Read from `importlib.metadata` at runtime (`src/thesma/_version.py`)
- Canonical source: `pyproject.toml` `[project] version`

## Test conventions

- Framework: pytest + pytest-asyncio (asyncio mode: auto)
- HTTP mocking: respx
- Test files: `tests/test_<module>.py`

## Canonical files

- **Client**: `src/thesma/__init__.py` (stubs, replaced in SDK-03)
- **CLI entry**: `src/thesma/cli/main.py`
- **Generated models**: `src/thesma/_generated/models.py`
- **Version**: `src/thesma/_version.py`

## Pre-handoff checklist

```bash
ruff check src/ tests/
ruff format --check src/ tests/
mypy src/
pytest
```

## CI

GitHub Actions (`.github/workflows/ci.yml`):

1. **lint** -- ruff check + format --check
2. **type-check** -- mypy src/
3. **test** -- pytest across Python 3.9, 3.12, 3.13
