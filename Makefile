.PHONY: install test lint typecheck format regenerate

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

typecheck:
	mypy src/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

regenerate:
	python scripts/regenerate.py
