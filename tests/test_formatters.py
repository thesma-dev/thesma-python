"""Tests for CLI output formatters."""

from __future__ import annotations

import csv
import io
import json

from pydantic import BaseModel

from thesma.cli._formatters import format_csv, format_json, format_table


class _SampleModel(BaseModel):
    name: str
    value: int


class TestFormatJson:
    def test_single_pydantic_model(self) -> None:
        model = _SampleModel(name="Acme", value=42)
        result = format_json(model)
        parsed = json.loads(result)
        assert parsed == {"name": "Acme", "value": 42}

    def test_list_of_pydantic_models(self) -> None:
        models = [_SampleModel(name="A", value=1), _SampleModel(name="B", value=2)]
        result = format_json(models)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "A"
        assert parsed[1]["value"] == 2

    def test_list_of_dicts(self) -> None:
        data = [{"x": 1}, {"x": 2}]
        result = format_json(data)
        parsed = json.loads(result)
        assert parsed == [{"x": 1}, {"x": 2}]

    def test_empty_list(self) -> None:
        result = format_json([])
        parsed = json.loads(result)
        assert parsed == []

    def test_indent_is_two(self) -> None:
        result = format_json({"a": 1})
        assert "  " in result  # indent=2


class TestFormatCsv:
    def test_produces_correct_headers_and_rows(self) -> None:
        data = [_SampleModel(name="Acme", value=42)]
        columns = ("name", "value")
        result = format_csv(data, columns)
        reader = csv.DictReader(io.StringIO(result))
        assert reader.fieldnames == ["name", "value"]
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["name"] == "Acme"
        assert rows[0]["value"] == "42"

    def test_multiple_rows(self) -> None:
        data = [
            _SampleModel(name="A", value=1),
            _SampleModel(name="B", value=2),
        ]
        result = format_csv(data, ("name", "value"))
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2

    def test_dict_data(self) -> None:
        data = [{"name": "X", "value": 99, "extra": "ignored"}]
        result = format_csv(data, ("name", "value"))
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows[0]["name"] == "X"
        assert "extra" not in reader.fieldnames  # type: ignore[operator]

    def test_empty_data_produces_headers_only(self) -> None:
        result = format_csv([], ("name", "value"))
        lines = result.strip().split("\n")
        assert len(lines) == 1
        assert "name" in lines[0]
        assert "value" in lines[0]

    def test_uses_unix_line_endings(self) -> None:
        data = [_SampleModel(name="A", value=1)]
        result = format_csv(data, ("name", "value"))
        assert "\r\n" not in result
        assert "\n" in result


class TestFormatTable:
    def test_produces_readable_output(self) -> None:
        data = [_SampleModel(name="Acme", value=42)]
        result = format_table(data, ("name", "value"))
        assert "name" in result
        assert "value" in result
        assert "Acme" in result
        assert "42" in result

    def test_multiple_rows(self) -> None:
        data = [
            _SampleModel(name="A", value=1),
            _SampleModel(name="B", value=2),
        ]
        result = format_table(data, ("name", "value"))
        assert "A" in result
        assert "B" in result

    def test_empty_data(self) -> None:
        result = format_table([], ("name", "value"))
        # tabulate with empty data still includes headers
        assert "name" in result
        assert "value" in result

    def test_dict_data(self) -> None:
        data = [{"name": "X", "value": 99}]
        result = format_table(data, ("name", "value"))
        assert "X" in result
        assert "99" in result
