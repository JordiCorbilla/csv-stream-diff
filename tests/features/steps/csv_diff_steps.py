from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import yaml
from behave import given, then, when

from csvstreamdiff.comparer import compare_from_path


def table_to_rows(table) -> list[dict[str, str]]:  # type: ignore[no-untyped-def]
    return [dict(row.items()) for row in table]


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@given("a comparison workspace")
def step_workspace(context) -> None:  # type: ignore[no-untyped-def]
    context.temp_dir = tempfile.mkdtemp(prefix="csv_stream_diff_behave_")
    context.left_path = Path(context.temp_dir) / "left.csv"
    context.right_path = Path(context.temp_dir) / "right.csv"
    context.config_path = Path(context.temp_dir) / "config.yaml"
    context.output_dir = Path(context.temp_dir) / "output"
    context.sampling_size = 0
    context.sampling_seed = 123


@given("the left CSV contains")
def step_left_csv(context) -> None:  # type: ignore[no-untyped-def]
    rows = table_to_rows(context.table)
    write_csv(context.left_path, list(context.table.headings), rows)


@given("the right CSV contains")
def step_right_csv(context) -> None:  # type: ignore[no-untyped-def]
    rows = table_to_rows(context.table)
    write_csv(context.right_path, list(context.table.headings), rows)


@given('sampling size is {size:d} with seed {seed:d}')
def step_sampling(context, size: int, seed: int) -> None:  # type: ignore[no-untyped-def]
    context.sampling_size = size
    context.sampling_seed = seed


@given("the default comparison config")
def step_default_config(context) -> None:  # type: ignore[no-untyped-def]
    config = {
        "files": {"left": str(context.left_path), "right": str(context.right_path)},
        "keys": {"left": ["customer_id", "transaction_date"], "right": ["cust_id", "txn_dt"]},
        "compare": {
            "left": ["amount", "status", "description"],
            "right": ["transaction_amount", "txn_status", "desc"],
        },
        "comparison": {
            "case_insensitive": False,
            "trim_whitespace": True,
            "treat_null_as_equal": True,
            "normalize_numeric_values": True,
            "treat_null_as_zero_for_numeric": True,
        },
        "sampling": {"size": context.sampling_size, "seed": context.sampling_seed},
        "performance": {
            "chunk_size": 2,
            "workers": 1,
            "bucket_count": 4,
            "report_every_rows": 1,
            "temp_directory": str(Path(context.temp_dir) / "tmp"),
            "keep_temp_files": False,
            "show_progress": False,
        },
        "output": {
            "directory": str(context.output_dir),
            "prefix": "behave_",
            "include_full_rows": False,
            "summary_format": "both",
        },
    }
    context.config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


@when("I run the comparison")
def step_run_comparison(context) -> None:  # type: ignore[no-untyped-def]
    context.summary = compare_from_path(context.config_path)


@then('the summary count "{name}" is {value:d}')
def step_summary_count(context, name: str, value: int) -> None:  # type: ignore[no-untyped-def]
    assert context.summary["counts"][name] == value


@then('the sampling field "{name}" is {value:d}')
def step_sampling_field(context, name: str, value: int) -> None:  # type: ignore[no-untyped-def]
    assert context.summary["sampling"][name] == value


@then('the output file "{name}" contains')
def step_output_file_contains(context, name: str) -> None:  # type: ignore[no-untyped-def]
    rows = read_csv(Path(context.summary["outputs"][name]))
    expected = table_to_rows(context.table)
    headings = list(context.table.headings)
    projected_rows = [{heading: row.get(heading, "") for heading in headings} for row in rows]
    assert projected_rows == expected


@then('the summary warning contains "{text}"')
def step_summary_warning_contains(context, text: str) -> None:  # type: ignore[no-untyped-def]
    assert any(text in warning for warning in context.summary["warnings"])
