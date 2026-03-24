from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from csvstreamdiff.comparer import compare_csv_files


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_config(tmp_path: Path, left_path: Path, right_path: Path, *, sample_size: int = 0, seed: int = 123) -> dict:
    return {
        "files": {"left": str(left_path), "right": str(right_path)},
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
        "sampling": {"size": sample_size, "seed": seed},
        "performance": {
            "chunk_size": 2,
            "workers": 1,
            "bucket_count": 4,
            "report_every_rows": 1,
            "temp_directory": str(tmp_path / "tmp"),
            "keep_temp_files": False,
            "show_progress": False,
        },
        "output": {
            "directory": str(tmp_path / "output"),
            "prefix": "test_",
            "include_full_rows": True,
            "summary_format": "both",
        },
    }


def test_compare_csv_files_reports_differences_and_presence_gaps(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    write_csv(
        left_path,
        ["customer_id", "transaction_date", "amount", "status", "description"],
        [
            {"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "10.00", "status": "OPEN", "description": "alpha"},
            {"customer_id": "C2", "transaction_date": "2026-01-02", "amount": "20.00", "status": "OPEN", "description": "beta"},
            {"customer_id": "C3", "transaction_date": "2026-01-03", "amount": "30.00", "status": "CLOSED", "description": "gamma"},
        ],
    )
    write_csv(
        right_path,
        ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"],
        [
            {"cust_id": "C1", "txn_dt": "2026-01-01", "transaction_amount": "10.00", "txn_status": "OPEN", "desc": "alpha"},
            {"cust_id": "C2", "txn_dt": "2026-01-02", "transaction_amount": "25.00", "txn_status": "OPEN", "desc": "beta changed"},
            {"cust_id": "C4", "txn_dt": "2026-01-04", "transaction_amount": "40.00", "txn_status": "OPEN", "desc": "delta"},
        ],
    )

    summary = compare_csv_files(build_config(tmp_path, left_path, right_path))

    assert summary["counts"]["only_in_left"] == 1
    assert summary["counts"]["only_in_right"] == 1
    assert summary["counts"]["different_rows"] == 1
    assert summary["counts"]["different_cells"] == 2

    diff_rows = read_csv(Path(summary["outputs"]["differences"]))
    assert len(diff_rows) == 1
    assert diff_rows[0]["difference_count"] == "2"
    assert "amount/transaction_amount" in diff_rows[0]["differences_text"]
    difference_items = json.loads(diff_rows[0]["differences_json"])
    assert {item["left_column"] for item in difference_items} == {"amount", "description"}


def test_compare_csv_files_sampling_is_exact_and_reproducible(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    left_rows = []
    right_rows = []
    for index in range(1, 11):
        left_rows.append(
            {
                "customer_id": f"C{index}",
                "transaction_date": f"2026-01-{index:02d}",
                "amount": f"{index}.00",
                "status": "OPEN",
                "description": f"row-{index}",
            }
        )
        right_rows.append(
            {
                "cust_id": f"C{index}",
                "txn_dt": f"2026-01-{index:02d}",
                "transaction_amount": f"{index}.00",
                "txn_status": "OPEN",
                "desc": f"row-{index}",
            }
        )
    write_csv(left_path, ["customer_id", "transaction_date", "amount", "status", "description"], left_rows)
    write_csv(right_path, ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"], right_rows)

    config = build_config(tmp_path, left_path, right_path, sample_size=4, seed=999)
    first = compare_csv_files(config)

    config["output"]["directory"] = str(tmp_path / "output-2")
    config["performance"]["temp_directory"] = str(tmp_path / "tmp-2")
    second = compare_csv_files(config)

    assert first["sampling"]["actual_size"] == 4
    assert second["sampling"]["actual_size"] == 4
    assert first["counts"]["matched"] == 4
    assert second["counts"]["matched"] == 4


def test_duplicate_keys_use_first_occurrence(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    write_csv(
        left_path,
        ["customer_id", "transaction_date", "amount", "status", "description"],
        [
            {"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "10.00", "status": "OPEN", "description": "first"},
            {"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "99.00", "status": "OPEN", "description": "duplicate"},
        ],
    )
    write_csv(
        right_path,
        ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"],
        [
            {"cust_id": "C1", "txn_dt": "2026-01-01", "transaction_amount": "10.00", "txn_status": "OPEN", "desc": "first"},
        ],
    )

    summary = compare_csv_files(build_config(tmp_path, left_path, right_path))

    assert summary["counts"]["duplicate_keys_left"] == 1
    assert summary["counts"]["different_rows"] == 0
    assert any("first occurrence" in warning for warning in summary["warnings"])


def test_numeric_and_null_equivalence_avoids_false_discrepancies(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    write_csv(
        left_path,
        ["customer_id", "transaction_date", "amount", "status", "description"],
        [
            {"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "0", "status": "OPEN", "description": ""},
            {"customer_id": "C2", "transaction_date": "2026-01-02", "amount": "0", "status": "OPEN", "description": "NULL"},
        ],
    )
    write_csv(
        right_path,
        ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"],
        [
            {"cust_id": "C1", "txn_dt": "2026-01-01", "transaction_amount": "0.000000000", "txn_status": "OPEN", "desc": ""},
            {"cust_id": "C2", "txn_dt": "2026-01-02", "transaction_amount": "", "txn_status": "OPEN", "desc": ""},
        ],
    )

    summary = compare_csv_files(build_config(tmp_path, left_path, right_path))

    assert summary["counts"]["different_rows"] == 0
    assert summary["counts"]["different_cells"] == 0


def test_compare_csv_files_runs_with_multiple_workers(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    write_csv(
        left_path,
        ["customer_id", "transaction_date", "amount", "status", "description"],
        [
            {"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "10.00", "status": "OPEN", "description": "alpha"},
            {"customer_id": "C2", "transaction_date": "2026-01-02", "amount": "20.00", "status": "OPEN", "description": "beta"},
            {"customer_id": "C3", "transaction_date": "2026-01-03", "amount": "30.00", "status": "OPEN", "description": "gamma"},
            {"customer_id": "C4", "transaction_date": "2026-01-04", "amount": "40.00", "status": "OPEN", "description": "delta"},
        ],
    )
    write_csv(
        right_path,
        ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"],
        [
            {"cust_id": "C1", "txn_dt": "2026-01-01", "transaction_amount": "10.00", "txn_status": "OPEN", "desc": "alpha"},
            {"cust_id": "C2", "txn_dt": "2026-01-02", "transaction_amount": "22.00", "txn_status": "OPEN", "desc": "beta"},
            {"cust_id": "C4", "txn_dt": "2026-01-04", "transaction_amount": "40.00", "txn_status": "OPEN", "desc": "delta"},
            {"cust_id": "C5", "txn_dt": "2026-01-05", "transaction_amount": "50.00", "txn_status": "OPEN", "desc": "epsilon"},
        ],
    )

    config = build_config(tmp_path, left_path, right_path)
    config["performance"]["workers"] = 2
    config["performance"]["bucket_count"] = 8
    summary = compare_csv_files(config)

    assert summary["counts"]["only_in_left"] == 1
    assert summary["counts"]["only_in_right"] == 1
    assert summary["counts"]["different_rows"] == 1


def test_invalid_config_raises_on_compare_length_mismatch(tmp_path) -> None:
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    write_csv(left_path, ["customer_id", "transaction_date", "amount"], [{"customer_id": "C1", "transaction_date": "2026-01-01", "amount": "1.00"}])
    write_csv(right_path, ["cust_id", "txn_dt", "transaction_amount"], [{"cust_id": "C1", "txn_dt": "2026-01-01", "transaction_amount": "1.00"}])

    config = build_config(tmp_path, left_path, right_path)
    config["compare"]["right"] = ["transaction_amount", "txn_status"]

    with pytest.raises(ValueError, match="compare.left and compare.right"):
        compare_csv_files(config)
