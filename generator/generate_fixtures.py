from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
from typing import Any

import yaml


LEFT_HEADERS = ["customer_id", "transaction_date", "amount", "status", "description"]
RIGHT_HEADERS = ["cust_id", "txn_dt", "transaction_amount", "txn_status", "desc"]


def build_left_row(index: int) -> dict[str, str]:
    return {
        "customer_id": f"C{index:07d}",
        "transaction_date": f"2026-01-{(index % 28) + 1:02d}",
        "amount": f"{100 + index * 1.17:.2f}",
        "status": "OPEN" if index % 2 == 0 else "CLOSED",
        "description": f"transaction-{index}",
    }


def map_to_right_row(left_row: dict[str, str]) -> dict[str, str]:
    return {
        "cust_id": left_row["customer_id"],
        "txn_dt": left_row["transaction_date"],
        "transaction_amount": left_row["amount"],
        "txn_status": left_row["status"],
        "desc": left_row["description"],
    }


def write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def choose_distinct_indices(rng: random.Random, population_size: int, count: int, excluded: set[int]) -> list[int]:
    available = [index for index in range(population_size) if index not in excluded]
    count = min(count, len(available))
    selected = rng.sample(available, count)
    excluded.update(selected)
    return selected


def build_expected_manifest(
    *,
    changed_keys: list[str],
    left_only_keys: list[str],
    right_only_keys: list[str],
    duplicate_left_keys: list[str],
    duplicate_right_keys: list[str],
) -> dict[str, Any]:
    return {
        "counts": {
            "only_in_left": len(left_only_keys),
            "only_in_right": len(right_only_keys),
            "different_rows": len(changed_keys),
            "duplicate_keys_left": len(duplicate_left_keys),
            "duplicate_keys_right": len(duplicate_right_keys),
        },
        "keys": {
            "changed": changed_keys,
            "only_in_left": left_only_keys,
            "only_in_right": right_only_keys,
            "duplicate_left": duplicate_left_keys,
            "duplicate_right": duplicate_right_keys,
        },
    }


def generate_fixtures(
    *,
    output_dir: Path,
    rows: int,
    seed: int,
    changed: int,
    left_only: int,
    right_only: int,
    duplicate_left: int,
    duplicate_right: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    left_rows = [build_left_row(index) for index in range(1, rows + 1)]
    right_rows = [map_to_right_row(row) for row in left_rows]

    occupied: set[int] = set()
    left_only_indices = choose_distinct_indices(rng, rows, left_only, occupied)
    changed_indices = choose_distinct_indices(rng, rows, changed, occupied)
    duplicate_left_indices = choose_distinct_indices(rng, rows, duplicate_left, occupied)
    duplicate_right_indices = choose_distinct_indices(rng, rows, duplicate_right, occupied)

    left_only_keys = [left_rows[index]["customer_id"] for index in left_only_indices]
    changed_keys = [left_rows[index]["customer_id"] for index in changed_indices]
    duplicate_left_keys = [left_rows[index]["customer_id"] for index in duplicate_left_indices]
    duplicate_right_keys = [left_rows[index]["customer_id"] for index in duplicate_right_indices]

    for index in changed_indices:
        right_rows[index]["transaction_amount"] = f"{float(right_rows[index]['transaction_amount']) + 25.50:.2f}"
        right_rows[index]["desc"] = right_rows[index]["desc"] + "-updated"

    duplicate_right_rows = [dict(right_rows[index]) for index in duplicate_right_indices]

    for index in sorted(left_only_indices, reverse=True):
        del right_rows[index]

    right_only_keys: list[str] = []
    for extra_index in range(1, right_only + 1):
        left_row = build_left_row(rows + extra_index)
        right_row = map_to_right_row(left_row)
        right_rows.append(right_row)
        right_only_keys.append(left_row["customer_id"])

    for index in duplicate_left_indices:
        left_rows.append(dict(left_rows[index]))
    right_rows.extend(duplicate_right_rows)

    left_path = output_dir / "left.csv"
    right_path = output_dir / "right.csv"
    config_path = output_dir / "config.generated.yaml"
    expected_path = output_dir / "expected.json"

    write_csv(left_path, LEFT_HEADERS, left_rows)
    write_csv(right_path, RIGHT_HEADERS, right_rows)

    config = {
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
        "sampling": {"size": 0, "seed": seed},
        "performance": {
            "chunk_size": 1000,
            "workers": 1,
            "bucket_count": 8,
            "report_every_rows": 500,
            "temp_directory": str(output_dir / "tmp"),
            "keep_temp_files": False,
            "show_progress": False,
        },
        "output": {
            "directory": str(output_dir / "output"),
            "prefix": "generated_",
            "include_full_rows": True,
            "summary_format": "both",
        },
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    expected = build_expected_manifest(
        changed_keys=changed_keys,
        left_only_keys=left_only_keys,
        right_only_keys=right_only_keys,
        duplicate_left_keys=duplicate_left_keys,
        duplicate_right_keys=duplicate_right_keys,
    )
    expected_path.write_text(json.dumps(expected, indent=2), encoding="utf-8")

    return {
        "left": str(left_path),
        "right": str(right_path),
        "config": str(config_path),
        "expected": str(expected_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate CSV fixtures for csv-stream-diff.")
    parser.add_argument("--output-dir", required=True, help="Directory where the generator writes its artifacts.")
    parser.add_argument("--rows", type=int, default=1000, help="Number of baseline rows to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed used for mutation selection.")
    parser.add_argument("--changed", type=int, default=5, help="Number of existing rows to change on the right side.")
    parser.add_argument("--left-only", type=int, default=3, help="Number of left rows removed from the right file.")
    parser.add_argument("--right-only", type=int, default=3, help="Number of extra rows added to the right file.")
    parser.add_argument("--duplicate-left", type=int, default=2, help="Number of duplicate rows appended to the left file.")
    parser.add_argument("--duplicate-right", type=int, default=2, help="Number of duplicate rows appended to the right file.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    artifacts = generate_fixtures(
        output_dir=Path(args.output_dir),
        rows=args.rows,
        seed=args.seed,
        changed=args.changed,
        left_only=args.left_only,
        right_only=args.right_only,
        duplicate_left=args.duplicate_left,
        duplicate_right=args.duplicate_right,
    )
    print(json.dumps(artifacts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
