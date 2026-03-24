from __future__ import annotations

import json
import multiprocessing as mp
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from .hashing import NormalizationSettings, NormalizedKey, stable_bucket_for_key
from .multiprocessing import (
    ProgressMonitor,
    cleanup_temp_dir,
    compare_bucket_worker,
    finish_progress_threads,
    partition_file,
    sample_left_keys,
    start_progress_threads,
)
from .streaming import CSVOptions, merge_csv_parts


DEFAULT_CONFIG: dict[str, Any] = {
    "csv": {
        "left": {
            "encoding": "utf-8-sig",
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": None,
            "newline": "",
        },
        "right": {
            "encoding": "utf-8-sig",
            "delimiter": ",",
            "quotechar": '"',
            "escapechar": None,
            "newline": "",
        },
    },
    "comparison": {
        "case_insensitive": False,
        "trim_whitespace": True,
        "treat_null_as_equal": True,
        "normalize_numeric_values": True,
        "treat_null_as_zero_for_numeric": True,
        "numeric_decimal_places": None,
        "normalize_boolean_values": True,
    },
    "sampling": {
        "size": 0,
        "seed": 12345,
    },
    "performance": {
        "chunk_size": 100000,
        "workers": None,
        "bucket_count": None,
        "report_every_rows": 50000,
        "temp_directory": None,
        "keep_temp_files": False,
        "show_progress": True,
    },
    "output": {
        "directory": "./output",
        "prefix": "comparison_",
        "include_full_rows": True,
        "summary_format": "both",
    },
}


@dataclass
class CompareStats:
    total_left_rows: int = 0
    total_right_rows: int = 0
    left_unique_keys: int = 0
    right_unique_keys: int = 0
    matched: int = 0
    only_in_left: int = 0
    only_in_right: int = 0
    different_rows: int = 0
    different_cells: int = 0
    duplicate_keys_left: int = 0
    duplicate_keys_right: int = 0
    sample_mode: str = "full"
    sample_requested_size: int = 0
    sample_actual_size: int = 0
    sample_seed: int = 0
    elapsed_seconds: float = 0.0
    warnings: list[str] = field(default_factory=list)
    outputs: dict[str, str] = field(default_factory=dict)


def load_config(config_path: str | Path) -> dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Configuration file must contain a YAML mapping at the top level")
    return data


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def normalize_compare_section(compare_section: Any) -> dict[str, list[str]]:
    if isinstance(compare_section, dict) and "left" in compare_section and "right" in compare_section:
        return {
            "left": list(compare_section["left"]),
            "right": list(compare_section["right"]),
        }
    if isinstance(compare_section, dict):
        return {
            "left": list(compare_section.keys()),
            "right": list(compare_section.values()),
        }
    raise ValueError("compare must be a mapping or a {left: [...], right: [...]} structure")


def normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    merged = deep_merge(DEFAULT_CONFIG, config)
    merged["compare"] = normalize_compare_section(merged.get("compare"))
    return merged


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    effective = normalize_config(config)

    for section in ("files", "keys", "compare"):
        if section not in effective:
            raise ValueError(f"Missing required configuration section: {section}")

    files = effective["files"]
    if "left" not in files or "right" not in files:
        raise ValueError("files.left and files.right are required")
    if not Path(files["left"]).exists():
        raise ValueError(f"Left CSV file does not exist: {files['left']}")
    if not Path(files["right"]).exists():
        raise ValueError(f"Right CSV file does not exist: {files['right']}")

    keys = effective["keys"]
    if len(keys.get("left", [])) == 0 or len(keys.get("right", [])) == 0:
        raise ValueError("keys.left and keys.right must each contain at least one column")
    if len(keys["left"]) != len(keys["right"]):
        raise ValueError("keys.left and keys.right must contain the same number of columns")

    compare = effective["compare"]
    if len(compare["left"]) == 0 or len(compare["right"]) == 0:
        raise ValueError("compare.left and compare.right must each contain at least one column")
    if len(compare["left"]) != len(compare["right"]):
        raise ValueError("compare.left and compare.right must contain the same number of columns")

    chunk_size = int(effective["performance"]["chunk_size"])
    if chunk_size <= 0:
        raise ValueError("performance.chunk_size must be greater than zero")
    effective["performance"]["chunk_size"] = chunk_size

    sample_size = int(effective["sampling"]["size"])
    if sample_size < 0:
        raise ValueError("sampling.size must be zero or greater")
    effective["sampling"]["size"] = sample_size
    effective["sampling"]["seed"] = int(effective["sampling"]["seed"])

    workers = effective["performance"]["workers"]
    effective["performance"]["workers"] = int(workers) if workers else max(1, os.cpu_count() or 1)
    bucket_count = effective["performance"]["bucket_count"]
    default_bucket_count = max(effective["performance"]["workers"] * 8, 64)
    effective["performance"]["bucket_count"] = int(bucket_count) if bucket_count else default_bucket_count
    if effective["performance"]["bucket_count"] <= 0:
        raise ValueError("performance.bucket_count must be greater than zero")

    effective["performance"]["report_every_rows"] = int(effective["performance"]["report_every_rows"])
    if effective["performance"]["report_every_rows"] <= 0:
        raise ValueError("performance.report_every_rows must be greater than zero")

    summary_format = str(effective["output"]["summary_format"]).lower()
    if summary_format not in {"json", "text", "both"}:
        raise ValueError("output.summary_format must be one of: json, text, both")
    effective["output"]["summary_format"] = summary_format

    return effective


def apply_overrides(config: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = deep_merge({}, config)
    if overrides.get("left_file"):
        merged.setdefault("files", {})["left"] = overrides["left_file"]
    if overrides.get("right_file"):
        merged.setdefault("files", {})["right"] = overrides["right_file"]
    if overrides.get("chunk_size") is not None:
        merged.setdefault("performance", {})["chunk_size"] = overrides["chunk_size"]
    if overrides.get("sample_size") is not None:
        merged.setdefault("sampling", {})["size"] = overrides["sample_size"]
    if overrides.get("sample_seed") is not None:
        merged.setdefault("sampling", {})["seed"] = overrides["sample_seed"]
    if overrides.get("workers") is not None:
        merged.setdefault("performance", {})["workers"] = overrides["workers"]
    if overrides.get("output_dir"):
        merged.setdefault("output", {})["directory"] = overrides["output_dir"]
    if overrides.get("output_prefix"):
        merged.setdefault("output", {})["prefix"] = overrides["output_prefix"]
    return merged


def build_required_columns(config: dict[str, Any]) -> tuple[list[str], list[str]]:
    left_columns = list(dict.fromkeys(list(config["keys"]["left"]) + list(config["compare"]["left"])))
    right_columns = list(dict.fromkeys(list(config["keys"]["right"]) + list(config["compare"]["right"])))
    return left_columns, right_columns


def sample_keys_by_bucket(sampled_keys: set[NormalizedKey], bucket_count: int) -> dict[int, set[NormalizedKey]]:
    grouped: dict[int, set[NormalizedKey]] = {}
    for key in sampled_keys:
        grouped.setdefault(stable_bucket_for_key(key, bucket_count), set()).add(key)
    return grouped


def write_summary_files(output_dir: Path, prefix: str, summary: dict[str, Any], summary_format: str) -> None:
    (output_dir / f"{prefix}summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if summary_format not in {"text", "both"}:
        return

    lines = [
        "csv-stream-diff summary",
        "======================",
        "",
        f"left_file: {summary['files']['left']}",
        f"right_file: {summary['files']['right']}",
        f"mode: {summary['sampling']['mode']}",
        f"sample_requested_size: {summary['sampling']['requested_size']}",
        f"sample_actual_size: {summary['sampling']['actual_size']}",
        f"sample_seed: {summary['sampling']['seed']}",
        "",
        "counts",
        "------",
        f"total_left_rows: {summary['counts']['total_left_rows']:,}",
        f"total_right_rows: {summary['counts']['total_right_rows']:,}",
        f"left_unique_keys: {summary['counts']['left_unique_keys']:,}",
        f"right_unique_keys: {summary['counts']['right_unique_keys']:,}",
        f"matched: {summary['counts']['matched']:,}",
        f"only_in_left: {summary['counts']['only_in_left']:,}",
        f"only_in_right: {summary['counts']['only_in_right']:,}",
        f"different_rows: {summary['counts']['different_rows']:,}",
        f"different_cells: {summary['counts']['different_cells']:,}",
        f"duplicate_keys_left: {summary['counts']['duplicate_keys_left']:,}",
        f"duplicate_keys_right: {summary['counts']['duplicate_keys_right']:,}",
        "",
        "outputs",
        "-------",
    ]
    lines.extend(summary["outputs"].values())
    if summary["warnings"]:
        lines.extend(["", "warnings", "--------"])
        lines.extend(summary["warnings"])
    lines.extend(["", f"elapsed_seconds: {summary['elapsed_seconds']:.3f}"])
    (output_dir / f"{prefix}summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def compare_csv_files(config: dict[str, Any]) -> dict[str, Any]:
    effective = validate_config(config)
    start_time = time.time()

    left_required_columns, right_required_columns = build_required_columns(effective)
    left_csv_options = CSVOptions(**effective["csv"]["left"])
    right_csv_options = CSVOptions(**effective["csv"]["right"])
    normalization = NormalizationSettings(**effective["comparison"])

    output_dir = Path(effective["output"]["directory"])
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = effective["output"]["prefix"]

    temp_root = Path(
        effective["performance"]["temp_directory"]
        or tempfile.mkdtemp(prefix="csv_stream_diff_")
    )
    temp_root.mkdir(parents=True, exist_ok=True)

    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    progress_queue = manager.Queue()
    monitor = ProgressMonitor(
        bucket_count=effective["performance"]["bucket_count"],
        total_bytes_left=os.path.getsize(effective["files"]["left"]),
        total_bytes_right=os.path.getsize(effective["files"]["right"]),
        progress_enabled=bool(effective["performance"]["show_progress"]),
    )
    stop_drainer, stop_renderer, drainer, renderer = start_progress_threads(
        monitor,
        progress_queue,
        bool(effective["performance"]["show_progress"]),
    )

    sampled_keys: Optional[set[NormalizedKey]] = None
    sampled_unique_population = 0
    try:
        left_partition = partition_file(
            file_path=effective["files"]["left"],
            file_label="left",
            csv_options=left_csv_options,
            key_columns=effective["keys"]["left"],
            keep_columns=left_required_columns,
            normalization=normalization,
            bucket_count=effective["performance"]["bucket_count"],
            chunk_size=effective["performance"]["chunk_size"],
            stable_bucket_for_key=stable_bucket_for_key,
            temp_dir=str(temp_root),
            progress_queue=progress_queue,
            report_every_rows=effective["performance"]["report_every_rows"],
        )
        right_partition = partition_file(
            file_path=effective["files"]["right"],
            file_label="right",
            csv_options=right_csv_options,
            key_columns=effective["keys"]["right"],
            keep_columns=right_required_columns,
            normalization=normalization,
            bucket_count=effective["performance"]["bucket_count"],
            chunk_size=effective["performance"]["chunk_size"],
            stable_bucket_for_key=stable_bucket_for_key,
            temp_dir=str(temp_root),
            progress_queue=progress_queue,
            report_every_rows=effective["performance"]["report_every_rows"],
        )

        if effective["sampling"]["size"] > 0:
            sampled_keys, sampled_unique_population = sample_left_keys(
                left_bucket_dir=left_partition.bucket_dir,
                left_key_columns=effective["keys"]["left"],
                normalization=normalization,
                sample_size=effective["sampling"]["size"],
                seed=effective["sampling"]["seed"],
                bucket_count=effective["performance"]["bucket_count"],
                progress_queue=progress_queue,
                report_every_rows=effective["performance"]["report_every_rows"],
            )

        sampled_by_bucket = sample_keys_by_bucket(sampled_keys or set(), effective["performance"]["bucket_count"])
        stats = CompareStats(
            total_left_rows=left_partition.rows,
            total_right_rows=right_partition.rows,
            sample_mode="sampled" if sampled_keys is not None else "full",
            sample_requested_size=effective["sampling"]["size"],
            sample_actual_size=len(sampled_keys or set()),
            sample_seed=effective["sampling"]["seed"],
        )

        tasks: list[tuple[Any, ...]] = []
        for bucket_id in range(effective["performance"]["bucket_count"]):
            left_bucket_path = str(Path(left_partition.bucket_dir) / f"bucket_{bucket_id:04d}.csv")
            right_bucket_path = str(Path(right_partition.bucket_dir) / f"bucket_{bucket_id:04d}.csv")
            if not Path(left_bucket_path).exists() and not Path(right_bucket_path).exists():
                progress_queue.put({"type": "worker_done", "bucket_id": bucket_id, "stats": {}})
                continue
            tasks.append(
                (
                    bucket_id,
                    left_bucket_path,
                    right_bucket_path,
                    effective,
                    str(temp_root),
                    sampled_by_bucket.get(bucket_id, set()) if sampled_keys is not None else None,
                    progress_queue,
                )
            )

        if effective["performance"]["workers"] == 1:
            worker_results = map(compare_bucket_worker, tasks)
        else:
            pool = ctx.Pool(processes=effective["performance"]["workers"])
            worker_results = pool.imap_unordered(compare_bucket_worker, tasks)

        try:
            for worker_stats in worker_results:
                stats.left_unique_keys += worker_stats["left_unique"]
                stats.right_unique_keys += worker_stats["right_unique"]
                stats.only_in_left += worker_stats["only_in_left"]
                stats.only_in_right += worker_stats["only_in_right"]
                stats.matched += worker_stats["matched"]
                stats.different_rows += worker_stats["different_rows"]
                stats.different_cells += worker_stats["different_cells"]
                stats.duplicate_keys_left += worker_stats["duplicate_keys_left"]
                stats.duplicate_keys_right += worker_stats["duplicate_keys_right"]
                stats.warnings.extend(worker_stats["warnings"])
        finally:
            if effective["performance"]["workers"] != 1:
                pool.close()
                pool.join()

        bucket_results_dir = temp_root / "bucket_results"
        outputs = {
            "only_in_left": str(output_dir / f"{prefix}only_in_left.csv"),
            "only_in_right": str(output_dir / f"{prefix}only_in_right.csv"),
            "differences": str(output_dir / f"{prefix}differences.csv"),
            "duplicate_keys": str(output_dir / f"{prefix}duplicate_keys.csv"),
            "summary_json": str(output_dir / f"{prefix}summary.json"),
        }
        merge_csv_parts(
            [bucket_results_dir / f"only_in_left_{bucket_id:04d}.csv" for bucket_id in range(effective["performance"]["bucket_count"])],
            outputs["only_in_left"],
        )
        merge_csv_parts(
            [bucket_results_dir / f"only_in_right_{bucket_id:04d}.csv" for bucket_id in range(effective["performance"]["bucket_count"])],
            outputs["only_in_right"],
        )
        merge_csv_parts(
            [bucket_results_dir / f"differences_{bucket_id:04d}.csv" for bucket_id in range(effective["performance"]["bucket_count"])],
            outputs["differences"],
        )
        merge_csv_parts(
            [bucket_results_dir / f"duplicate_keys_{bucket_id:04d}.csv" for bucket_id in range(effective["performance"]["bucket_count"])],
            outputs["duplicate_keys"],
        )

        if stats.duplicate_keys_left:
            stats.warnings.append(
                f"Left file contains {stats.duplicate_keys_left} duplicate rows and comparison uses the first occurrence."
            )
        if stats.duplicate_keys_right:
            stats.warnings.append(
                f"Right file contains {stats.duplicate_keys_right} duplicate rows and comparison uses the first occurrence."
            )

        stats.elapsed_seconds = time.time() - start_time
        stats.outputs = outputs
        summary = {
            "files": {
                "left": effective["files"]["left"],
                "right": effective["files"]["right"],
            },
            "counts": {
                "total_left_rows": stats.total_left_rows,
                "total_right_rows": stats.total_right_rows,
                "left_unique_keys": stats.left_unique_keys,
                "right_unique_keys": stats.right_unique_keys,
                "matched": stats.matched,
                "only_in_left": stats.only_in_left,
                "only_in_right": stats.only_in_right,
                "different_rows": stats.different_rows,
                "different_cells": stats.different_cells,
                "duplicate_keys_left": stats.duplicate_keys_left,
                "duplicate_keys_right": stats.duplicate_keys_right,
            },
            "sampling": {
                "mode": stats.sample_mode,
                "requested_size": stats.sample_requested_size,
                "actual_size": stats.sample_actual_size,
                "seed": stats.sample_seed,
                "left_unique_population": sampled_unique_population if sampled_keys is not None else stats.left_unique_keys,
            },
            "outputs": outputs,
            "warnings": list(dict.fromkeys(stats.warnings)),
            "effective_config": effective,
            "elapsed_seconds": stats.elapsed_seconds,
        }
        write_summary_files(output_dir, prefix, summary, effective["output"]["summary_format"])
        return summary
    finally:
        finish_progress_threads(
            stop_drainer=stop_drainer,
            stop_renderer=stop_renderer,
            drainer=drainer,
            renderer=renderer,
            progress_queue=progress_queue,
        )
        manager.shutdown()
        cleanup_temp_dir(temp_root, bool(effective["performance"]["keep_temp_files"]))


def compare_from_path(config_path: str | Path, overrides: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    loaded = load_config(config_path)
    return compare_csv_files(apply_overrides(loaded, overrides or {}))
