from __future__ import annotations

import csv
import json
import multiprocessing as mp
import os
import queue
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from . import __version__
from .hashing import NormalizationSettings, NormalizedKey, key_to_output_dict, normalized_key, normalized_pair
from .streaming import CSVOptions, ensure_columns_exist, get_stream_position, open_dict_reader, open_dict_writer


def format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def format_bytes(value: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024.0
        index += 1
    return f"{value:.1f}{units[index]}"

@dataclass(frozen=True)
class PartitionResult:
    rows: int
    bucket_dir: str


def make_output_headers(
    left_key_columns: Sequence[str],
    include_full_rows: bool,
    include_normalized_values: bool,
) -> dict[str, list[str]]:
    common_key_fields = [f"key_{column}" for column in left_key_columns]
    only_left = common_key_fields + (["left_row_json"] if include_full_rows else [])
    only_right = common_key_fields + (["right_row_json"] if include_full_rows else [])
    differences = common_key_fields + [
        "difference_count",
        "differences_text",
    ]
    if include_normalized_values:
        differences.append("normalized_differences_text")
    differences.append("differences_json")
    if include_full_rows:
        differences.extend(["left_row_json", "right_row_json"])
    duplicates = common_key_fields + ["side", "row_number", "row_json"]
    return {
        "only_left": only_left,
        "only_right": only_right,
        "differences": differences,
        "duplicates": duplicates,
    }


def partition_file(
    *,
    file_path: str,
    file_label: str,
    csv_options: CSVOptions,
    key_columns: Sequence[str],
    keep_columns: Sequence[str],
    normalization: NormalizationSettings,
    bucket_count: int,
    chunk_size: int,
    stable_bucket_for_key,
    temp_dir: str,
    progress_queue: Optional[mp.Queue],
    report_every_rows: int,
) -> PartitionResult:
    bucket_directory = Path(temp_dir) / file_label
    bucket_directory.mkdir(parents=True, exist_ok=True)

    handles: dict[int, Any] = {}
    writers: dict[int, csv.DictWriter] = {}
    total_rows = 0
    total_bytes = os.path.getsize(file_path)
    last_report_rows = 0
    keep_fields = list(dict.fromkeys(keep_columns))

    handle, reader = open_dict_reader(file_path, csv_options)
    try:
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError(f"{file_label} does not contain a CSV header row")
        ensure_columns_exist(fieldnames, keep_fields, file_label)

        chunk: list[dict[str, str]] = []
        for row in reader:
            chunk.append(row)
            if len(chunk) < chunk_size:
                continue

            total_rows = _flush_partition_chunk(
                chunk=chunk,
                key_columns=key_columns,
                keep_fields=keep_fields,
                normalization=normalization,
                bucket_count=bucket_count,
                stable_bucket_for_key=stable_bucket_for_key,
                writers=writers,
                handles=handles,
                bucket_directory=bucket_directory,
                total_rows=total_rows,
            )
            chunk = []

            if progress_queue is not None and (total_rows - last_report_rows) >= report_every_rows:
                last_report_rows = total_rows
                progress_queue.put(
                    {
                        "type": "partition_progress",
                        "phase": file_label,
                        "rows": total_rows,
                        "bytes": min(total_bytes, get_stream_position(handle)),
                        "total_bytes": total_bytes,
                    }
                )

        if chunk:
            total_rows = _flush_partition_chunk(
                chunk=chunk,
                key_columns=key_columns,
                keep_fields=keep_fields,
                normalization=normalization,
                bucket_count=bucket_count,
                stable_bucket_for_key=stable_bucket_for_key,
                writers=writers,
                handles=handles,
                bucket_directory=bucket_directory,
                total_rows=total_rows,
            )
    finally:
        handle.close()
        for bucket_handle in handles.values():
            bucket_handle.close()

    if progress_queue is not None:
        progress_queue.put(
            {
                "type": "partition_done",
                "phase": file_label,
                "rows": total_rows,
                "bytes": total_bytes,
                "total_bytes": total_bytes,
            }
        )

    return PartitionResult(rows=total_rows, bucket_dir=str(bucket_directory))


def _flush_partition_chunk(
    *,
    chunk: list[dict[str, str]],
    key_columns: Sequence[str],
    keep_fields: Sequence[str],
    normalization: NormalizationSettings,
    bucket_count: int,
    stable_bucket_for_key,
    writers: dict[int, csv.DictWriter],
    handles: dict[int, Any],
    bucket_directory: Path,
    total_rows: int,
) -> int:
    for row in chunk:
        key = normalized_key(row, key_columns, normalization)
        bucket_id = stable_bucket_for_key(key, bucket_count)
        if bucket_id not in handles:
            bucket_path = bucket_directory / f"bucket_{bucket_id:04d}.csv"
            bucket_handle = open(bucket_path, "w", encoding="utf-8", newline="")
            writer = csv.DictWriter(bucket_handle, fieldnames=list(keep_fields))
            writer.writeheader()
            handles[bucket_id] = bucket_handle
            writers[bucket_id] = writer
        writers[bucket_id].writerow({column: row.get(column) for column in keep_fields})
        total_rows += 1
    return total_rows


def sample_left_keys(
    *,
    left_bucket_dir: str,
    left_key_columns: Sequence[str],
    normalization: NormalizationSettings,
    sample_size: int,
    seed: int,
    bucket_count: int,
    progress_queue: Optional[mp.Queue],
    report_every_rows: int,
) -> tuple[set[NormalizedKey], int]:
    if sample_size <= 0:
        return set(), 0

    import random

    reservoir: list[NormalizedKey] = []
    rng = random.Random(seed)
    unique_keys_seen = 0
    rows_seen = 0
    last_report_rows = 0

    for bucket_id in range(bucket_count):
        bucket_path = Path(left_bucket_dir) / f"bucket_{bucket_id:04d}.csv"
        if not bucket_path.exists():
            continue

        seen_in_bucket: set[NormalizedKey] = set()
        handle, reader = open_dict_reader(bucket_path, CSVOptions(encoding="utf-8"))
        try:
            for row in reader:
                rows_seen += 1
                key = normalized_key(row, left_key_columns, normalization)
                if key in seen_in_bucket:
                    continue
                seen_in_bucket.add(key)
                unique_keys_seen += 1

                if len(reservoir) < sample_size:
                    reservoir.append(key)
                else:
                    index = rng.randint(0, unique_keys_seen - 1)
                    if index < sample_size:
                        reservoir[index] = key

                if progress_queue is not None and (rows_seen - last_report_rows) >= report_every_rows:
                    last_report_rows = rows_seen
                    progress_queue.put(
                        {
                            "type": "sample_progress",
                            "rows": rows_seen,
                            "unique_keys": unique_keys_seen,
                            "target": sample_size,
                        }
                    )
        finally:
            handle.close()

    if progress_queue is not None:
        progress_queue.put(
            {
                "type": "sample_done",
                "rows": rows_seen,
                "unique_keys": unique_keys_seen,
                "target": sample_size,
            }
        )

    return set(reservoir), unique_keys_seen


def compare_bucket_worker(args: tuple[Any, ...]) -> dict[str, Any]:
    (
        bucket_id,
        left_bucket_path,
        right_bucket_path,
        config,
        temp_dir,
        sampled_keys,
        progress_queue,
    ) = args

    left_keys = list(config["keys"]["left"])
    right_keys = list(config["keys"]["right"])
    compare_pairs = list(zip(config["compare"]["left"], config["compare"]["right"]))
    normalization = NormalizationSettings(**config["comparison"])
    include_full_rows = bool(config["output"]["include_full_rows"])
    include_normalized_values = bool(config["output"].get("include_normalized_values", False))
    report_every_rows = int(config["performance"]["report_every_rows"])
    headers = make_output_headers(left_keys, include_full_rows, include_normalized_values)

    results_dir = Path(temp_dir) / "bucket_results"
    results_dir.mkdir(parents=True, exist_ok=True)

    only_left_path = results_dir / f"only_in_left_{bucket_id:04d}.csv"
    only_right_path = results_dir / f"only_in_right_{bucket_id:04d}.csv"
    differences_path = results_dir / f"differences_{bucket_id:04d}.csv"
    duplicates_path = results_dir / f"duplicate_keys_{bucket_id:04d}.csv"

    only_left_handle, only_left_writer = open_dict_writer(only_left_path, headers["only_left"])
    only_right_handle, only_right_writer = open_dict_writer(only_right_path, headers["only_right"])
    differences_handle, differences_writer = open_dict_writer(differences_path, headers["differences"])
    duplicates_handle, duplicates_writer = open_dict_writer(duplicates_path, headers["duplicates"])

    left_index: dict[NormalizedKey, dict[str, str]] = {}
    sampled_left_keys_remaining: set[NormalizedKey] = set()
    right_seen_first_occurrence: set[NormalizedKey] = set()
    stats = {
        "bucket_id": bucket_id,
        "left_rows": 0,
        "right_rows": 0,
        "left_unique": 0,
        "right_unique": 0,
        "only_in_left": 0,
        "only_in_right": 0,
        "matched": 0,
        "different_rows": 0,
        "different_cells": 0,
        "duplicate_keys_left": 0,
        "duplicate_keys_right": 0,
        "warnings": [],
        "only_left_path": str(only_left_path),
        "only_right_path": str(only_right_path),
        "differences_path": str(differences_path),
        "duplicates_path": str(duplicates_path),
    }

    last_report_rows = 0
    if os.path.exists(left_bucket_path):
        handle, reader = open_dict_reader(left_bucket_path, CSVOptions(encoding="utf-8"))
        try:
            for row_number, row in enumerate(reader, start=2):
                stats["left_rows"] += 1
                key = normalized_key(row, left_keys, normalization)
                if key in left_index:
                    stats["duplicate_keys_left"] += 1
                    duplicates_writer.writerow(
                        {
                            **key_to_output_dict(key, left_keys),
                            "side": "left",
                            "row_number": row_number,
                            "row_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
                        }
                    )
                    continue

                left_index[key] = row
                stats["left_unique"] += 1
                if sampled_keys is None or key in sampled_keys:
                    sampled_left_keys_remaining.add(key)

                if progress_queue is not None and (stats["left_rows"] - last_report_rows) >= report_every_rows:
                    last_report_rows = stats["left_rows"]
                    progress_queue.put(
                        {
                            "type": "worker_progress",
                            "bucket_id": bucket_id,
                            "stage": "index_left",
                            "rows": stats["left_rows"],
                        }
                    )
        finally:
            handle.close()

    last_report_rows = 0
    if os.path.exists(right_bucket_path):
        handle, reader = open_dict_reader(right_bucket_path, CSVOptions(encoding="utf-8"))
        try:
            for row_number, row in enumerate(reader, start=2):
                stats["right_rows"] += 1
                key = normalized_key(row, right_keys, normalization)
                if key in right_seen_first_occurrence:
                    stats["duplicate_keys_right"] += 1
                    duplicates_writer.writerow(
                        {
                            **key_to_output_dict(key, left_keys),
                            "side": "right",
                            "row_number": row_number,
                            "row_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
                        }
                    )
                    continue

                right_seen_first_occurrence.add(key)
                stats["right_unique"] += 1

                if key not in left_index:
                    only_right_row = key_to_output_dict(key, left_keys)
                    if include_full_rows:
                        only_right_row["right_row_json"] = json.dumps(row, ensure_ascii=False, sort_keys=True)
                    only_right_writer.writerow(only_right_row)
                    stats["only_in_right"] += 1
                    continue

                if sampled_keys is not None and key not in sampled_keys:
                    continue

                stats["matched"] += 1
                row_has_difference = False
                left_row = left_index[key]
                row_differences: list[dict[str, Any]] = []
                for left_column, right_column in compare_pairs:
                    left_normalized, right_normalized = normalized_pair(
                        left_row.get(left_column),
                        row.get(right_column),
                        normalization,
                    )
                    if left_normalized == right_normalized:
                        continue

                    row_has_difference = True
                    stats["different_cells"] += 1
                    row_differences.append(
                        {
                            "left_column": left_column,
                            "right_column": right_column,
                            "left_value": left_row.get(left_column),
                            "right_value": row.get(right_column),
                            **(
                                {
                                    "normalized_left_value": left_normalized,
                                    "normalized_right_value": right_normalized,
                                }
                                if include_normalized_values
                                else {}
                            ),
                        }
                    )

                if row_has_difference:
                    differences_text = "; ".join(
                        f"{item['left_column']}: {item['left_value']} -> {item['right_value']}"
                        if item["left_column"] == item["right_column"]
                        else f"{item['left_column']}/{item['right_column']}: {item['left_value']} -> {item['right_value']}"
                        for item in row_differences
                    )
                    diff_row: dict[str, Any] = {
                        **key_to_output_dict(key, left_keys),
                        "difference_count": len(row_differences),
                        "differences_text": differences_text,
                        "differences_json": json.dumps(row_differences, ensure_ascii=False, sort_keys=True),
                    }
                    if include_normalized_values:
                        diff_row["normalized_differences_text"] = "; ".join(
                            (
                                f"{item['left_column']}: {item['normalized_left_value']} -> {item['normalized_right_value']}"
                                if item["left_column"] == item["right_column"]
                                else f"{item['left_column']}/{item['right_column']}: {item['normalized_left_value']} -> {item['normalized_right_value']}"
                            )
                            for item in row_differences
                        )
                    if include_full_rows:
                        diff_row["left_row_json"] = json.dumps(left_row, ensure_ascii=False, sort_keys=True)
                        diff_row["right_row_json"] = json.dumps(row, ensure_ascii=False, sort_keys=True)
                    differences_writer.writerow(diff_row)
                    stats["different_rows"] += 1
                sampled_left_keys_remaining.discard(key)

                if progress_queue is not None and (stats["right_rows"] - last_report_rows) >= report_every_rows:
                    last_report_rows = stats["right_rows"]
                    progress_queue.put(
                        {
                            "type": "worker_progress",
                            "bucket_id": bucket_id,
                            "stage": "scan_right",
                            "rows": stats["right_rows"],
                        }
                    )
        finally:
            handle.close()

    for key in sampled_left_keys_remaining:
        only_left_row = key_to_output_dict(key, left_keys)
        if include_full_rows:
            only_left_row["left_row_json"] = json.dumps(left_index[key], ensure_ascii=False, sort_keys=True)
        only_left_writer.writerow(only_left_row)
        stats["only_in_left"] += 1

    if stats["duplicate_keys_left"] or stats["duplicate_keys_right"]:
        stats["warnings"].append(
            "Duplicate keys detected. The comparison used the first occurrence for each key."
        )

    only_left_handle.close()
    only_right_handle.close()
    differences_handle.close()
    duplicates_handle.close()

    if progress_queue is not None:
        progress_queue.put({"type": "worker_done", "bucket_id": bucket_id, "stats": stats})

    return stats


class ProgressMonitor:
    def __init__(
        self,
        *,
        bucket_count: int,
        total_bytes_left: int,
        total_bytes_right: int,
        left_file: str,
        right_file: str,
        progress_enabled: bool,
    ):
        self.bucket_count = bucket_count
        self.total_bytes = {"left": max(1, total_bytes_left), "right": max(1, total_bytes_right)}
        self.files = {"left": left_file, "right": right_file}
        self.partition = {
            "left": {"rows": 0, "bytes": 0, "done": False},
            "right": {"rows": 0, "bytes": 0, "done": False},
        }
        self.sampling = {"rows": 0, "unique_keys": 0, "target": 0, "done": False}
        self.workers: dict[int, dict[str, Any]] = {}
        self.completed_buckets = 0
        self.warning_count = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.progress_enabled = progress_enabled

    def update(self, message: dict[str, Any]) -> None:
        with self.lock:
            message_type = message.get("type")
            if message_type in {"partition_progress", "partition_done"}:
                phase = message["phase"]
                self.partition[phase]["rows"] = message.get("rows", self.partition[phase]["rows"])
                self.partition[phase]["bytes"] = message.get("bytes", self.partition[phase]["bytes"])
                if message_type == "partition_done":
                    self.partition[phase]["done"] = True
            elif message_type in {"sample_progress", "sample_done"}:
                self.sampling["rows"] = message.get("rows", self.sampling["rows"])
                self.sampling["unique_keys"] = message.get("unique_keys", self.sampling["unique_keys"])
                self.sampling["target"] = message.get("target", self.sampling["target"])
                if message_type == "sample_done":
                    self.sampling["done"] = True
            elif message_type == "worker_progress":
                bucket_id = message["bucket_id"]
                snapshot = self.workers.setdefault(bucket_id, {})
                snapshot["stage"] = message.get("stage", "?")
                snapshot["rows"] = message.get("rows", 0)
                snapshot["done"] = False
            elif message_type == "worker_done":
                bucket_id = message["bucket_id"]
                snapshot = self.workers.setdefault(bucket_id, {})
                if not snapshot.get("done"):
                    self.completed_buckets += 1
                snapshot["done"] = True
                snapshot["stage"] = "done"
                stats = message.get("stats") or {}
                if stats.get("duplicate_keys_left", 0) or stats.get("duplicate_keys_right", 0):
                    self.warning_count += 1

    def renderable(self):
        with self.lock:
            elapsed = max(time.time() - self.start_time, 0.001)
            summary = Table.grid(expand=True)
            summary.add_column(justify="left")
            summary.add_column(justify="right")
            summary.add_row(
                Text(f"csv-stream-diff v{__version__}", style="bold cyan"),
                Text(f"elapsed={format_seconds(elapsed)}  warnings={self.warning_count}", style="bold"),
            )
            summary.add_row(Text(f"left: {self.files['left']}", style="cyan"), Text(""))
            summary.add_row(Text(f"right: {self.files['right']}", style="cyan"), Text(""))

            phases = Table.grid(expand=True)
            phases.add_column(style="bold")
            phases.add_column()
            phases.add_column(justify="right")
            phases.add_column(justify="right")
            phases.add_column(justify="right")

            for phase in ("left", "right"):
                processed = min(self.partition[phase]["bytes"], self.total_bytes[phase])
                ratio = processed / self.total_bytes[phase]
                rate = self.partition[phase]["rows"] / elapsed
                eta = (elapsed / ratio - elapsed) if ratio > 0 else 0
                state = "[green]done[/green]" if self.partition[phase]["done"] else "[blue]run[/blue]"
                phases.add_row(
                    f"partition {phase}",
                    ProgressBar(total=100, completed=int(ratio * 100), width=None),
                    f"{state}  {format_bytes(processed)}/{format_bytes(self.total_bytes[phase])}",
                    f" rows={self.partition[phase]['rows']:,} rate={rate:,.0f}/s eta={format_seconds(eta)}",
                    "",
                )

            if self.sampling["target"] > 0 or self.sampling["done"]:
                ratio = min(1.0, self.sampling["unique_keys"] / max(1, self.sampling["target"]))
                state_text = "[green]done[/green]" if self.sampling["done"] else "[blue]run[/blue]"
                phases.add_row(
                    "sampling",
                    ProgressBar(total=100, completed=int(ratio * 100), width=None),
                    f"{state_text}  target={self.sampling['target']:,}",
                    f"rows={self.sampling['rows']:,}",
                    f"unique_keys={self.sampling['unique_keys']:,}",
                )

            bucket_ratio = self.completed_buckets / max(1, self.bucket_count)
            compare_eta = (elapsed / bucket_ratio - elapsed) if bucket_ratio > 0 else 0
            phases.add_row(
                "compare buckets",
                ProgressBar(total=100, completed=int(bucket_ratio * 100), width=None),
                f"[blue]run[/blue]  {self.completed_buckets}/{self.bucket_count}",
                "",
                f" eta={format_seconds(compare_eta)}",
            )

            workers = Table.grid(expand=True)
            workers.add_column()
            active_workers = sorted(
                (bucket_id, info)
                for bucket_id, info in self.workers.items()
                if not info.get("done")
            )
            for bucket_id, info in active_workers[:6]:
                workers.add_row(
                    f"bucket={bucket_id:04d}  stage={info.get('stage', '?'):<11}  rows={info.get('rows', 0):,}"
                )

            group_items = [summary, phases]
            if active_workers:
                group_items.append(Text("active workers", style="bold yellow"))
                group_items.append(workers)

            return Panel(Group(*group_items), border_style="cyan")


def drain_queue(progress_queue: mp.Queue, monitor: ProgressMonitor, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            monitor.update(progress_queue.get(timeout=0.2))
        except queue.Empty:
            continue


def start_progress_threads(
    monitor: ProgressMonitor,
    progress_queue: mp.Queue,
    show_progress: bool,
) -> tuple[threading.Event, threading.Event, threading.Thread, threading.Thread]:
    stop_drainer = threading.Event()
    stop_renderer = threading.Event()
    drainer = threading.Thread(target=drain_queue, args=(progress_queue, monitor, stop_drainer), daemon=True)
    drainer.start()

    def render_loop() -> None:
        console = Console(file=sys.stderr if sys.stderr.isatty() else sys.stdout)
        if not show_progress or not console.is_terminal:
            while not stop_renderer.is_set():
                time.sleep(1.0)
            return

        with Live(
            monitor.renderable(),
            console=console,
            refresh_per_second=4,
            transient=False,
            auto_refresh=False,
        ) as live:
            while not stop_renderer.is_set():
                live.update(monitor.renderable(), refresh=True)
                time.sleep(0.25)

    renderer = threading.Thread(target=render_loop, daemon=True)
    renderer.start()
    return stop_drainer, stop_renderer, drainer, renderer


def finish_progress_threads(
    *,
    stop_drainer: threading.Event,
    stop_renderer: threading.Event,
    drainer: threading.Thread,
    renderer: threading.Thread,
    progress_queue: mp.Queue,
) -> None:
    stop_renderer.set()
    stop_drainer.set()
    drainer.join(timeout=1.0)
    renderer.join(timeout=1.0)
    try:
        close = getattr(progress_queue, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def cleanup_temp_dir(temp_root: Path, keep_temp_files: bool) -> None:
    if not keep_temp_files and temp_root.exists():
        shutil.rmtree(temp_root, ignore_errors=True)
