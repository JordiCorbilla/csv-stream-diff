from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence, TextIO


@dataclass(frozen=True)
class CSVOptions:
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    newline: str = ""


def csv_kwargs(options: CSVOptions) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "delimiter": options.delimiter,
        "quotechar": options.quotechar,
    }
    if options.escapechar:
        kwargs["escapechar"] = options.escapechar
    return kwargs


def open_dict_reader(path: str | Path, options: CSVOptions) -> tuple[TextIO, csv.DictReader]:
    handle = open(path, "r", encoding=options.encoding, newline=options.newline)
    return handle, csv.DictReader(handle, **csv_kwargs(options))


def open_dict_writer(path: str | Path, fieldnames: Sequence[str]) -> tuple[TextIO, csv.DictWriter]:
    handle = open(path, "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
    writer.writeheader()
    return handle, writer


def ensure_columns_exist(fieldnames: Sequence[str], required: Sequence[str], file_label: str) -> None:
    missing = [column for column in required if column not in fieldnames]
    if missing:
        raise ValueError(f"{file_label} is missing required columns: {missing}")


def get_stream_position(handle: TextIO) -> int:
    for candidate in (handle, getattr(handle, "buffer", None)):
        if candidate is None:
            continue
        try:
            return int(candidate.tell())
        except (AttributeError, OSError, ValueError):
            continue
    return 0


def iter_csv_chunks(
    path: str | Path,
    options: CSVOptions,
    chunk_size: int,
) -> Iterator[tuple[list[str], list[dict[str, str]]]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    handle, reader = open_dict_reader(path, options)
    try:
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError(f"{path} does not contain a CSV header row")

        chunk: list[dict[str, str]] = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield fieldnames, chunk
                chunk = []
        if chunk:
            yield fieldnames, chunk
    finally:
        handle.close()


def merge_csv_parts(part_paths: Iterable[str | Path], destination: str | Path) -> int:
    rows_written = 0
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    header_written = False
    writer: Optional[csv.DictWriter] = None
    with open(destination_path, "w", encoding="utf-8", newline="") as destination_handle:
        for part_path in part_paths:
            current_path = Path(part_path)
            if not current_path.exists():
                continue

            with open(current_path, "r", encoding="utf-8", newline="") as part_handle:
                reader = csv.DictReader(part_handle)
                if reader.fieldnames is None:
                    continue

                if writer is None:
                    writer = csv.DictWriter(destination_handle, fieldnames=reader.fieldnames)
                if not header_written:
                    writer.writeheader()
                    header_written = True

                for row in reader:
                    writer.writerow(row)
                    rows_written += 1

    if not header_written:
        destination_path.write_text("", encoding="utf-8")

    return rows_written
