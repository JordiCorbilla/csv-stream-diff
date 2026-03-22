from __future__ import annotations

import csv

from csvstreamdiff.streaming import CSVOptions, iter_csv_chunks


def test_iter_csv_chunks_yields_expected_chunk_sizes(tmp_path) -> None:
    path = tmp_path / "input.csv"
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "value"])
        writer.writeheader()
        writer.writerows(
            [
                {"id": "1", "value": "a"},
                {"id": "2", "value": "b"},
                {"id": "3", "value": "c"},
            ]
        )

    chunks = list(iter_csv_chunks(path, CSVOptions(), chunk_size=2))
    assert [len(rows) for _, rows in chunks] == [2, 1]
    assert chunks[0][0] == ["id", "value"]
