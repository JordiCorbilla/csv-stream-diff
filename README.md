# csv-stream-diff

`csv-stream-diff` compares very large CSV files with streaming I/O, hashed bucket partitioning, and multiprocessing. It is designed for datasets that are too large to load fully into memory.

## Features

- Compare CSVs by configurable key columns, even when left and right headers differ
- Stream files in chunks with configurable `chunk_size`
- Partition by stable hashed key to keep worker memory bounded
- Use all CPUs by default, or set a worker count explicitly
- Write machine-usable output artifacts for left-only, right-only, cell differences, duplicate keys, and run summary
- Support exact random sampling for validation runs with `sampling.size > 0`
- Warn on duplicate keys and continue using the first occurrence per key
- Include a fixture generator and both `pytest` and `behave` tests

## Installation

```bash
pip install csv-stream-diff
```

For local development:

```bash
poetry install
```

## CLI

```bash
csv-stream-diff --config config.yaml
```

Optional overrides:

```bash
csv-stream-diff \
  --config config.yaml \
  --left-file ./left.csv \
  --right-file ./right.csv \
  --chunk-size 100000 \
  --sample-size 100000 \
  --sample-seed 20260321 \
  --workers 8 \
  --output-dir ./output \
  --output-prefix run_
```

The YAML config is the default source of truth. CLI flags override it for a single run.

## Configuration

See [config.example.yaml](/c:/repo/csv-stream-diff/config.example.yaml) for a full example.

Main sections:

- `files.left`, `files.right`: input CSV paths
- `csv.left`, `csv.right`: dialect and encoding settings
- `keys.left`, `keys.right`: key columns used to match rows
- `compare.left`, `compare.right`: value columns to compare
- `comparison`: normalization options
- `sampling`: `size: 0` means full comparison; any positive value means exact random sample by left-side unique key with a fixed seed
- `performance`: chunking, worker count, bucket count, temp directory, progress reporting
- `output`: output directory, filename prefix, whether to include serialized full rows, and whether to write a text summary

## Output Files

The tool writes these artifacts to `output.directory`:

- `<prefix>only_in_left.csv`
- `<prefix>only_in_right.csv`
- `<prefix>differences.csv`
- `<prefix>duplicate_keys.csv`
- `<prefix>summary.json`
- `<prefix>summary.txt` when `output.summary_format` is `text` or `both`

`differences.csv` contains one row per differing cell with both the left and right column names and values.

## Sampling

- `sampling.size: 0` runs the full comparison.
- `sampling.size > 0` selects an exact random sample of left-side unique keys using reservoir sampling.
- Sampling is reproducible when `sampling.seed` stays the same.
- Duplicate keys do not expand the sampling population because only the first occurrence per key is considered.

## Duplicate Keys

Duplicate keys do not stop the run. They are written to `duplicate_keys.csv`, counted in the summary, and the main comparison uses the first occurrence of each key on each side.

## Generator

The generator creates two baseline-identical CSVs, applies controlled mutations, writes a matching config, and saves an expected manifest:

```bash
python generator/generate_fixtures.py --output-dir ./generated --rows 10000 --seed 42
```

Generated artifacts:

- `left.csv`
- `right.csv`
- `config.generated.yaml`
- `expected.json`

## Tests

Run unit tests:

```bash
poetry run pytest
```

Run BDD acceptance tests:

```bash
poetry run behave tests/features
```

Run a package build:

```bash
poetry build
```

## PyPI Packaging

Build source and wheel distributions:

```bash
poetry build
```

Upload after verifying artifacts:

```bash
poetry publish
```
