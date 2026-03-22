from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys

from .comparer import compare_from_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare very large CSV files using streaming and multiprocessing.")
    parser.add_argument("--config", "-c", required=True, help="Path to the YAML configuration file.")
    parser.add_argument("--left-file", help="Override files.left from the config.")
    parser.add_argument("--right-file", help="Override files.right from the config.")
    parser.add_argument("--chunk-size", type=int, help="Override performance.chunk_size.")
    parser.add_argument("--size", "--sample-size", dest="sample_size", type=int, help="Override sampling.size.")
    parser.add_argument("--sample-seed", type=int, help="Override sampling.seed.")
    parser.add_argument("--workers", type=int, help="Override performance.workers.")
    parser.add_argument("--output-dir", help="Override output.directory.")
    parser.add_argument("--output-prefix", help="Override output.prefix.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    overrides = {
        "left_file": args.left_file,
        "right_file": args.right_file,
        "chunk_size": args.chunk_size,
        "sample_size": args.sample_size,
        "sample_seed": args.sample_seed,
        "workers": args.workers,
        "output_dir": args.output_dir,
        "output_prefix": args.output_prefix,
    }

    try:
        summary = compare_from_path(args.config, overrides)
    except Exception as exc:
        print(f"csv-stream-diff failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
