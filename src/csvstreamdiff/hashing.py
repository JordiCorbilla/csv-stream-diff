from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Tuple


NULL_LIKE_VALUES = {"", "null", "none", "nan", "na", "n/a"}


@dataclass(frozen=True)
class NormalizationSettings:
    case_insensitive: bool = False
    trim_whitespace: bool = True
    treat_null_as_equal: bool = False


NormalizedKey = Tuple[Optional[str], ...]


def normalize_value(value: Any, settings: NormalizationSettings) -> Optional[str]:
    if value is None:
        return None if settings.treat_null_as_equal else ""

    text = str(value)
    if settings.trim_whitespace:
        text = text.strip()
    if settings.case_insensitive:
        text = text.lower()

    if settings.treat_null_as_equal and text.lower() in NULL_LIKE_VALUES:
        return None
    return text


def normalized_key(
    row: dict[str, Any],
    columns: Sequence[str],
    settings: NormalizationSettings,
) -> NormalizedKey:
    return tuple(normalize_value(row.get(column), settings) for column in columns)


def normalized_pair(
    left: Any,
    right: Any,
    settings: NormalizationSettings,
) -> tuple[Optional[str], Optional[str]]:
    return normalize_value(left, settings), normalize_value(right, settings)


def stable_bucket_for_key(key: Iterable[Optional[str]], bucket_count: int) -> int:
    if bucket_count <= 0:
        raise ValueError("bucket_count must be greater than zero")

    encoded = "\x1f".join("" if part is None else str(part) for part in key).encode("utf-8", "ignore")
    digest = hashlib.blake2b(encoded, digest_size=8).digest()
    return int.from_bytes(digest, "big") % bucket_count


def key_to_output_dict(key: NormalizedKey, left_key_columns: Sequence[str]) -> dict[str, Optional[str]]:
    return {f"key_{column}": key[index] for index, column in enumerate(left_key_columns)}
