from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Tuple


NULL_LIKE_VALUES = {"", "null", "none", "nan", "na", "n/a"}


@dataclass(frozen=True)
class NormalizationSettings:
    case_insensitive: bool = False
    trim_whitespace: bool = True
    treat_null_as_equal: bool = False
    normalize_numeric_values: bool = False
    treat_null_as_zero_for_numeric: bool = False


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


def _decimal_from_text(text: Optional[str]) -> Optional[Decimal]:
    if text is None:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _canonical_decimal(value: Decimal) -> str:
    normalized = format(value.normalize(), "f").rstrip("0").rstrip(".")
    return normalized if normalized not in {"", "-0"} else "0"


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
    left_text = normalize_value(left, settings)
    right_text = normalize_value(right, settings)

    left_decimal = _decimal_from_text(left_text)
    right_decimal = _decimal_from_text(right_text)

    if settings.treat_null_as_zero_for_numeric:
        if left_text is None and right_decimal is not None:
            left_decimal = Decimal(0)
        if right_text is None and left_decimal is not None:
            right_decimal = Decimal(0)

    if settings.normalize_numeric_values and left_decimal is not None and right_decimal is not None:
        return _canonical_decimal(left_decimal), _canonical_decimal(right_decimal)

    if settings.treat_null_as_zero_for_numeric and left_decimal is not None and right_decimal is not None:
        return _canonical_decimal(left_decimal), _canonical_decimal(right_decimal)

    return left_text, right_text


def stable_bucket_for_key(key: Iterable[Optional[str]], bucket_count: int) -> int:
    if bucket_count <= 0:
        raise ValueError("bucket_count must be greater than zero")

    encoded = "\x1f".join("" if part is None else str(part) for part in key).encode("utf-8", "ignore")
    digest = hashlib.blake2b(encoded, digest_size=8).digest()
    return int.from_bytes(digest, "big") % bucket_count


def key_to_output_dict(key: NormalizedKey, left_key_columns: Sequence[str]) -> dict[str, Optional[str]]:
    return {f"key_{column}": key[index] for index, column in enumerate(left_key_columns)}
