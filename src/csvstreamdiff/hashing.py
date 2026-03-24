from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Tuple


NULL_LIKE_VALUES = {"", "null", "none", "nan", "na", "n/a"}
TRUE_LIKE_VALUES = {"true", "t", "yes", "y", "1"}
FALSE_LIKE_VALUES = {"false", "f", "no", "n", "0"}
EXPLICIT_TRUE_LIKE_VALUES = {"true", "t", "yes", "y"}
EXPLICIT_FALSE_LIKE_VALUES = {"false", "f", "no", "n"}


@dataclass(frozen=True)
class NormalizationSettings:
    case_insensitive: bool = False
    trim_whitespace: bool = True
    treat_null_as_equal: bool = False
    normalize_numeric_values: bool = False
    treat_null_as_zero_for_numeric: bool = False
    numeric_decimal_places: Optional[int] = None
    normalize_boolean_values: bool = False


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


def _quantize_decimal(value: Decimal, decimal_places: Optional[int]) -> Decimal:
    if decimal_places is None:
        return value
    quantizer = Decimal("1").scaleb(-decimal_places)
    return value.quantize(quantizer, rounding=ROUND_HALF_UP)


def _boolean_from_text(text: Optional[str]) -> Optional[bool]:
    if text is None:
        return None

    lowered = text.lower()
    if lowered in TRUE_LIKE_VALUES:
        return True
    if lowered in FALSE_LIKE_VALUES:
        return False

    decimal_value = _decimal_from_text(text)
    if decimal_value is not None and decimal_value in {Decimal(0), Decimal(1)}:
        return bool(decimal_value)

    return None


def _explicit_boolean_from_text(text: Optional[str]) -> Optional[bool]:
    if text is None:
        return None

    lowered = text.lower()
    if lowered in EXPLICIT_TRUE_LIKE_VALUES:
        return True
    if lowered in EXPLICIT_FALSE_LIKE_VALUES:
        return False
    return None


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

    if settings.normalize_boolean_values:
        left_explicit_boolean = _explicit_boolean_from_text(left_text)
        right_explicit_boolean = _explicit_boolean_from_text(right_text)
        left_boolean = _boolean_from_text(left_text)
        right_boolean = _boolean_from_text(right_text)
        if (
            (left_explicit_boolean is not None or right_explicit_boolean is not None)
            and left_boolean is not None
            and right_boolean is not None
        ):
            return str(left_boolean).lower(), str(right_boolean).lower()

    if settings.normalize_numeric_values and left_decimal is not None and right_decimal is not None:
        left_decimal = _quantize_decimal(left_decimal, settings.numeric_decimal_places)
        right_decimal = _quantize_decimal(right_decimal, settings.numeric_decimal_places)
        return _canonical_decimal(left_decimal), _canonical_decimal(right_decimal)

    if settings.treat_null_as_zero_for_numeric and left_decimal is not None and right_decimal is not None:
        left_decimal = _quantize_decimal(left_decimal, settings.numeric_decimal_places)
        right_decimal = _quantize_decimal(right_decimal, settings.numeric_decimal_places)
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
