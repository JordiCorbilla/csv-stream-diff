from __future__ import annotations

from csvstreamdiff.hashing import NormalizationSettings, normalize_value, normalized_pair, stable_bucket_for_key


def test_normalize_value_honors_whitespace_case_and_null_rules() -> None:
    settings = NormalizationSettings(
        case_insensitive=True,
        trim_whitespace=True,
        treat_null_as_equal=True,
    )

    assert normalize_value("  ABC  ", settings) == "abc"
    assert normalize_value("", settings) is None
    assert normalize_value(None, settings) is None


def test_stable_bucket_for_key_is_repeatable() -> None:
    key = ("customer-1", "2026-01-01")
    assert stable_bucket_for_key(key, 64) == stable_bucket_for_key(key, 64)


def test_normalized_pair_can_treat_numeric_zero_and_null_as_equal() -> None:
    settings = NormalizationSettings(
        treat_null_as_equal=True,
        normalize_numeric_values=True,
        treat_null_as_zero_for_numeric=True,
        numeric_decimal_places=4,
        normalize_boolean_values=True,
    )

    assert normalized_pair("0", "0.000000000", settings) == ("0", "0")
    assert normalized_pair("0", None, settings) == ("0", "0")
    assert normalized_pair("14.3553", "14.355344355", settings) == ("14.3553", "14.3553")
    assert normalized_pair("1", "True", settings) == ("true", "true")
