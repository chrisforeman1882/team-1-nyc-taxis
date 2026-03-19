"""Unit tests for src/validators.py.

Each validator is tested for both the pass case (returns quietly)
and the fail case (raises AssertionError with a descriptive message).
Uses a local SparkSession with synthetic DataFrames.
"""

import pytest
from pyspark.sql import SparkSession

from src.validators import (
    check_accepted_values,
    check_column_exists,
    check_max,
    check_no_duplicates,
    check_no_nulls,
    check_non_negative,
    check_not_empty,
    check_positive,
)


# ── check_not_empty ──────────────────────────────────────────────────────


class TestCheckNotEmpty:
    def test_pass_non_empty(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,)], ["x"])
        check_not_empty(df, "test")  # should not raise

    def test_fail_empty(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([], "x: int")
        with pytest.raises(AssertionError, match="empty"):
            check_not_empty(df, "test")


# ── check_no_nulls ───────────────────────────────────────────────────────


class TestCheckNoNulls:
    def test_pass_no_nulls(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,), (2,)], ["x"])
        check_no_nulls(df, "x")

    def test_fail_with_nulls(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,), (None,)], "x: int")
        with pytest.raises(AssertionError, match="NULL"):
            check_no_nulls(df, "x")


# ── check_positive ───────────────────────────────────────────────────────


class TestCheckPositive:
    def test_pass_all_positive(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,), (2,)], ["x"])
        check_positive(df, "x")

    def test_fail_with_zero(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(0,), (1,)], ["x"])
        with pytest.raises(AssertionError, match="non-positive"):
            check_positive(df, "x")

    def test_fail_with_negative(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(-1,), (1,)], ["x"])
        with pytest.raises(AssertionError, match="non-positive"):
            check_positive(df, "x")


# ── check_non_negative ───────────────────────────────────────────────────


class TestCheckNonNegative:
    def test_pass_with_zero(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(0,), (1,)], ["x"])
        check_non_negative(df, "x")

    def test_fail_with_negative(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(-1,), (1,)], ["x"])
        with pytest.raises(AssertionError, match="negative"):
            check_non_negative(df, "x")


# ── check_max ────────────────────────────────────────────────────────────


class TestCheckMax:
    def test_pass_within_limit(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(50,), (100,)], ["x"])
        check_max(df, "x", 100)

    def test_fail_exceeds_limit(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(50,), (101,)], ["x"])
        with pytest.raises(AssertionError, match="values > 100"):
            check_max(df, "x", 100)


# ── check_accepted_values ────────────────────────────────────────────────


class TestCheckAcceptedValues:
    def test_pass_all_valid(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,), (2,)], ["x"])
        check_accepted_values(df, "x", [1, 2, 3])

    def test_pass_nulls_ignored(self, local_spark: SparkSession):
        """NULL values should be skipped (only non-null checked)."""
        df = local_spark.createDataFrame([(1,), (None,)], "x: int")
        check_accepted_values(df, "x", [1])

    def test_fail_invalid_value(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,), (99,)], ["x"])
        with pytest.raises(AssertionError, match="outside"):
            check_accepted_values(df, "x", [1, 2, 3])


# ── check_no_duplicates ──────────────────────────────────────────────────


class TestCheckNoDuplicates:
    def test_pass_no_dupes(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1, "a"), (2, "b")], ["x", "y"])
        check_no_duplicates(df, ["x", "y"])

    def test_fail_with_dupes(self, local_spark: SparkSession):
        df = local_spark.createDataFrame(
            [(1, "a"), (1, "a"), (2, "b")], ["x", "y"]
        )
        with pytest.raises(AssertionError, match="duplicate"):
            check_no_duplicates(df, ["x", "y"])


# ── check_column_exists ──────────────────────────────────────────────────


class TestCheckColumnExists:
    def test_pass_column_present(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,)], ["x"])
        check_column_exists(df, "x")

    def test_fail_column_missing(self, local_spark: SparkSession):
        df = local_spark.createDataFrame([(1,)], ["x"])
        with pytest.raises(AssertionError, match="not found"):
            check_column_exists(df, "missing_col")
