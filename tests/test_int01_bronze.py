"""INT-01 — Bronze layer integration tests.

Validates that the Bronze Delta table (I-01) exists, is populated,
and retains the expected raw schema from the NYC Yellow Taxi CSVs.
"""

import pytest
from pyspark.sql import DataFrame

from src.constants import BRONZE_TABLE

# Columns expected in the raw source (pre-rename).
# Bronze is append-only / no transforms, so these should survive as-is
# (with possible rescued-data column from schema mismatch).
EXPECTED_RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_longitude",
    "pickup_latitude",
    "RateCodeID",
    "store_and_fwd_flag",
    "dropoff_longitude",
    "dropoff_latitude",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]


class TestBronzeTableExists:
    """Bronze table must exist and be readable."""

    def test_table_is_readable(self, spark):
        """spark.read.table() should not raise on the Bronze table."""
        df = spark.read.table(BRONZE_TABLE)
        assert df is not None

    def test_table_is_not_empty(self, bronze_df: DataFrame):
        count = bronze_df.count()
        assert count > 0, "Bronze table is empty (0 rows)"


class TestBronzeSchema:
    """Bronze schema should contain all expected raw columns."""

    @pytest.mark.parametrize("column", EXPECTED_RAW_COLUMNS)
    def test_expected_column_exists(self, bronze_df: DataFrame, column: str):
        assert column in bronze_df.columns, (
            f"Column '{column}' missing from Bronze. Available: {bronze_df.columns}"
        )

    def test_has_rescued_data_column(self, bronze_df: DataFrame):
        """Schema mismatch between 2015/2016 CSVs produces _rescued_data."""
        assert "_rescued_data" in bronze_df.columns, (
            "Expected '_rescued_data' column from 2015/2016 schema mismatch"
        )


class TestBronzeRowCount:
    """Bronze row count should be reasonable for the NYC taxi dataset."""

    def test_minimum_row_count(self, bronze_df: DataFrame):
        """Dataset contains Jan 2015 + Jan–Mar 2016 (~47M rows)."""
        count = bronze_df.count()
        assert count >= 40_000_000, (
            f"Bronze has only {count:,} rows; expected >= 40M "
            f"for the NYC Yellow Taxi dataset"
        )
