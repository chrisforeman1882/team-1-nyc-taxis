"""INT-01 — Silver layer integration tests.

Validates the persisted Silver Delta table against:
  1. Structural guarantees (I-03)
  2. Business-rule guarantees (I-04)
  3. Derived-column guarantees (I-05)
  4. Referential integrity (I-06)
"""

import pytest
from pyspark.sql import DataFrame, functions as F

from src.constants import (
    MAX_FARE_AMOUNT,
    MAX_TIP_AMOUNT,
    MAX_TOTAL_AMOUNT,
    MAX_TRIP_DISTANCE,
    MAX_TRIP_DURATION_MIN,
    REQUIRED_COLUMNS,
    VALID_EXTRA_VALUES,
    VALID_RATE_CODES,
)

# Full expected Silver schema (26 columns after I-03 + I-05)
EXPECTED_SILVER_COLUMNS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_longitude",
    "pickup_latitude",
    "rate_code_id",
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
    "_ingested_at",
    "pickup_zone",
    "dropoff_zone",
    "trip_duration_min",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
]


# ── 1. Structural checks (I-03) ────────────────────────────────────────────


class TestSilverStructure:
    """Silver table must exist, be non-empty, and have the expected schema."""

    def test_table_is_not_empty(self, silver_df: DataFrame):
        count = silver_df.count()
        assert count > 0, "Silver table is empty"

    @pytest.mark.parametrize("column", EXPECTED_SILVER_COLUMNS)
    def test_expected_column_exists(self, silver_df: DataFrame, column: str):
        assert column in silver_df.columns, (
            f"Column '{column}' missing from Silver. Available: {silver_df.columns}"
        )

    def test_rescued_data_dropped(self, silver_df: DataFrame):
        """_rescued_data should be consumed and dropped by I-03."""
        assert "_rescued_data" not in silver_df.columns

    @pytest.mark.parametrize("column", REQUIRED_COLUMNS)
    def test_no_nulls_in_required_columns(self, silver_df: DataFrame, column: str):
        null_count = silver_df.filter(F.col(column).isNull()).count()
        assert null_count == 0, (
            f"Column '{column}' has {null_count:,} NULLs (expected 0)"
        )


# ── 2. Business-rule checks (I-04) ─────────────────────────────────────────


class TestSilverBusinessRules:
    """I-04 guarantees: outliers removed, monetary values capped."""

    def test_trip_distance_positive(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("trip_distance") <= 0).count()
        assert bad == 0, f"{bad:,} rows with trip_distance <= 0"

    def test_trip_distance_within_max(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("trip_distance") > MAX_TRIP_DISTANCE).count()
        assert bad == 0, f"{bad:,} rows with trip_distance > {MAX_TRIP_DISTANCE}"

    def test_fare_amount_positive(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("fare_amount") <= 0).count()
        assert bad == 0, f"{bad:,} rows with fare_amount <= 0"

    def test_fare_amount_within_max(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("fare_amount") > MAX_FARE_AMOUNT).count()
        assert bad == 0, f"{bad:,} rows with fare_amount > {MAX_FARE_AMOUNT}"

    def test_total_amount_non_negative(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("total_amount") < 0).count()
        assert bad == 0, f"{bad:,} rows with total_amount < 0"

    def test_total_amount_within_max(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("total_amount") > MAX_TOTAL_AMOUNT).count()
        assert bad == 0, f"{bad:,} rows with total_amount > {MAX_TOTAL_AMOUNT}"

    def test_passenger_count_positive(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("passenger_count") <= 0).count()
        assert bad == 0, f"{bad:,} rows with passenger_count <= 0"

    def test_tip_amount_non_negative(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("tip_amount") < 0).count()
        assert bad == 0, f"{bad:,} rows with tip_amount < 0"

    def test_tip_amount_within_max(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("tip_amount") > MAX_TIP_AMOUNT).count()
        assert bad == 0, f"{bad:,} rows with tip_amount > {MAX_TIP_AMOUNT}"

    def test_extra_values_valid_or_null(self, silver_df: DataFrame):
        bad = silver_df.filter(
            F.col("extra").isNotNull() & ~F.col("extra").isin(list(VALID_EXTRA_VALUES))
        ).count()
        assert bad == 0, (
            f"{bad:,} rows with extra not in {VALID_EXTRA_VALUES} and not NULL"
        )


# ── 3. Derived-column checks (I-05) ────────────────────────────────────────


class TestSilverDerivedColumns:
    """I-05 derived columns must be present, non-null, and in valid ranges."""

    def test_trip_duration_min_not_null(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("trip_duration_min").isNull()).count()
        assert bad == 0, f"{bad:,} NULLs in trip_duration_min"

    def test_trip_duration_min_positive(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("trip_duration_min") <= 0).count()
        assert bad == 0, f"{bad:,} rows with trip_duration_min <= 0"

    def test_trip_duration_min_within_max(self, silver_df: DataFrame):
        bad = silver_df.filter(
            F.col("trip_duration_min") > MAX_TRIP_DURATION_MIN
        ).count()
        assert bad == 0, (
            f"{bad:,} rows with trip_duration_min > {MAX_TRIP_DURATION_MIN}"
        )

    def test_hour_of_day_not_null(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("hour_of_day").isNull()).count()
        assert bad == 0, f"{bad:,} NULLs in hour_of_day"

    def test_hour_of_day_range(self, silver_df: DataFrame):
        bad = silver_df.filter(
            (F.col("hour_of_day") < 0) | (F.col("hour_of_day") > 23)
        ).count()
        assert bad == 0, f"{bad:,} rows with hour_of_day outside 0–23"

    def test_day_of_week_not_null(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("day_of_week").isNull()).count()
        assert bad == 0, f"{bad:,} NULLs in day_of_week"

    def test_day_of_week_range(self, silver_df: DataFrame):
        bad = silver_df.filter(
            (F.col("day_of_week") < 1) | (F.col("day_of_week") > 7)
        ).count()
        assert bad == 0, f"{bad:,} rows with day_of_week outside 1–7"

    def test_is_weekend_not_null(self, silver_df: DataFrame):
        bad = silver_df.filter(F.col("is_weekend").isNull()).count()
        assert bad == 0, f"{bad:,} NULLs in is_weekend"

    def test_is_weekend_consistent_with_day_of_week(self, silver_df: DataFrame):
        """is_weekend=True iff day_of_week in (1=Sun, 7=Sat)."""
        bad = silver_df.filter(
            (F.col("is_weekend") & ~F.col("day_of_week").isin(1, 7))
            | (~F.col("is_weekend") & F.col("day_of_week").isin(1, 7))
        ).count()
        assert bad == 0, (
            f"{bad:,} rows where is_weekend is inconsistent with day_of_week"
        )


# ── 4. Referential integrity (I-06) ────────────────────────────────────────


class TestSilverReferentialIntegrity:
    """Categorical codes must be within documented valid sets."""

    def test_payment_type_valid(self, silver_df: DataFrame):
        bad = silver_df.filter(
            F.col("payment_type").isNotNull()
            & ~F.col("payment_type").isin([1, 2, 3, 4, 5, 6])
        ).count()
        assert bad == 0, f"{bad:,} rows with invalid payment_type"

    def test_rate_code_id_valid_or_null(self, silver_df: DataFrame):
        bad = silver_df.filter(
            F.col("rate_code_id").isNotNull()
            & ~F.col("rate_code_id").isin(list(VALID_RATE_CODES))
        ).count()
        assert bad == 0, f"{bad:,} rows with invalid rate_code_id"

    def test_vendor_id_valid(self, silver_df: DataFrame):
        bad = silver_df.filter(
            F.col("vendor_id").isNotNull() & ~F.col("vendor_id").isin([1, 2])
        ).count()
        assert bad == 0, f"{bad:,} rows with invalid vendor_id"
