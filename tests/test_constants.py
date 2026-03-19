"""Unit tests for src/constants.py.

Validates internal consistency of lookup maps, threshold sanity,
and column list completeness.  No Spark session required.
"""

import pytest

from src.constants import (
    BRONZE_TABLE,
    COLUMN_RENAME_MAP,
    DATETIME_COLUMNS,
    DAY_NAME_MAP,
    GOLD_FACT_TABLE,
    MAX_FARE_AMOUNT,
    MAX_TIP_AMOUNT,
    MAX_TOTAL_AMOUNT,
    MAX_TRIP_DISTANCE,
    MAX_TRIP_DURATION_MIN,
    ML_FEATURE_COLUMNS,
    ML_TARGET_COLUMN,
    ML_TEST_SIZE,
    NUMERIC_CAST_MAP,
    PAYMENT_TYPE_MAP,
    RATE_CODE_MAP,
    REQUIRED_COLUMNS,
    SILVER_TABLE,
    TIME_PERIOD_BINS,
    VALID_EXTRA_VALUES,
    VALID_RATE_CODES,
)


# ── Lookup maps ──────────────────────────────────────────────────────────


class TestDayNameMap:
    def test_has_seven_entries(self):
        assert len(DAY_NAME_MAP) == 7

    def test_keys_are_1_through_7(self):
        assert set(DAY_NAME_MAP.keys()) == {1, 2, 3, 4, 5, 6, 7}

    def test_sunday_is_1(self):
        """Spark dayofweek convention: 1 = Sunday."""
        assert DAY_NAME_MAP[1] == "Sunday"

    def test_saturday_is_7(self):
        assert DAY_NAME_MAP[7] == "Saturday"

    def test_all_values_unique(self):
        assert len(set(DAY_NAME_MAP.values())) == 7


class TestTimePeriodBins:
    def test_four_periods(self):
        assert len(TIME_PERIOD_BINS) == 4

    def test_covers_all_24_hours(self):
        covered = set()
        for start, end in TIME_PERIOD_BINS.values():
            covered.update(range(start, end + 1))
        assert covered == set(range(24))

    def test_no_overlapping_hours(self):
        all_hours = []
        for start, end in TIME_PERIOD_BINS.values():
            all_hours.extend(range(start, end + 1))
        assert len(all_hours) == len(set(all_hours)), "Overlapping hour bins"


class TestPaymentTypeMap:
    def test_has_codes_0_through_6(self):
        assert set(PAYMENT_TYPE_MAP.keys()) == {0, 1, 2, 3, 4, 5, 6}

    def test_credit_card_is_1(self):
        assert PAYMENT_TYPE_MAP[1] == "Credit card"

    def test_cash_is_2(self):
        assert PAYMENT_TYPE_MAP[2] == "Cash"

    def test_all_values_are_strings(self):
        assert all(isinstance(v, str) for v in PAYMENT_TYPE_MAP.values())


class TestRateCodeMap:
    def test_keys_match_valid_rate_codes(self):
        assert set(RATE_CODE_MAP.keys()) == VALID_RATE_CODES

    def test_standard_is_1(self):
        assert RATE_CODE_MAP[1] == "Standard"

    def test_jfk_is_2(self):
        assert RATE_CODE_MAP[2] == "JFK"


# ── Thresholds ───────────────────────────────────────────────────────────


class TestOutlierThresholds:
    def test_max_trip_distance_positive(self):
        assert MAX_TRIP_DISTANCE > 0

    def test_max_fare_amount_positive(self):
        assert MAX_FARE_AMOUNT > 0

    def test_max_total_exceeds_max_fare(self):
        assert MAX_TOTAL_AMOUNT >= MAX_FARE_AMOUNT

    def test_max_tip_positive(self):
        assert MAX_TIP_AMOUNT > 0

    def test_max_duration_is_24h(self):
        assert MAX_TRIP_DURATION_MIN == 1440

    def test_valid_extra_contains_zero(self):
        assert 0.0 in VALID_EXTRA_VALUES


# ── Column lists ─────────────────────────────────────────────────────────


class TestColumnLists:
    def test_required_columns_non_empty(self):
        assert len(REQUIRED_COLUMNS) > 0

    def test_datetime_columns_are_two(self):
        assert len(DATETIME_COLUMNS) == 2
        assert "tpep_pickup_datetime" in DATETIME_COLUMNS
        assert "tpep_dropoff_datetime" in DATETIME_COLUMNS

    def test_numeric_cast_map_includes_payment_type(self):
        assert "payment_type" in NUMERIC_CAST_MAP

    def test_column_rename_map_has_vendor_id(self):
        assert "VendorID" in COLUMN_RENAME_MAP

    def test_ml_target_not_in_features(self):
        """Target column must not appear in feature list (leakage)."""
        assert ML_TARGET_COLUMN not in ML_FEATURE_COLUMNS

    def test_ml_features_non_empty(self):
        assert len(ML_FEATURE_COLUMNS) > 0

    def test_ml_test_size_between_0_and_1(self):
        assert 0 < ML_TEST_SIZE < 1


# ── Table paths ─────────────────────────────────────────────────────────


class TestTablePaths:
    @pytest.mark.parametrize("table", [BRONZE_TABLE, SILVER_TABLE, GOLD_FACT_TABLE])
    def test_table_is_three_part_name(self, table: str):
        """Unity Catalog tables must be catalog.schema.table."""
        parts = table.replace("`", "").split(".")
        assert len(parts) == 3, f"Expected 3-part name, got: {table}"

    def test_all_tables_same_catalog_and_schema(self):
        def _normalise(t: str) -> str:
            return ".".join(t.replace("`", "").split(".")[:2])

        assert _normalise(BRONZE_TABLE) == _normalise(SILVER_TABLE)
        assert _normalise(SILVER_TABLE) == _normalise(GOLD_FACT_TABLE)
