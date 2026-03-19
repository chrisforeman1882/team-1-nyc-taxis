"""INT-01 — Gold layer integration tests.

Validates the persisted Gold fact table (A-02) and the buildability
of dimension views (A-01).  Also checks dashboard readiness (A-05).
"""

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.constants import DAY_NAME_MAP, TIME_PERIOD_BINS
from src.transforms import build_dim_location, build_dim_time

EXPECTED_FACT_COLUMNS = [
    "pickup_zone",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "trip_count",
    "total_revenue",
    "avg_fare_amount",
    "avg_trip_distance",
    "avg_trip_duration_min",
    "avg_tip_amount",
    "total_passengers",
]

FACT_METRIC_COLUMNS = [
    "trip_count",
    "total_revenue",
    "avg_fare_amount",
    "avg_trip_distance",
    "avg_trip_duration_min",
    "avg_tip_amount",
    "total_passengers",
]

FACT_GRAIN_COLUMNS = ["pickup_zone", "hour_of_day", "day_of_week"]


# ── Fact table schema & content ───────────────────────────────────────────


class TestGoldFactSchema:
    """Gold fact table must have the expected columns."""

    def test_table_is_not_empty(self, gold_fact_df: DataFrame):
        count = gold_fact_df.count()
        assert count > 0, "Gold fact table is empty"

    @pytest.mark.parametrize("column", EXPECTED_FACT_COLUMNS)
    def test_expected_column_exists(
        self, gold_fact_df: DataFrame, column: str
    ):
        assert column in gold_fact_df.columns, (
            f"Column '{column}' missing from Gold fact table. "
            f"Available: {gold_fact_df.columns}"
        )


class TestGoldFactGrain:
    """Grain must be unique: one row per (pickup_zone, hour_of_day, day_of_week)."""

    def test_no_duplicate_grain(self, gold_fact_df: DataFrame):
        total = gold_fact_df.count()
        distinct = gold_fact_df.select(FACT_GRAIN_COLUMNS).distinct().count()
        dupes = total - distinct
        assert dupes == 0, (
            f"{dupes:,} duplicate grain rows in Gold fact table"
        )

    def test_no_null_pickup_zone(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(F.col("pickup_zone").isNull()).count()
        assert bad == 0, f"{bad:,} rows with NULL pickup_zone in fact"

    def test_no_empty_pickup_zone(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(F.col("pickup_zone") == "").count()
        assert bad == 0, f"{bad:,} rows with empty-string pickup_zone"

    def test_hour_of_day_range(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(
            (F.col("hour_of_day") < 0) | (F.col("hour_of_day") > 23)
        ).count()
        assert bad == 0, f"{bad:,} rows with hour_of_day outside 0–23"

    def test_day_of_week_range(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(
            (F.col("day_of_week") < 1) | (F.col("day_of_week") > 7)
        ).count()
        assert bad == 0, f"{bad:,} rows with day_of_week outside 1–7"


class TestGoldFactMetrics:
    """All metric columns must be non-negative; trip_count must be > 0."""

    def test_trip_count_positive(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(F.col("trip_count") <= 0).count()
        assert bad == 0, f"{bad:,} rows with trip_count <= 0"

    @pytest.mark.parametrize("column", FACT_METRIC_COLUMNS)
    def test_metric_non_negative(
        self, gold_fact_df: DataFrame, column: str
    ):
        bad = gold_fact_df.filter(F.col(column) < 0).count()
        assert bad == 0, f"{bad:,} rows with {column} < 0"

    def test_total_revenue_not_null(self, gold_fact_df: DataFrame):
        bad = gold_fact_df.filter(F.col("total_revenue").isNull()).count()
        assert bad == 0, f"{bad:,} rows with NULL total_revenue"


# ── Dimension views ───────────────────────────────────────────────────────


class TestDimTime:
    """dim_time must have exactly 168 rows (24 hours × 7 days)."""

    def test_row_count(self, spark: SparkSession):
        dim_time = build_dim_time(spark)
        assert dim_time.count() == 168, (
            f"dim_time has {dim_time.count()} rows, expected 168"
        )

    def test_expected_columns(self, spark: SparkSession):
        dim_time = build_dim_time(spark)
        expected = {
            "hour_of_day",
            "day_of_week",
            "day_name",
            "is_weekend",
            "time_period",
        }
        assert set(dim_time.columns) == expected

    def test_all_day_names_present(self, spark: SparkSession):
        dim_time = build_dim_time(spark)
        actual_names = {
            row["day_name"]
            for row in dim_time.select("day_name").distinct().collect()
        }
        expected_names = set(DAY_NAME_MAP.values())
        assert actual_names == expected_names

    def test_all_time_periods_present(self, spark: SparkSession):
        dim_time = build_dim_time(spark)
        actual_periods = {
            row["time_period"]
            for row in dim_time.select("time_period").distinct().collect()
        }
        expected_periods = set(TIME_PERIOD_BINS.keys())
        assert actual_periods == expected_periods


class TestDimLocation:
    """dim_location must be buildable from Silver and contain valid zones."""

    def test_not_empty(self, silver_df: DataFrame):
        dim_loc = build_dim_location(silver_df)
        assert dim_loc.count() > 0, "dim_location is empty"

    def test_expected_columns(self, silver_df: DataFrame):
        dim_loc = build_dim_location(silver_df)
        expected = {"zone_id", "zone_lat", "zone_lon"}
        assert set(dim_loc.columns) == expected

    def test_no_null_coordinates(self, silver_df: DataFrame):
        dim_loc = build_dim_location(silver_df)
        bad = dim_loc.filter(
            F.col("zone_lat").isNull() | F.col("zone_lon").isNull()
        ).count()
        assert bad == 0, f"{bad} zones with NULL lat/lon"

    def test_unique_zone_ids(self, silver_df: DataFrame):
        dim_loc = build_dim_location(silver_df)
        total = dim_loc.count()
        distinct = dim_loc.select("zone_id").distinct().count()
        assert total == distinct, (
            f"dim_location has {total - distinct} duplicate zone_ids"
        )


# ── Dashboard readiness (A-05) ────────────────────────────────────────────


class TestDashboardReadiness:
    """Gold fact table must support the A-05 dashboard visualisations."""

    def test_heatmap_columns_present(self, gold_fact_df: DataFrame):
        """Demand heatmap needs pickup_zone, hour_of_day, trip_count."""
        for col in ["pickup_zone", "hour_of_day", "trip_count"]:
            assert col in gold_fact_df.columns, (
                f"Dashboard heatmap requires '{col}'"
            )

    def test_revenue_chart_columns_present(self, gold_fact_df: DataFrame):
        """Revenue-by-hour chart needs hour_of_day, total_revenue."""
        for col in ["hour_of_day", "total_revenue"]:
            assert col in gold_fact_df.columns, (
                f"Revenue chart requires '{col}'"
            )

    def test_duration_chart_columns_present(self, gold_fact_df: DataFrame):
        """Duration-by-day chart needs day_of_week, avg_trip_duration_min,
        trip_count (for weighting)."""
        for col in ["day_of_week", "avg_trip_duration_min", "trip_count"]:
            assert col in gold_fact_df.columns, (
                f"Duration chart requires '{col}'"
            )

    def test_kpi_cards_computable(self, gold_fact_df: DataFrame):
        """KPI cards require aggregatable trip_count and total_revenue."""
        kpis = gold_fact_df.agg(
            F.sum("trip_count").alias("total_trips"),
            F.sum("total_revenue").alias("total_revenue"),
        ).first()
        assert kpis["total_trips"] is not None and kpis["total_trips"] > 0
        assert (
            kpis["total_revenue"] is not None and kpis["total_revenue"] > 0
        )
