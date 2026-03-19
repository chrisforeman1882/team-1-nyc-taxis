"""INT-01 — Cross-layer pipeline integration tests.

Validates data-flow integrity across the full Bronze → Silver → Gold
pipeline.  These tests ensure row counts reconcile across layers and
the aggregation contract is honoured.
"""

from pyspark.sql import DataFrame, functions as F


class TestBronzeToSilver:
    """Silver must have fewer rows than Bronze (cleaning drops rows)."""

    def test_silver_leq_bronze(
        self, bronze_df: DataFrame, silver_df: DataFrame
    ):
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        assert silver_count <= bronze_count, (
            f"Silver ({silver_count:,}) > Bronze ({bronze_count:,}); "
            f"cleaning should only remove rows"
        )

    def test_cleaning_removed_rows(
        self, bronze_df: DataFrame, silver_df: DataFrame
    ):
        """I-03 + I-04 should drop at least some rows."""
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        assert silver_count < bronze_count, (
            f"Silver ({silver_count:,}) == Bronze ({bronze_count:,}); "
            f"no rows removed by cleaning pipeline"
        )


class TestSilverToGold:
    """Gold fact trip_count must reconcile with Silver valid-zone rows."""

    def test_gold_trip_count_matches_silver_valid_zones(
        self, silver_df: DataFrame, gold_fact_df: DataFrame
    ):
        """SUM(trip_count) in Gold must equal Silver rows with non-empty
        pickup_zone (the Gold aggregation filter)."""
        silver_valid = silver_df.filter(
            F.col("pickup_zone").isNotNull()
            & (F.col("pickup_zone") != "")
        ).count()

        gold_trip_sum = gold_fact_df.agg(
            F.sum("trip_count")
        ).collect()[0][0]

        assert gold_trip_sum == silver_valid, (
            f"Gold SUM(trip_count)={gold_trip_sum:,} != "
            f"Silver valid-zone rows={silver_valid:,}"
        )

    def test_gold_row_count_within_theoretical_max(
        self, silver_df: DataFrame, gold_fact_df: DataFrame
    ):
        """Fact rows must be <= zones × 24 × 7 (theoretical maximum)."""
        distinct_zones = (
            silver_df.filter(
                F.col("pickup_zone").isNotNull()
                & (F.col("pickup_zone") != "")
            )
            .select("pickup_zone")
            .distinct()
            .count()
        )
        theoretical_max = distinct_zones * 24 * 7
        fact_count = gold_fact_df.count()

        assert fact_count <= theoretical_max, (
            f"Fact rows ({fact_count:,}) > theoretical max "
            f"({distinct_zones} zones × 24h × 7d = {theoretical_max:,})"
        )

    def test_gold_is_aggregation_of_silver(
        self, silver_df: DataFrame, gold_fact_df: DataFrame
    ):
        """Gold must have far fewer rows than Silver (aggregation ratio)."""
        silver_count = silver_df.count()
        gold_count = gold_fact_df.count()
        assert gold_count < silver_count, (
            f"Gold ({gold_count:,}) >= Silver ({silver_count:,}); "
            f"Gold should be an aggregated subset"
        )


class TestRevenueReconciliation:
    """Total revenue must reconcile between Silver and Gold."""

    def test_total_revenue_matches(
        self, silver_df: DataFrame, gold_fact_df: DataFrame
    ):
        """SUM(total_amount) for valid-zone Silver rows must equal
        SUM(total_revenue) in Gold fact table."""
        silver_revenue = (
            silver_df.filter(
                F.col("pickup_zone").isNotNull()
                & (F.col("pickup_zone") != "")
            )
            .agg(F.sum("total_amount"))
            .collect()[0][0]
        )

        gold_revenue = (
            gold_fact_df.agg(F.sum("total_revenue")).collect()[0][0]
        )

        # Allow tiny floating-point tolerance (< $1 on ~$2B total)
        diff = abs(silver_revenue - gold_revenue)
        assert diff < 1.0, (
            f"Revenue mismatch: Silver=${silver_revenue:,.2f}, "
            f"Gold=${gold_revenue:,.2f}, diff=${diff:,.2f}"
        )
