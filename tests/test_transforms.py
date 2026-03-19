"""Unit tests for src/transforms.py.

Uses a local SparkSession with small synthetic DataFrames.
No Databricks cluster or Unity Catalog required.

Note: Spark Connect (DBR 13.3+) cannot infer types from rows with
None values.  Tests that require None use explicit StructType schemas
and pass plain tuples (not Row objects) to ``createDataFrame``.
"""

import datetime as dt

import pytest
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql import types as T

from src.constants import (
    MAX_FARE_AMOUNT,
    MAX_TIP_AMOUNT,
    MAX_TOTAL_AMOUNT,
    MAX_TRIP_DISTANCE,
    MAX_TRIP_DURATION_MIN,
    VALID_EXTRA_VALUES,
)
from src.transforms import (
    add_derived_columns,
    add_zone_bins,
    build_dim_location,
    build_dim_time,
    build_fact_trips,
    cap_monetary_outliers,
    cast_datetime_columns,
    cast_numeric_columns,
    clean_gps_coordinates,
    deduplicate,
    drop_corrupt_rows,
    drop_duration_anomalies,
    drop_extreme_outliers,
    drop_invalid_fares,
    drop_zero_distance_trips,
    drop_zero_passenger_trips,
    recover_rate_code_id,
    standardise_column_names,
)


# ── Helpers ──────────────────────────────────────────────────────────────

# Column order for _TRIP_SCHEMA — must match _make_trip_row() key order.
_TRIP_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "pickup_latitude",
    "pickup_longitude", "RateCodeID", "store_and_fwd_flag",
    "dropoff_latitude", "dropoff_longitude", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]

_TRIP_SCHEMA = T.StructType([
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("tpep_pickup_datetime", T.TimestampType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("pickup_latitude", T.DoubleType()),
    T.StructField("pickup_longitude", T.DoubleType()),
    T.StructField("RateCodeID", T.IntegerType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("dropoff_latitude", T.DoubleType()),
    T.StructField("dropoff_longitude", T.DoubleType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
])

_RESCUED_SCHEMA = T.StructType([
    T.StructField("rate_code_id", T.IntegerType(), True),
    T.StructField("_rescued_data", T.StringType(), True),
])


def _make_trip_row(**overrides):
    """Return a dict with sensible defaults for a trip row."""
    base = dict(zip(_TRIP_COLUMNS, [
        1,
        dt.datetime(2016, 1, 15, 8, 30, 0),
        dt.datetime(2016, 1, 15, 8, 45, 0),
        2, 3.5, 40.75, -73.98, 1, "N", 40.76, -73.97,
        1, 12.0, 0.5, 0.5, 2.0, 0.0, 0.3, 15.3,
    ]))
    base.update(overrides)
    return base


def _trip_df(spark, rows, has_nulls=False):
    """Create a DataFrame from a list of trip-row dicts.

    Set ``has_nulls=True`` when any field may be None — uses explicit
    schema + plain tuples (Spark Connect cannot infer types from None).
    """
    if has_nulls:
        tuples = [tuple(r[c] for c in _TRIP_COLUMNS) for r in rows]
        return spark.createDataFrame(tuples, schema=_TRIP_SCHEMA)
    return spark.createDataFrame([Row(**r) for r in rows])


# ── I-03 transforms ─────────────────────────────────────────────────────


class TestStandardiseColumnNames:
    def test_renames_vendor_id(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = standardise_column_names(df)
        assert "vendor_id" in result.columns
        assert "VendorID" not in result.columns

    def test_renames_rate_code_id(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = standardise_column_names(df)
        assert "rate_code_id" in result.columns
        assert "RateCodeID" not in result.columns

    def test_preserves_other_columns(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = standardise_column_names(df)
        assert "trip_distance" in result.columns


class TestRecoverRateCodeId:
    def test_coalesces_from_rescued_data(self, local_spark):
        df = local_spark.createDataFrame(
            [(None, '{"RatecodeID": 2}')], schema=_RESCUED_SCHEMA,
        )
        result = recover_rate_code_id(df)
        assert result.collect()[0]["rate_code_id"] == 2

    def test_maps_code_99_to_null(self, local_spark):
        df = local_spark.createDataFrame(
            [(99, None)], schema=_RESCUED_SCHEMA,
        )
        result = recover_rate_code_id(df)
        assert result.collect()[0]["rate_code_id"] is None

    def test_drops_rescued_data_column(self, local_spark):
        df = local_spark.createDataFrame(
            [(1, None)], schema=_RESCUED_SCHEMA,
        )
        result = recover_rate_code_id(df)
        assert "_rescued_data" not in result.columns

    def test_noop_without_rescued_column(self, local_spark):
        df = local_spark.createDataFrame([(1,)], ["rate_code_id"])
        result = recover_rate_code_id(df)
        assert result.collect()[0]["rate_code_id"] == 1


class TestDeduplicate:
    def test_removes_exact_duplicates(self, local_spark):
        row = _make_trip_row()
        df = _trip_df(local_spark, [row, row])
        result = deduplicate(df)
        assert result.count() == 1

    def test_keeps_distinct_rows(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(trip_distance=1.0),
            _make_trip_row(trip_distance=2.0),
        ])
        result = deduplicate(df)
        assert result.count() == 2


class TestCastDatetimeColumns:
    def test_casts_to_timestamp(self, local_spark):
        df = local_spark.createDataFrame(
            [("2016-01-15 08:30:00", "2016-01-15 08:45:00")],
            ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        )
        result = cast_datetime_columns(df)
        dtype_name = result.schema["tpep_pickup_datetime"].dataType.typeName()
        assert dtype_name == "timestamp"


class TestCastNumericColumns:
    def test_casts_payment_type_to_int(self, local_spark):
        df = local_spark.createDataFrame([("1",)], ["payment_type"])
        result = cast_numeric_columns(df)
        assert "int" in str(result.schema["payment_type"].dataType).lower()

    def test_casts_trip_distance_to_double(self, local_spark):
        df = local_spark.createDataFrame([("3.5",)], ["trip_distance"])
        result = cast_numeric_columns(df)
        assert "double" in str(result.schema["trip_distance"].dataType).lower()


class TestDropCorruptRows:
    def test_drops_null_required_field(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(),
            _make_trip_row(fare_amount=None),
        ], has_nulls=True)
        result = drop_corrupt_rows(df)
        assert result.count() == 1

    def test_keeps_complete_rows(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = drop_corrupt_rows(df)
        assert result.count() == 1


class TestCleanGpsCoordinates:
    def test_nulls_out_zero_zero(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=0.0, pickup_longitude=0.0),
        ])
        result = clean_gps_coordinates(df)
        row = result.collect()[0]
        assert row["pickup_latitude"] is None
        assert row["pickup_longitude"] is None

    def test_nulls_out_of_bbox(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=50.0, pickup_longitude=-73.98),
        ])
        result = clean_gps_coordinates(df)
        assert result.collect()[0]["pickup_latitude"] is None

    def test_keeps_valid_coordinates(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=40.75, pickup_longitude=-73.98),
        ])
        result = clean_gps_coordinates(df)
        row = result.collect()[0]
        assert row["pickup_latitude"] == pytest.approx(40.75)


class TestAddZoneBins:
    def test_adds_pickup_zone(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_zone_bins(df)
        assert "pickup_zone" in result.columns
        assert "dropoff_zone" in result.columns

    def test_null_coords_produce_empty_zone(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=None, pickup_longitude=None),
        ], has_nulls=True)
        result = add_zone_bins(df)
        zone = result.collect()[0]["pickup_zone"]
        # concat_ws with NULLs produces empty string
        assert zone == "" or zone is None


# ── I-04 transforms ─────────────────────────────────────────────────────


class TestDropZeroDistanceTrips:
    def test_drops_zero_distance(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(trip_distance=0.0),
            _make_trip_row(trip_distance=5.0),
        ])
        result = drop_zero_distance_trips(df)
        assert result.count() == 1

    def test_keeps_positive_distance(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(trip_distance=0.1)])
        assert drop_zero_distance_trips(df).count() == 1


class TestDropInvalidFares:
    def test_drops_zero_fare(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(fare_amount=0.0)])
        assert drop_invalid_fares(df).count() == 0

    def test_drops_negative_total(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(total_amount=-5.0)])
        assert drop_invalid_fares(df).count() == 0

    def test_keeps_valid_fare(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(fare_amount=12.0)])
        assert drop_invalid_fares(df).count() == 1


class TestDropZeroPassengerTrips:
    def test_drops_zero_passengers(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(passenger_count=0)])
        assert drop_zero_passenger_trips(df).count() == 0

    def test_keeps_nonzero_passengers(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(passenger_count=3)])
        assert drop_zero_passenger_trips(df).count() == 1


class TestDropExtremeOutliers:
    def test_drops_extreme_distance(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(trip_distance=MAX_TRIP_DISTANCE + 1),
        ])
        assert drop_extreme_outliers(df).count() == 0

    def test_drops_extreme_fare(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(fare_amount=MAX_FARE_AMOUNT + 1),
        ])
        assert drop_extreme_outliers(df).count() == 0

    def test_drops_extreme_total(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(total_amount=MAX_TOTAL_AMOUNT + 1),
        ])
        assert drop_extreme_outliers(df).count() == 0

    def test_keeps_within_limits(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        assert drop_extreme_outliers(df).count() == 1


class TestDropDurationAnomalies:
    def test_drops_negative_duration(self, local_spark):
        """Dropoff before pickup → negative duration."""
        df = _trip_df(local_spark, [_make_trip_row(
            tpep_pickup_datetime=dt.datetime(2016, 1, 15, 9, 0, 0),
            tpep_dropoff_datetime=dt.datetime(2016, 1, 15, 8, 0, 0),
        )])
        assert drop_duration_anomalies(df).count() == 0

    def test_drops_zero_duration(self, local_spark):
        ts = dt.datetime(2016, 1, 15, 8, 0, 0)
        df = _trip_df(local_spark, [_make_trip_row(
            tpep_pickup_datetime=ts, tpep_dropoff_datetime=ts,
        )])
        assert drop_duration_anomalies(df).count() == 0

    def test_drops_over_24h(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(
            tpep_pickup_datetime=dt.datetime(2016, 1, 15, 8, 0, 0),
            tpep_dropoff_datetime=dt.datetime(2016, 1, 17, 8, 0, 0),
        )])
        assert drop_duration_anomalies(df).count() == 0

    def test_keeps_normal_duration(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        assert drop_duration_anomalies(df).count() == 1


class TestCapMonetaryOutliers:
    def test_negative_tip_set_to_zero(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(tip_amount=-5.0)])
        result = cap_monetary_outliers(df)
        assert result.collect()[0]["tip_amount"] == 0.0

    def test_extreme_tip_capped(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(tip_amount=999.0)])
        result = cap_monetary_outliers(df)
        assert result.collect()[0]["tip_amount"] == float(MAX_TIP_AMOUNT)

    def test_valid_tip_unchanged(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(tip_amount=5.0)])
        result = cap_monetary_outliers(df)
        assert result.collect()[0]["tip_amount"] == 5.0

    def test_invalid_extra_nulled(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row(extra=4.50)])
        result = cap_monetary_outliers(df)
        assert result.collect()[0]["extra"] is None

    def test_valid_extra_kept(self, local_spark):
        for val in VALID_EXTRA_VALUES:
            df = _trip_df(local_spark, [_make_trip_row(extra=val)])
            result = cap_monetary_outliers(df)
            assert result.collect()[0]["extra"] == val


# ── I-05 transforms ─────────────────────────────────────────────────────


class TestAddDerivedColumns:
    def test_adds_all_four_columns(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_derived_columns(df)
        for col in ["trip_duration_min", "hour_of_day", "day_of_week", "is_weekend"]:
            assert col in result.columns

    def test_duration_is_15_min(self, local_spark):
        """8:30 to 8:45 = 15 minutes."""
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_derived_columns(df)
        assert result.collect()[0]["trip_duration_min"] == pytest.approx(15.0)

    def test_hour_of_day(self, local_spark):
        """Pickup at 8:30 → hour_of_day = 8."""
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_derived_columns(df)
        assert result.collect()[0]["hour_of_day"] == 8

    def test_day_of_week_friday(self, local_spark):
        """2016-01-15 is a Friday → Spark dayofweek = 6."""
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_derived_columns(df)
        assert result.collect()[0]["day_of_week"] == 6

    def test_is_weekend_false_on_friday(self, local_spark):
        df = _trip_df(local_spark, [_make_trip_row()])
        result = add_derived_columns(df)
        assert result.collect()[0]["is_weekend"] is False

    def test_is_weekend_true_on_sunday(self, local_spark):
        """2016-01-17 is a Sunday."""
        df = _trip_df(local_spark, [_make_trip_row(
            tpep_pickup_datetime=dt.datetime(2016, 1, 17, 10, 0, 0),
            tpep_dropoff_datetime=dt.datetime(2016, 1, 17, 10, 15, 0),
        )])
        result = add_derived_columns(df)
        assert result.collect()[0]["is_weekend"] is True


# ── A-01 Gold transforms ─────────────────────────────────────────────────


class TestBuildFactTrips:
    @pytest.fixture()
    def silver_sample(self, local_spark):
        """Two trips in same zone/hour/day, one in different hour."""
        rows = [
            _make_trip_row(
                pickup_latitude=40.75, pickup_longitude=-73.98,
            ),
            _make_trip_row(
                pickup_latitude=40.75, pickup_longitude=-73.98,
            ),
            _make_trip_row(
                pickup_latitude=40.75, pickup_longitude=-73.98,
                tpep_pickup_datetime=dt.datetime(2016, 1, 15, 14, 0, 0),
                tpep_dropoff_datetime=dt.datetime(2016, 1, 15, 14, 20, 0),
            ),
        ]
        df = _trip_df(local_spark, rows)
        df = add_zone_bins(df)
        df = add_derived_columns(df)
        return df

    def test_aggregates_to_correct_grain(self, silver_sample):
        fact = build_fact_trips(silver_sample)
        # 2 rows at hour 8 + 1 row at hour 14 = 2 grain rows
        assert fact.count() == 2

    def test_trip_count_is_correct(self, silver_sample):
        fact = build_fact_trips(silver_sample)
        total = fact.agg(F.sum("trip_count")).collect()[0][0]
        assert total == 3

    def test_excludes_null_zones(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=None, pickup_longitude=None),
        ], has_nulls=True)
        df = add_zone_bins(df)
        df = add_derived_columns(df)
        fact = build_fact_trips(df)
        assert fact.count() == 0


class TestBuildDimTime:
    def test_row_count_168(self, local_spark):
        assert build_dim_time(local_spark).count() == 168

    def test_schema(self, local_spark):
        cols = set(build_dim_time(local_spark).columns)
        assert cols == {"hour_of_day", "day_of_week", "day_name", "is_weekend", "time_period"}


class TestBuildDimLocation:
    def test_extracts_distinct_zones(self, local_spark):
        df = _trip_df(local_spark, [
            _make_trip_row(pickup_latitude=40.75, pickup_longitude=-73.98),
            _make_trip_row(pickup_latitude=40.75, pickup_longitude=-73.98),
            _make_trip_row(pickup_latitude=40.76, pickup_longitude=-73.97),
        ])
        df = add_zone_bins(df)
        dim = build_dim_location(df)
        assert dim.count() == 2
        assert set(dim.columns) == {"zone_id", "zone_lat", "zone_lon"}
