"""Silver transformation helpers for NYC Yellow Taxi data."""

from pyspark.sql import DataFrame, functions as F

from src.constants import (
    COLUMN_RENAME_MAP,
    DATETIME_COLUMNS,
    MAX_FARE_AMOUNT,
    MAX_TIP_AMOUNT,
    MAX_TOTAL_AMOUNT,
    MAX_TRIP_DISTANCE,
    MAX_TRIP_DURATION_MIN,
    NYC_LAT_MAX,
    NYC_LAT_MIN,
    NYC_LON_MAX,
    NYC_LON_MIN,
    NUMERIC_CAST_MAP,
    REQUIRED_COLUMNS,
    VALID_EXTRA_VALUES,
    VALID_RATE_CODES,
)


# ── I-03 transforms ─────────────────────────────────────────────────────────


def standardise_column_names(df: DataFrame) -> DataFrame:
    """Rename columns from Bronze naming conventions to snake_case.

    Only renames columns present in COLUMN_RENAME_MAP; all other
    columns pass through unchanged.
    """
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def recover_rate_code_id(df: DataFrame) -> DataFrame:
    """Recover RateCodeID from _rescued_data JSON for 2016 rows.

    The 2016 CSVs use 'RatecodeID' (lowercase c) while 2015 uses
    'RateCodeID', causing a schema mismatch on ingest.  73% of rows
    have NULL rate_code_id with the real value in _rescued_data JSON.

    Also maps the anomalous code 99 (1,670 rows) to NULL.
    """
    if "_rescued_data" not in df.columns:
        return df

    rescued_rate = F.get_json_object(F.col("_rescued_data"), "$.RatecodeID").cast("int")

    df = df.withColumn(
        "rate_code_id",
        F.coalesce(F.col("rate_code_id"), rescued_rate),
    )

    # Map invalid code 99 -> NULL
    df = df.withColumn(
        "rate_code_id",
        F.when(
            F.col("rate_code_id").isin(list(VALID_RATE_CODES)),
            F.col("rate_code_id"),
        ),
    )

    # Drop _rescued_data — no longer needed after recovery
    df = df.drop("_rescued_data")
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove exact duplicate rows."""
    return df.dropDuplicates()


def cast_datetime_columns(df: DataFrame) -> DataFrame:
    """Cast datetime string columns to TimestampType."""
    for col_name in DATETIME_COLUMNS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.to_timestamp(col_name))
    return df


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """Cast numeric columns to their expected types (int / double)."""
    for col_name, target_type in NUMERIC_CAST_MAP.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def drop_corrupt_rows(df: DataFrame) -> DataFrame:
    """Drop rows where required columns are null.

    This covers structurally invalid rows (e.g. null timestamps, null fare).
    Business-rule outlier removal (zero distance, negative fares) is handled
    separately in I-04.
    """
    for col_name in REQUIRED_COLUMNS:
        if col_name in df.columns:
            df = df.filter(F.col(col_name).isNotNull())
    return df


def clean_gps_coordinates(df: DataFrame) -> DataFrame:
    """NULL-out GPS coordinates that are (0, 0) or outside the NYC bounding box.

    Finding #10: 1.55M pickup rows have (0,0) coords, plus ~15K-62K rows
    fall outside NYC bbox. These would create junk zone bins.
    """
    for lat_col, lon_col in [
        ("pickup_latitude", "pickup_longitude"),
        ("dropoff_latitude", "dropoff_longitude"),
    ]:
        if lat_col in df.columns and lon_col in df.columns:
            is_valid = (
                (F.col(lat_col) != 0.0)
                & (F.col(lon_col) != 0.0)
                & (F.col(lat_col).between(NYC_LAT_MIN, NYC_LAT_MAX))
                & (F.col(lon_col).between(NYC_LON_MIN, NYC_LON_MAX))
            )
            df = df.withColumn(lat_col, F.when(is_valid, F.col(lat_col))).withColumn(
                lon_col, F.when(is_valid, F.col(lon_col))
            )
    return df


def add_zone_bins(df: DataFrame) -> DataFrame:
    """Add grid-binned pickup/dropoff zone columns from lat/lon.

    The dataset uses raw lat/lon (pre-2016 format) rather than TLC taxi
    zone IDs.  We round to a fixed grid so Gold can aggregate 'per zone'
    without needing a shapefile spatial join.

    Rows with NULL lat/lon (from clean_gps_coordinates) get NULL zones.

    Adds:
        pickup_zone  (string): "lat_bin,lon_bin" for pickup location
        dropoff_zone (string): "lat_bin,lon_bin" for dropoff location
    """
    from src.constants import LAT_LON_BIN_SIZE

    for prefix, lat_col, lon_col in [
        ("pickup", "pickup_latitude", "pickup_longitude"),
        ("dropoff", "dropoff_latitude", "dropoff_longitude"),
    ]:
        if lat_col in df.columns and lon_col in df.columns:
            lat_bin = F.round(F.col(lat_col) / LAT_LON_BIN_SIZE) * LAT_LON_BIN_SIZE
            lon_bin = F.round(F.col(lon_col) / LAT_LON_BIN_SIZE) * LAT_LON_BIN_SIZE
            df = df.withColumn(
                f"{prefix}_zone",
                F.concat_ws(",", lat_bin.cast("string"), lon_bin.cast("string")),
            )
    return df


# ── I-04 transforms ─────────────────────────────────────────────────────────


def drop_zero_distance_trips(df: DataFrame) -> DataFrame:
    """Drop trips with zero distance (Finding #3: 564K rows, 0.60%).

    Zero-distance trips are likely cancellations or meter errors.
    Decision: drop — these cannot represent valid completed trips.
    """
    return df.filter(F.col("trip_distance") > 0)


def drop_invalid_fares(df: DataFrame) -> DataFrame:
    """Drop trips with non-positive fares or negative totals (Findings #4, #5).

    Finding #4: 62K rows with fare_amount <= 0 (0.07%)
    Finding #5: 34K rows with total_amount < 0 (0.04%, voided/disputed)
    Decision: drop — non-positive fares are structurally invalid;
    negative totals are voided/disputed trips.
    """
    return df.filter((F.col("fare_amount") > 0) & (F.col("total_amount") >= 0))


def drop_zero_passenger_trips(df: DataFrame) -> DataFrame:
    """Drop trips reporting zero passengers (Finding #6: 16K rows, 0.02%).

    Decision: drop rather than impute to 1. Only 0.02% of data, and
    passenger_count = 0 is structurally invalid for a completed trip.
    """
    return df.filter(F.col("passenger_count") > 0)


def drop_extreme_outliers(df: DataFrame) -> DataFrame:
    """Drop trips with extreme distance, fare, or total values (Findings #8, #12).

    Finding #8:  914 trips > 100 mi, 458 fares > $500
    Finding #12: 210 totals > $1,000 (max $3.95M)
    Decision: drop — these are likely data entry errors or GPS glitches.
    Thresholds defined in constants.py.
    """
    return df.filter(
        (F.col("trip_distance") <= MAX_TRIP_DISTANCE)
        & (F.col("fare_amount") <= MAX_FARE_AMOUNT)
        & (F.col("total_amount") <= MAX_TOTAL_AMOUNT)
    )


def drop_duration_anomalies(df: DataFrame) -> DataFrame:
    """Drop trips with impossible durations (Finding #11).

    Finding #11: 946 negative, 101K zero-second, 330 over 24 hours.
    Decision: drop negative/zero and > 24 h. Trips <= 24 h are retained
    because 3 h+ trips may include legitimate JFK/Newark flat-rate rides.

    Note: duration is computed inline for filtering only — the permanent
    trip_duration_min column is added in I-05.
    """
    duration_min = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0

    return df.filter((duration_min > 0) & (duration_min <= MAX_TRIP_DURATION_MIN))


def cap_monetary_outliers(df: DataFrame) -> DataFrame:
    """Cap or correct extreme monetary values (Finding #12).

    - Negative tip_amount (840 rows): set to 0 (row otherwise valid).
    - tip_amount > $200: cap at $200 (likely data entry errors; max was $3.95M).
    - extra surcharge: NULL out values not in {0, 0.5, 1.0}.
      The $4.50 value (168K rows) may be a legitimate later surcharge,
      but other irregular/negative values are data errors.

    Finding #7 (tip > 0 on non-card payments, 1,150 rows) is NOT handled
    here — those rows are kept as-is; tip-prediction models (BQ-4) should
    restrict training to payment_type = 1 (credit card) only.
    """
    # Negative tips → 0
    df = df.withColumn(
        "tip_amount",
        F.when(F.col("tip_amount") < 0, F.lit(0.0)).otherwise(F.col("tip_amount")),
    )

    # Cap extreme tips at threshold
    df = df.withColumn(
        "tip_amount",
        F.when(
            F.col("tip_amount") > MAX_TIP_AMOUNT, F.lit(float(MAX_TIP_AMOUNT))
        ).otherwise(F.col("tip_amount")),
    )

    # Clean extra surcharge — NULL out non-standard values
    df = df.withColumn(
        "extra",
        F.when(F.col("extra").isin(list(VALID_EXTRA_VALUES)), F.col("extra")),
    )

    return df
