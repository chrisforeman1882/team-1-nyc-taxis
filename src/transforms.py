"""Silver transformation helpers for NYC Yellow Taxi data."""

from pyspark.sql import DataFrame, functions as F

from src.constants import (
    COLUMN_RENAME_MAP,
    DATETIME_COLUMNS,
    NYC_LAT_MAX,
    NYC_LAT_MIN,
    NYC_LON_MAX,
    NYC_LON_MIN,
    NUMERIC_CAST_MAP,
    REQUIRED_COLUMNS,
    VALID_RATE_CODES,
)


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
