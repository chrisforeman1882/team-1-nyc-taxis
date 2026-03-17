"""Silver transformation helpers for NYC Yellow Taxi data."""

from __future__ import annotations


def standardise_column_names(df):
    """Lower-case all column names and replace spaces with underscores."""
    return df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])


def drop_zero_distance_trips(df):
    """Remove trips where trip_distance is zero or negative."""
    from pyspark.sql import functions as F

    return df.filter(F.col("trip_distance") > 0)


def drop_invalid_fares(df):
    """Remove rows where fare_amount is zero or negative."""
    from pyspark.sql import functions as F

    return df.filter(F.col("fare_amount") > 0)


def cast_datetime_columns(df):
    """Cast pickup/dropoff strings to TimestampType."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    return (
        df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    )


def add_trip_duration_minutes(df):
    """Derive trip duration in minutes from pickup/dropoff timestamps."""
    from pyspark.sql import functions as F

    return df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
