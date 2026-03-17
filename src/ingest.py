"""Bronze ingestion helpers for NYC Yellow Taxi data."""

from pyspark.sql import DataFrame, functions as F


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns for lineage tracking.

    Adds:
        _ingested_at (timestamp): UTC timestamp of when the row was ingested.

    Parameters
    ----------
    df : DataFrame
        Raw source DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with ingestion metadata column appended.
    """
    return df.withColumn("_ingested_at", F.current_timestamp())


def write_bronze(df: DataFrame, target_table: str) -> None:
    """Write a DataFrame to a Bronze Delta table in append-only mode.

    Parameters
    ----------
    df : DataFrame
        DataFrame to write (should already include ingestion metadata).
    target_table : str
        Fully qualified Delta table name (catalog.schema.table).
    """
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
