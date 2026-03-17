"""Bronze ingestion helpers for NYC Yellow Taxi data."""

from __future__ import annotations

from datetime import datetime, timezone


def add_ingestion_metadata(df):
    """Add standard audit columns to a DataFrame."""
    from pyspark.sql import functions as F

    ingested_at = datetime.now(timezone.utc).isoformat()
    return (
        df.withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_file", F.input_file_name())
    )
