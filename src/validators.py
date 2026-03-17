"""Data quality validators for the NYC Yellow Taxi pipeline."""

from __future__ import annotations


def check_not_empty(df, label: str = "DataFrame") -> None:
    """Raise if the DataFrame has no rows."""
    count = df.count()
    if count == 0:
        raise ValueError(f"{label} is empty — expected at least one row.")


def check_no_nulls(df, column: str) -> None:
    """Raise if the named column contains any nulls."""
    from pyspark.sql import functions as F

    null_count = df.filter(F.col(column).isNull()).count()
    if null_count > 0:
        raise ValueError(f"Column '{column}' contains {null_count} null value(s).")


def check_accepted_values(df, column: str, accepted: list) -> None:
    """Raise if the column contains values outside the accepted set."""
    from pyspark.sql import functions as F

    bad = df.filter(~F.col(column).isin(accepted))
    bad_count = bad.count()
    if bad_count > 0:
        raise ValueError(
            f"Column '{column}' has {bad_count} row(s) with unexpected values. "
            f"Accepted: {accepted}"
        )
