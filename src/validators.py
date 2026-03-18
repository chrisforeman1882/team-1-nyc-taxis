"""Data quality validators for the NYC Yellow Taxi pipeline.

Each validator takes a DataFrame and either:
  - Returns quietly on success, or
  - Raises an AssertionError with a descriptive message.

Designed to run against the Silver table after I-03/I-04 cleaning.
"""

from pyspark.sql import DataFrame, functions as F


def check_not_empty(df: DataFrame, table_name: str) -> None:
    """Assert the table has at least one row."""
    count = df.count()
    assert count > 0, f"{table_name} table is empty (0 rows)"
    print(f"  PASS  {table_name} is not empty ({count:,} rows)")


def check_no_nulls(df: DataFrame, column: str) -> None:
    """Assert a column contains zero NULL values."""
    null_count = df.filter(F.col(column).isNull()).count()
    assert null_count == 0, (
        f"Column '{column}' has {null_count:,} NULL values (expected 0)"
    )
    print(f"  PASS  '{column}' has no NULLs")


def check_positive(df: DataFrame, column: str) -> None:
    """Assert all values in a numeric column are strictly positive (> 0)."""
    bad_count = df.filter(F.col(column) <= 0).count()
    assert bad_count == 0, (
        f"Column '{column}' has {bad_count:,} non-positive values (expected all > 0)"
    )
    print(f"  PASS  '{column}' all values > 0")


def check_non_negative(df: DataFrame, column: str) -> None:
    """Assert all values in a numeric column are >= 0."""
    bad_count = df.filter(F.col(column) < 0).count()
    assert bad_count == 0, (
        f"Column '{column}' has {bad_count:,} negative values (expected all >= 0)"
    )
    print(f"  PASS  '{column}' all values >= 0")


def check_max(df: DataFrame, column: str, max_value: float) -> None:
    """Assert all values in a column are <= max_value."""
    bad_count = df.filter(F.col(column) > max_value).count()
    assert bad_count == 0, f"Column '{column}' has {bad_count:,} values > {max_value}"
    print(f"  PASS  '{column}' all values <= {max_value}")


def check_accepted_values(df: DataFrame, column: str, accepted: list) -> None:
    """Assert all non-null values in a column are from an accepted set."""
    bad_count = df.filter(
        F.col(column).isNotNull() & ~F.col(column).isin(accepted)
    ).count()
    assert bad_count == 0, (
        f"Column '{column}' has {bad_count:,} values outside {accepted}"
    )
    print(f"  PASS  '{column}' all non-null values in {accepted}")


def check_no_duplicates(df: DataFrame, columns: list[str]) -> None:
    """Assert there are no duplicate rows across the given columns."""
    total = df.count()
    distinct = df.select(columns).distinct().count()
    dupe_count = total - distinct
    assert dupe_count == 0, f"Found {dupe_count:,} duplicate rows on columns {columns}"
    print(f"  PASS  No duplicates on {columns}")


def check_column_exists(df: DataFrame, column: str) -> None:
    """Assert a column exists in the DataFrame schema."""
    assert column in df.columns, f"Column '{column}' not found. Available: {df.columns}"
    print(f"  PASS  Column '{column}' exists")
