"""Shared test fixtures.

Provides two tiers of fixtures:

1. **Unit tests** (local / CI / Databricks) — ``local_spark`` returns the
   active Databricks session when available, or creates a lightweight
   local SparkSession for CI environments.
2. **INT-01 integration tests** (Databricks only) — ``spark``,
   ``bronze_df``, ``silver_df``, ``gold_fact_df`` read persisted Delta
   tables from Unity Catalog via the notebook kernel's active session.

Usage (Databricks notebook)::

    import sys, pytest
    sys.dont_write_bytecode = True
    pytest.main(["tests/", "-v", "--tb=short", "-p", "no:cacheprovider"])
"""

import os
import sys

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.constants import BRONZE_TABLE, GOLD_FACT_TABLE, SILVER_TABLE


def _get_or_create_spark() -> SparkSession:
    """Return the active SparkSession, or create a local one for CI.

    On Databricks (Spark Connect), ``getActiveSession()`` returns the
    notebook kernel's session.  In CI / local dev there is no active
    session, so we fall back to a lightweight ``local[1]`` session.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    # Ensure PySpark workers use the same Python interpreter as the driver.
    # Without this, workers default to the system Python, causing a version
    # mismatch error when the venv Python differs from the system Python.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Unit-test fixtures ───────────────────────────────────────────────────


@pytest.fixture(scope="session")
def local_spark() -> SparkSession:
    """SparkSession for unit tests (works on Databricks and CI)."""
    return _get_or_create_spark()


# ── INT-01 integration-test fixtures (Databricks only) ──────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Return the active Databricks SparkSession (INT-01 only)."""
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession. Run INT-01 tests from a Databricks "
            "notebook cell using pytest.main()."
        )
    return session


@pytest.fixture(scope="session")
def bronze_df(spark: SparkSession) -> DataFrame:
    """Read the persisted Bronze Delta table."""
    return spark.read.table(BRONZE_TABLE)


@pytest.fixture(scope="session")
def silver_df(spark: SparkSession) -> DataFrame:
    """Read the persisted Silver Delta table."""
    return spark.read.table(SILVER_TABLE)


@pytest.fixture(scope="session")
def gold_fact_df(spark: SparkSession) -> DataFrame:
    """Read the persisted Gold fact Delta table."""
    return spark.read.table(GOLD_FACT_TABLE)
