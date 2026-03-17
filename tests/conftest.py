"""Shared fixtures for the NYC Taxi test suite."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-taxis-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
