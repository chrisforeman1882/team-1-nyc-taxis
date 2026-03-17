"""Smoke tests — verifies the test harness works."""


def test_true():
    assert True


def test_spark_session(spark):
    df = spark.createDataFrame([{"id": 1}])
    assert df.count() == 1
