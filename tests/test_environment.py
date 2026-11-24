"""Smoke tests: the package installs and Spark can start locally."""

import pytest


def test_package_imports():
    import music_streaming

    assert music_streaming.__version__ == "0.1.0"


def test_pyspark_imports():
    import pyspark

    assert pyspark.__version__.startswith("4.")


@pytest.mark.spark
def test_spark_starts_locally():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("msp-smoke-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame([(1, "song-a"), (2, "song-b")], ["id", "track"])
        assert df.count() == 2
    finally:
        spark.stop()
