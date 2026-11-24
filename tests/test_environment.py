"""Smoke tests verifying the local development environment."""

import pyspark
from pyspark.sql import SparkSession

import music_streaming_platform


def test_package_imports() -> None:
    assert music_streaming_platform.__version__ == "0.1.0"


def test_pyspark_version() -> None:
    assert int(pyspark.__version__.split(".")[0]) >= 4


def test_spark_starts_locally() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("environment-smoke-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        assert spark.range(10).count() == 10
    finally:
        spark.stop()
