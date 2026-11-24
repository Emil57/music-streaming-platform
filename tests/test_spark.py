import pytest
from pyspark.sql import SparkSession

from music_streaming.spark import (
    DEFAULT_APP_NAME,
    DEFAULT_SHUFFLE_PARTITIONS,
    SparkConfig,
    create_spark_session,
    spark_session,
)

# --- SparkConfig: pure Python, no JVM needed --------------------------------------------------


def test_config_defaults():
    config = SparkConfig()

    assert config.app_name == DEFAULT_APP_NAME
    assert config.master is None
    assert config.shuffle_partitions == DEFAULT_SHUFFLE_PARTITIONS


def test_config_from_env_reads_variables():
    env = {
        "MSP_SPARK_APP_NAME": "nightly-job",
        "MSP_SPARK_MASTER": "local[2]",
        "MSP_SPARK_SHUFFLE_PARTITIONS": "16",
    }

    config = SparkConfig.from_env(env)

    assert config == SparkConfig(app_name="nightly-job", master="local[2]", shuffle_partitions=16)


def test_config_from_env_uses_defaults_when_unset():
    assert SparkConfig.from_env({}) == SparkConfig()


def test_config_overrides_win_over_env():
    config = SparkConfig.from_env({"MSP_SPARK_APP_NAME": "from-env"}, app_name="explicit")

    assert config.app_name == "explicit"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"app_name": "  "}, "app_name"),
        ({"shuffle_partitions": 0}, "shuffle_partitions"),
    ],
)
def test_config_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        SparkConfig(**kwargs)


def test_config_rejects_non_integer_partitions_env():
    with pytest.raises(ValueError, match="MSP_SPARK_SHUFFLE_PARTITIONS"):
        SparkConfig.from_env({"MSP_SPARK_SHUFFLE_PARTITIONS": "many"})


# --- SparkSession: starts a local JVM ---------------------------------------------------------

TEST_CONFIG = SparkConfig(
    app_name="msp-test",
    master="local[2]",
    shuffle_partitions=4,
    extra_conf={"spark.ui.enabled": "false"},
)


@pytest.mark.spark
def test_session_applies_config():
    with spark_session(TEST_CONFIG) as spark:
        assert spark.sparkContext.appName == "msp-test"
        assert spark.sparkContext.master == "local[2]"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
        assert spark.conf.get("spark.ui.enabled") == "false"


@pytest.mark.spark
def test_session_runs_local_job():
    with spark_session(TEST_CONFIG) as spark:
        plays = spark.createDataFrame(
            [("track-1", "MX"), ("track-2", "MX"), ("track-1", "US")], ["track_id", "country"]
        )

        # groupBy forces a shuffle, so this exercises the configured shuffle partitions.
        result = {
            row["country"]: row["count"] for row in plays.groupBy("country").count().collect()
        }

    assert result == {"MX": 2, "US": 1}


@pytest.mark.spark
def test_context_manager_stops_session():
    with spark_session(TEST_CONFIG) as spark:
        assert SparkSession.getActiveSession() is spark

    assert SparkSession.getActiveSession() is None
    assert spark.sparkContext._jsc is None  # JVM context released


@pytest.mark.spark
def test_context_manager_stops_session_on_error():
    with pytest.raises(RuntimeError, match="job failed"), spark_session(TEST_CONFIG):
        raise RuntimeError("job failed")

    assert SparkSession.getActiveSession() is None


@pytest.mark.spark
def test_create_spark_session_reuses_running_session():
    first = create_spark_session(TEST_CONFIG)
    try:
        second = create_spark_session(TEST_CONFIG)
        assert second is first
    finally:
        first.stop()
