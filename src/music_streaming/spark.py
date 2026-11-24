"""Centralized SparkSession creation.

All jobs and tests should get their SparkSession from here, so configuration lives in one place.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "music-streaming"
# Spark's default is 200, which is sized for clusters. On a laptop with small data it creates
# hundreds of tiny tasks, so we default lower and let each environment override it.
DEFAULT_SHUFFLE_PARTITIONS = 8

ENV_APP_NAME = "MSP_SPARK_APP_NAME"
ENV_MASTER = "MSP_SPARK_MASTER"
ENV_SHUFFLE_PARTITIONS = "MSP_SPARK_SHUFFLE_PARTITIONS"


@dataclass(frozen=True)
class SparkConfig:
    """Settings used to build a SparkSession.

    ``master`` defaults to ``None`` on purpose: in production the cluster manager is chosen by
    ``spark-submit --master`` (or the platform, e.g. EMR), and setting it in code would override
    that. When ``None`` and run as a plain Python process, PySpark falls back to ``local[*]``.
    """

    app_name: str = DEFAULT_APP_NAME
    master: str | None = None
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS
    extra_conf: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.app_name.strip():
            raise ValueError("app_name must not be empty")
        if self.shuffle_partitions < 1:
            raise ValueError(f"shuffle_partitions must be >= 1, got {self.shuffle_partitions}")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **overrides) -> SparkConfig:
        """Build a config from ``MSP_SPARK_*`` environment variables.

        Explicit ``overrides`` win over environment variables, which win over defaults.
        """
        env = os.environ if env is None else env
        values: dict = {}
        if ENV_APP_NAME in env:
            values["app_name"] = env[ENV_APP_NAME]
        if ENV_MASTER in env:
            values["master"] = env[ENV_MASTER]
        if ENV_SHUFFLE_PARTITIONS in env:
            raw = env[ENV_SHUFFLE_PARTITIONS]
            try:
                values["shuffle_partitions"] = int(raw)
            except ValueError:
                raise ValueError(
                    f"{ENV_SHUFFLE_PARTITIONS} must be an integer, got {raw!r}"
                ) from None
        values.update(overrides)
        return cls(**values)


def create_spark_session(config: SparkConfig | None = None) -> SparkSession:
    """Create (or reuse) a SparkSession from ``config`` (defaults to ``SparkConfig.from_env()``).

    Note: ``getOrCreate`` returns the already-running session if there is one. Runtime SQL
    settings such as shuffle partitions are still applied to it, but JVM-level settings such as
    master and app name only take effect when a new session is started.
    """
    config = config or SparkConfig.from_env()

    # Spark launches separate Python worker processes; make them use this interpreter rather
    # than whatever `python` is first on PATH (otherwise workers can fail to connect back).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(config.app_name).config(
        "spark.sql.shuffle.partitions", str(config.shuffle_partitions)
    )
    if config.master:
        builder = builder.master(config.master)
    for key, value in config.extra_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


@contextmanager
def spark_session(config: SparkConfig | None = None) -> Iterator[SparkSession]:
    """Context manager that guarantees the session is stopped, even if the job fails.

    >>> with spark_session(SparkConfig(app_name="my-job")) as spark:
    ...     spark.range(10).count()
    """
    spark = create_spark_session(config)
    try:
        yield spark
    finally:
        spark.stop()
