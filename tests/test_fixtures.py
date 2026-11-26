"""Tests for the test infrastructure itself: the `spark` fixture and failure reporting."""

from pathlib import Path

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession

CONFTEST = Path(__file__).with_name("conftest.py")


@pytest.mark.spark
def test_spark_fixture_is_released_even_when_a_test_fails(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(CONFTEST.read_text())
    pytester.makeini("[pytest]\nmarkers =\n    spark: starts Spark\n")
    pytester.makepyfile(
        """
        def test_passes(spark):
            assert spark.range(3).count() == 3

        def test_fails(spark):
            assert spark.range(3).count() == 4
        """
    )

    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")

    # A failing test makes pytest exit non-zero, which is what turns a CI job red.
    result.assert_outcomes(passed=1, failed=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    # The module-scoped fixture stopped the session and its JVM-side context.
    assert SparkSession.getActiveSession() is None
    assert SparkContext._active_spark_context is None
