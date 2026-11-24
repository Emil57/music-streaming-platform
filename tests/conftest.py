import functools
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

MIN_JAVA_VERSION = 17

# Spark's JVM launches separate Python worker processes. Point them at the
# interpreter running pytest (the project .venv) instead of whatever `python`
# happens to be first on PATH - otherwise workers fail to connect back on Windows.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@functools.cache
def java_major_version() -> int | None:
    """Return the major version of the Java that Spark will use (JAVA_HOME first, then PATH)."""
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home, "bin", "java")) if java_home else shutil.which("java")
    if not java:
        return None
    result = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30)
    # Java 8 reports "1.8.0_202", Java 9+ reports "17.0.21" etc.
    match = re.search(r'version "(\d+)(?:\.(\d+))?', result.stderr + result.stdout)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2)) if major == 1 else major


def pytest_runtest_setup(item: pytest.Item) -> None:
    # An incompatible Java makes Spark hang instead of erroring, so fail fast with a clear message.
    if item.get_closest_marker("spark") is None:
        return
    version = java_major_version()
    if version is None or version < MIN_JAVA_VERSION:
        pytest.fail(
            f"Spark needs Java {MIN_JAVA_VERSION}+, found: {version or 'none'}. "
            "Set JAVA_HOME to a JDK 17+ and open a new terminal (see README).",
            pytrace=False,
        )
