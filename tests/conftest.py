import os
import sys

# Spark's JVM launches separate Python worker processes. Point them at the
# interpreter running pytest (the project .venv) instead of whatever `python`
# happens to be first on PATH - otherwise workers fail to connect back on Windows.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
