# Music Streaming Platform

A music streaming analytics platform built with PySpark, developed sprint by sprint as a
data-engineering learning project.

## Requirements

| Tool | Version | Why |
|------|---------|-----|
| [uv](https://docs.astral.sh/uv/) | 0.13+ | Python, dependencies and virtualenv management |
| Python | 3.13 (installed by uv) | Pinned in `.python-version` |
| Java (JDK) | **17 or 21** | Spark runs on the JVM; PySpark 4.x does not support Java 8/11 |

### Java setup

PySpark is a Python API that drives a Spark engine running in a Java process, so a
compatible JDK must be installed and discoverable.

Windows (winget):

```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```

macOS / Linux: install Temurin 17 from <https://adoptium.net> or via your package manager.

Then make sure `JAVA_HOME` points to the JDK and `java -version` reports 17+:

```powershell
# Windows (PowerShell) - persists for new terminals
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17<version>-hotspot"
java -version
```

```bash
# macOS / Linux
export JAVA_HOME=/path/to/jdk-17
java -version
```

### Windows only: Hadoop native binaries

Spark uses Hadoop's filesystem layer, which on Windows needs `winutils.exe` and `hadoop.dll`
to list folders and write files. PySpark does not ship them. Download both from
[cdarlint/winutils](https://github.com/cdarlint/winutils) (`hadoop-3.3.6/bin`, a community
build) into `C:\hadoop\bin`, then:

```powershell
setx HADOOP_HOME "C:\hadoop"
# and add C:\hadoop\bin to your user PATH, then restart the terminal/IDE
```

Without them you get `UnsatisfiedLinkError: ... NativeIO$Windows.access0` when reading a folder.
Linux and macOS do not need this.

## Local setup

```bash
git clone https://github.com/Emil57/music-streaming-platform.git
cd music-streaming-platform
uv sync          # creates .venv and installs locked dependencies
```

## Development commands

```bash
uv run pytest                 # all tests
uv run pytest -m "not spark"  # skip tests that start Spark
uv run ruff check .           # lint
uv run ruff format .          # format
```

## Testing

```bash
uv run pytest tests/test_spark.py               # one file
uv run pytest -k referential                    # tests whose name matches
uv run pytest -x                                # stop at the first failure
uv run ruff check . && uv run ruff format --check . && uv run pytest  # same checks as CI
```

Tests that need Spark use the `spark` fixture from `tests/conftest.py`: one local session per test
module (`local[1]`, 1 shuffle partition, UI off), stopped when the module finishes. Such tests are
marked `spark` automatically, so `-m "not spark"` gives a fast run with no JVM.

## Continuous integration

[GitHub Actions](.github/workflows/ci.yml) runs on every pull request and every push to `main`:

- **lint**: `ruff check` and `ruff format --check`
- **test**: `pytest` on Java 17 and Java 21 (Temurin), the supported range

Dependencies are installed with `uv sync --locked`, so CI fails if `uv.lock` is out of date with
`pyproject.toml`. Any failing check marks the PR red.

## Spark sessions

Always create sessions through `music_streaming.spark` so configuration lives in one place:

```python
from music_streaming.spark import SparkConfig, spark_session

with spark_session(SparkConfig(app_name="daily-streams")) as spark:
    spark.range(10).count()
# the session is stopped here, even if the job raised an error
```

`SparkConfig.from_env()` (the default) reads these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MSP_SPARK_APP_NAME` | `music-streaming` | Name shown in the Spark UI and logs |
| `MSP_SPARK_MASTER` | unset | Cluster URL, e.g. `local[4]`. Leave unset in production so `spark-submit --master` decides; locally PySpark then uses `local[*]` |
| `MSP_SPARK_SHUFFLE_PARTITIONS` | `8` | `spark.sql.shuffle.partitions` (Spark's default of 200 is sized for clusters) |

## Data model

Source datasets, their grain, keys and ingestion rules are documented in
[docs/data-model.md](docs/data-model.md).

## Synthetic data

Generate reproducible raw data (written to `data/raw/<dataset>/part-NNNNN.json`, git-ignored):

```bash
uv run msp-generate                                  # defaults: 10k users, 1M listening events
uv run msp-generate --events 5000000 --seed 7        # bigger, different seed
uv run msp-generate --format csv --output data/raw_csv
uv run msp-generate --help                           # all options
```

The defaults take about 30 seconds and ~190 MB. Same seed means byte-identical output; each
dataset uses its own random stream, so changing `--events` does not change users or tracks.
Track popularity and user activity follow a long-tail distribution, like real streaming data.

## Troubleshooting

- **Spark hangs or fails with `UnsupportedClassVersionError`**: Spark is using an old Java.
  Check that `JAVA_HOME` points to JDK 17+ and open a new terminal after changing it.
- **`Python worker failed to connect back` (Windows)**: Spark launched a different
  Python than the project's. Set `PYSPARK_PYTHON` to the `.venv` interpreter (the test
  suite does this automatically in `tests/conftest.py`).

## Project layout

```
src/music_streaming/   # application package (src layout)
tests/                 # pytest suite
.github/workflows/     # CI pipeline
pyproject.toml         # dependencies, Ruff and pytest configuration
uv.lock                # exact locked dependency versions
```
