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
pyproject.toml         # dependencies, Ruff and pytest configuration
uv.lock                # exact locked dependency versions
```
