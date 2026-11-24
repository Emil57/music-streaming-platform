# Music Streaming Platform

A music streaming analytics platform built with PySpark.

## Requirements

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** for dependency and environment management
- **Java 17 or 21** — PySpark runs on the JVM. Check your version with `java -version`
  and make sure `JAVA_HOME` points to a supported JDK.

## Local setup

```bash
uv sync            # create .venv and install runtime + dev dependencies
uv run pytest      # run the test suite (also verifies Spark starts locally)
uv run ruff check . && uv run ruff format --check .
```

## Project layout

```
src/music_streaming_platform/   # application package
tests/                          # pytest test suite
```
