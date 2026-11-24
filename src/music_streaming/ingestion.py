"""Read raw source files with explicit schemas and quarantine bad records."""

from __future__ import annotations

from typing import Literal

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from music_streaming.schemas import Dataset

CORRUPT_RECORD_COLUMN = "_corrupt_record"
REJECT_REASONS_COLUMN = "_reject_reasons"

FileFormat = Literal["json", "csv"]
# PERMISSIVE: keep malformed rows (fields null, raw text in the corrupt record column)
# DROPMALFORMED: silently drop them - you lose visibility, so avoid in ingestion
# FAILFAST: raise on the first malformed row
ReadMode = Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]


def read_dataset(
    spark: SparkSession,
    dataset: Dataset,
    path: str,
    fmt: FileFormat = "json",
    mode: ReadMode = "PERMISSIVE",
) -> DataFrame:
    """Read ``path`` using the dataset's explicit schema (never schema inference).

    In PERMISSIVE mode an extra ``_corrupt_record`` column holds the raw text of rows that
    could not be parsed.

    Note: file sources ignore ``nullable=False`` - every column is read as nullable. Use
    :func:`split_valid_rejected` to enforce required columns.
    """
    schema = dataset.schema
    reader = spark.read.format(fmt).option("mode", mode)
    if mode == "PERMISSIVE":
        schema = StructType([*schema.fields, StructField(CORRUPT_RECORD_COLUMN, StringType())])
        reader = reader.option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
    if fmt == "csv":
        reader = reader.option("header", "true")
    return reader.schema(schema).load(path)


def split_valid_rejected(df: DataFrame, dataset: Dataset) -> tuple[DataFrame, DataFrame]:
    """Split rows into ``(valid, rejected)``.

    A row is rejected if it was malformed or a required (non-nullable) column is null.
    ``valid`` has exactly the dataset's columns; ``rejected`` keeps every column plus
    ``_reject_reasons`` (array of strings) for debugging.
    """
    checks: list[Column] = [
        F.when(F.col(c).isNull(), F.lit(f"missing {c}")) for c in dataset.required_columns
    ]
    if CORRUPT_RECORD_COLUMN in df.columns:
        checks.insert(
            0, F.when(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), F.lit("malformed record"))
        )

    reasons = F.filter(F.array(*checks), lambda reason: reason.isNotNull())
    checked = df.withColumn(REJECT_REASONS_COLUMN, reasons)

    valid = checked.where(F.size(REJECT_REASONS_COLUMN) == 0).select(*dataset.schema.fieldNames())
    rejected = checked.where(F.size(REJECT_REASONS_COLUMN) > 0)
    return valid, rejected
