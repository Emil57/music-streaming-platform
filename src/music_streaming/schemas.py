"""Explicit schemas for the raw source datasets.

Every dataset documents its grain (what one row represents), primary key and foreign keys.
See docs/data-model.md for the human-readable version.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class Dataset:
    """A source dataset: its schema plus the metadata needed to reason about it."""

    name: str
    schema: StructType
    grain: str
    primary_key: tuple[str, ...]
    # column -> "dataset.column" it references
    foreign_keys: Mapping[str, str] = field(default_factory=dict)

    @property
    def required_columns(self) -> list[str]:
        return [f.name for f in self.schema.fields if not f.nullable]


USERS = Dataset(
    name="users",
    grain="One row per registered user.",
    primary_key=("user_id",),
    schema=StructType(
        [
            StructField("user_id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),  # ISO 3166-1 alpha-2, e.g. MX
            StructField("signup_date", DateType(), nullable=False),
            StructField("birth_year", IntegerType(), nullable=True),  # optional profile field
        ]
    ),
)

ARTISTS = Dataset(
    name="artists",
    grain="One row per artist.",
    primary_key=("artist_id",),
    schema=StructType(
        [
            StructField("artist_id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("country", StringType(), nullable=True),
        ]
    ),
)

TRACKS = Dataset(
    name="tracks",
    grain="One row per track in the catalog.",
    primary_key=("track_id",),
    foreign_keys={"artist_id": "artists.artist_id"},
    schema=StructType(
        [
            StructField("track_id", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("artist_id", StringType(), nullable=False),
            StructField("genre", StringType(), nullable=True),
            StructField("duration_ms", IntegerType(), nullable=False),
            StructField("release_date", DateType(), nullable=True),
            StructField("explicit", BooleanType(), nullable=False),
        ]
    ),
)

LISTENING_EVENTS = Dataset(
    name="listening_events",
    grain="One row per playback of a track by a user (a stream).",
    primary_key=("event_id",),
    foreign_keys={"user_id": "users.user_id", "track_id": "tracks.track_id"},
    schema=StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("user_id", StringType(), nullable=False),
            StructField("track_id", StringType(), nullable=False),
            StructField("event_ts", TimestampType(), nullable=False),
            # LongType: summing milliseconds over millions of plays overflows a 32-bit int.
            StructField("ms_played", LongType(), nullable=False),
            StructField("country", StringType(), nullable=True),  # where the play happened
            StructField("device_type", StringType(), nullable=True),  # mobile, desktop, ...
        ]
    ),
)

SUBSCRIPTION_EVENTS = Dataset(
    name="subscription_events",
    grain="One row per change in a user's subscription (start, renewal, plan change, cancel).",
    primary_key=("event_id",),
    foreign_keys={"user_id": "users.user_id"},
    schema=StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("user_id", StringType(), nullable=False),
            StructField("event_ts", TimestampType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),  # started, renewed, ...
            StructField("plan", StringType(), nullable=False),  # free, premium, family, student
            # Decimal, never float, for money: 0.1 + 0.2 != 0.3 in floating point.
            StructField("price", DecimalType(10, 2), nullable=True),
            StructField("currency", StringType(), nullable=True),  # ISO 4217, e.g. MXN
        ]
    ),
)

DATASETS: dict[str, Dataset] = {
    d.name: d for d in (USERS, ARTISTS, TRACKS, LISTENING_EVENTS, SUBSCRIPTION_EVENTS)
}
