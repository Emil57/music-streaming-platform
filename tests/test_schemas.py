from datetime import date, datetime
from decimal import Decimal

import pytest

from music_streaming.schemas import (
    DATASETS,
    LISTENING_EVENTS,
    SUBSCRIPTION_EVENTS,
    TRACKS,
    USERS,
)

# --- Contract: names and types (no JVM needed) -----------------------------------------------

EXPECTED_SCHEMAS = {
    "users": "struct<user_id:string,country:string,signup_date:date,birth_year:int>",
    "artists": "struct<artist_id:string,name:string,country:string>",
    "tracks": (
        "struct<track_id:string,title:string,artist_id:string,genre:string,"
        "duration_ms:int,release_date:date,explicit:boolean>"
    ),
    "listening_events": (
        "struct<event_id:string,user_id:string,track_id:string,event_ts:timestamp,"
        "ms_played:bigint,country:string,device_type:string>"
    ),
    "subscription_events": (
        "struct<event_id:string,user_id:string,event_ts:timestamp,event_type:string,"
        "plan:string,price:decimal(10,2),currency:string>"
    ),
}


@pytest.mark.parametrize("name", EXPECTED_SCHEMAS)
def test_field_names_and_types(name):
    assert DATASETS[name].schema.simpleString() == EXPECTED_SCHEMAS[name]


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=DATASETS.keys())
def test_primary_key_columns_are_required(dataset):
    for column in dataset.primary_key:
        assert column in dataset.required_columns


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=DATASETS.keys())
def test_foreign_keys_reference_existing_columns(dataset):
    for column, target in dataset.foreign_keys.items():
        target_dataset, target_column = target.split(".")
        assert column in dataset.schema.fieldNames()
        assert target_column in DATASETS[target_dataset].primary_key


def test_optional_fields_are_nullable():
    assert USERS.schema["birth_year"].nullable
    assert TRACKS.schema["genre"].nullable
    assert SUBSCRIPTION_EVENTS.schema["price"].nullable


# --- Representative records (needs Spark) ----------------------------------------------------

REPRESENTATIVE_RECORDS = {
    "users": [("u1", "MX", date(2025, 3, 1), 1994), ("u2", "US", date(2025, 4, 2), None)],
    "artists": [("a1", "Natalia Lafourcade", "MX")],
    "tracks": [("t1", "Hasta la Raíz", "a1", "pop", 210_000, date(2015, 3, 3), False)],
    "listening_events": [
        ("e1", "u1", "t1", datetime(2026, 11, 1, 20, 15), 185_000, "MX", "mobile"),
    ],
    "subscription_events": [
        ("s1", "u1", datetime(2026, 1, 1, 9, 0), "started", "premium", Decimal("129.00"), "MXN"),
        ("s2", "u2", datetime(2026, 1, 2, 9, 0), "started", "free", None, None),
    ],
}


@pytest.mark.parametrize("name", REPRESENTATIVE_RECORDS)
def test_representative_records_match_schema(spark, name):
    schema = DATASETS[name].schema

    df = spark.createDataFrame(REPRESENTATIVE_RECORDS[name], schema)

    assert df.schema == schema
    assert df.count() == len(REPRESENTATIVE_RECORDS[name])


def test_create_dataframe_enforces_required_fields(spark):
    # Unlike file readers, createDataFrame verifies nullability on the Python side.
    record = (None, "u1", "t1", datetime(2026, 11, 1), 1000, "MX", "mobile")

    with pytest.raises(Exception, match="event_id"):
        spark.createDataFrame([record], LISTENING_EVENTS.schema)
