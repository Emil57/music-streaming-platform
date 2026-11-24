import json

import pytest

from music_streaming.ingestion import (
    CORRUPT_RECORD_COLUMN,
    REJECT_REASONS_COLUMN,
    read_dataset,
    split_valid_rejected,
)
from music_streaming.schemas import LISTENING_EVENTS, USERS

VALID_EVENT = {
    "event_id": "e1",
    "user_id": "u1",
    "track_id": "t1",
    "event_ts": "2026-11-01T20:15:00",
    "ms_played": 185000,
    "country": "MX",
    "device_type": "mobile",
}


def write_json_lines(path, lines):
    """Write raw lines (dicts are serialized, strings are written as-is to simulate bad input)."""
    path.write_text(
        "\n".join(json.dumps(line) if isinstance(line, dict) else line for line in lines),
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture
def events_file(tmp_path):
    missing_user = {**VALID_EVENT, "event_id": "e2"}
    del missing_user["user_id"]
    wrong_type = {**VALID_EVENT, "event_id": "e3", "ms_played": "a lot"}
    return write_json_lines(
        tmp_path / "events.json",
        [VALID_EVENT, missing_user, wrong_type, "{this is not json"],
    )


def test_read_uses_explicit_schema(spark, events_file):
    df = read_dataset(spark, LISTENING_EVENTS, events_file)

    assert df.schema.fieldNames() == [*LISTENING_EVENTS.schema.fieldNames(), CORRUPT_RECORD_COLUMN]
    assert dict(df.dtypes)["ms_played"] == "bigint"
    assert dict(df.dtypes)["event_ts"] == "timestamp"


def test_file_reader_does_not_enforce_nullability(spark, events_file):
    # The schema says user_id is required, yet Spark happily reads the row with a null.
    df = read_dataset(spark, LISTENING_EVENTS, events_file)

    assert all(field.nullable for field in df.schema.fields)
    assert df.where("event_id = 'e2' AND user_id IS NULL").count() == 1


def test_split_valid_rejected(spark, events_file):
    df = read_dataset(spark, LISTENING_EVENTS, events_file)

    valid, rejected = split_valid_rejected(df, LISTENING_EVENTS)

    assert [r.event_id for r in valid.collect()] == ["e1"]
    assert valid.schema.fieldNames() == LISTENING_EVENTS.schema.fieldNames()

    reasons = {r.event_id: r[REJECT_REASONS_COLUMN] for r in rejected.collect()}
    assert reasons["e2"] == ["missing user_id"]
    assert "malformed record" in reasons["e3"]  # "a lot" is not a bigint
    # Unparseable JSON: every field is null, only the raw text survives.
    assert "malformed record" in reasons[None]
    assert len(reasons) == 3


def test_failfast_raises_on_malformed_input(spark, events_file):
    df = read_dataset(spark, LISTENING_EVENTS, events_file, mode="FAILFAST")

    with pytest.raises(Exception, match="(?i)malformed"):
        df.collect()


def test_dropmalformed_silently_drops_rows(spark, events_file):
    df = read_dataset(spark, LISTENING_EVENTS, events_file, mode="DROPMALFORMED")

    # Only the two parseable rows survive, and the missing user_id still slips through.
    assert sorted(r.event_id for r in df.collect()) == ["e1", "e2"]


def test_read_csv(spark, tmp_path):
    path = tmp_path / "users.csv"
    path.write_text(
        "user_id,country,signup_date,birth_year\n"
        "u1,MX,2025-03-01,1994\n"
        "u2,US,2025-04-02,\n"
        "u3,US,not-a-date,1990\n",
        encoding="utf-8",
    )

    valid, rejected = split_valid_rejected(read_dataset(spark, USERS, str(path), "csv"), USERS)

    assert [(r.user_id, r.birth_year) for r in valid.orderBy("user_id").collect()] == [
        ("u1", 1994),
        ("u2", None),
    ]
    assert [r.user_id for r in rejected.collect()] == ["u3"]


def test_empty_input(spark, tmp_path):
    path = write_json_lines(tmp_path / "empty.json", [])

    valid, rejected = split_valid_rejected(
        read_dataset(spark, LISTENING_EVENTS, path), LISTENING_EVENTS
    )

    assert valid.count() == 0
    assert rejected.count() == 0
