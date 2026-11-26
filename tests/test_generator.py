from collections import Counter
from itertools import islice

import pytest

from music_streaming.generator import (
    GeneratorConfig,
    generate,
    generate_artists,
    generate_listening_events,
    generate_subscription_events,
    generate_tracks,
    generate_users,
)
from music_streaming.ingestion import read_dataset, split_valid_rejected
from music_streaming.schemas import DATASETS

SMALL = GeneratorConfig(
    num_users=200, num_artists=20, num_tracks=300, num_listening_events=5_000, seed=7
)


def generate_in_memory(config: GeneratorConfig) -> dict[str, list[dict]]:
    users = generate_users(config)
    artists = generate_artists(config)
    tracks = generate_tracks(config, artists)
    return {
        "users": users,
        "artists": artists,
        "tracks": tracks,
        "listening_events": list(generate_listening_events(config, users, tracks)),
        "subscription_events": list(generate_subscription_events(config, users)),
    }


@pytest.fixture(scope="module")
def data():
    return generate_in_memory(SMALL)


# --- Determinism -----------------------------------------------------------------------------


def test_same_seed_same_data(data):
    assert generate_in_memory(SMALL) == data


def test_different_seed_different_data(data):
    other = generate_in_memory(GeneratorConfig(**{**SMALL.__dict__, "seed": 8}))

    assert other["listening_events"] != data["listening_events"]


def test_datasets_use_independent_random_streams(data):
    # More events must not change who the users are.
    bigger = GeneratorConfig(**{**SMALL.__dict__, "num_listening_events": 9_999})

    assert generate_users(bigger) == data["users"]


# --- Counts ----------------------------------------------------------------------------------


def test_expected_record_counts(data):
    assert len(data["users"]) == SMALL.num_users
    assert len(data["artists"]) == SMALL.num_artists
    assert len(data["tracks"]) == SMALL.num_tracks
    assert len(data["listening_events"]) == SMALL.num_listening_events
    # Every user gets a `started` event; paid users get more.
    assert len(data["subscription_events"]) >= SMALL.num_users


@pytest.mark.parametrize("name", DATASETS)
def test_primary_keys_are_unique(data, name):
    key = DATASETS[name].primary_key
    values = [tuple(row[c] for c in key) for row in data[name]]

    assert len(values) == len(set(values))


def test_rejects_invalid_config():
    with pytest.raises(ValueError, match="num_users"):
        GeneratorConfig(num_users=0)


# --- Referential integrity and domain rules --------------------------------------------------


def ids(rows, column):
    return {row[column] for row in rows}


def test_referential_integrity(data):
    user_ids = ids(data["users"], "user_id")
    track_ids = ids(data["tracks"], "track_id")

    assert ids(data["tracks"], "artist_id") <= ids(data["artists"], "artist_id")
    assert ids(data["listening_events"], "user_id") <= user_ids
    assert ids(data["listening_events"], "track_id") <= track_ids
    assert ids(data["subscription_events"], "user_id") == user_ids


def test_listening_events_are_plausible(data):
    duration = {t["track_id"]: t["duration_ms"] for t in data["tracks"]}

    for event in data["listening_events"]:
        assert SMALL.start <= event["event_ts"] < SMALL.end
        assert 0 < event["ms_played"] <= duration[event["track_id"]]


def test_subscriptions_start_with_started_event(data):
    first_event = {}
    for event in data["subscription_events"]:
        first_event.setdefault(event["user_id"], event["event_type"])

    assert set(first_event.values()) == {"started"}


def test_track_popularity_is_skewed(data):
    plays = Counter(e["track_id"] for e in data["listening_events"])
    top_1_percent = sum(count for _, count in plays.most_common(SMALL.num_tracks // 100))

    # The top 1% of tracks get far more than 1% of plays.
    assert top_1_percent / SMALL.num_listening_events > 0.15


def test_optional_fields_contain_nulls(data):
    assert any(u["birth_year"] is None for u in data["users"])
    assert any(e["device_type"] is None for e in data["listening_events"])


def test_events_are_streamed_lazily():
    config = GeneratorConfig(num_users=10, num_artists=2, num_tracks=10, num_listening_events=10**9)
    users = generate_users(config)
    tracks = generate_tracks(config, generate_artists(config))

    # A billion events would never fit in memory; a generator only builds what is consumed.
    first = list(islice(generate_listening_events(config, users, tracks, chunk_size=100), 5))
    assert len(first) == 5


# --- Export + Spark round trip ---------------------------------------------------------------


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_export_splits_into_part_files(tmp_path, fmt):
    counts = generate(SMALL, tmp_path, fmt, rows_per_file=2_000)

    parts = sorted(p.name for p in (tmp_path / "listening_events").iterdir())
    assert parts == [f"part-0000{i}.{fmt}" for i in range(3)]  # 5,000 rows / 2,000 per file
    assert counts["listening_events"] == SMALL.num_listening_events


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_exported_data_passes_ingestion_and_integrity(spark, tmp_path, fmt):
    counts = generate(SMALL, tmp_path, fmt)
    frames = {}
    for name, dataset in DATASETS.items():
        raw = read_dataset(spark, dataset, str(tmp_path / name), fmt)
        valid, rejected = split_valid_rejected(raw, dataset)
        assert rejected.count() == 0, f"{name} has rejected rows"
        assert valid.count() == counts[name]
        frames[name] = valid

    # Referential integrity in Spark: a left anti join keeps rows with no match.
    orphans = frames["listening_events"].join(frames["tracks"], "track_id", "left_anti")
    assert orphans.count() == 0
