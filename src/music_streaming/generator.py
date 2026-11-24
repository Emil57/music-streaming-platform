"""Reproducible synthetic data for the source datasets.

Rows are produced as plain dicts (keyed by the schema field names) and streamed to disk, so
memory stays flat even for millions of listening events.

Usage:
    uv run msp-generate --events 1000000 --format json --output data/raw
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import accumulate, batched
from pathlib import Path
from typing import Any, Literal

from music_streaming.schemas import (
    ARTISTS,
    LISTENING_EVENTS,
    SUBSCRIPTION_EVENTS,
    TRACKS,
    USERS,
    Dataset,
)

Row = dict[str, Any]
OutputFormat = Literal["json", "csv"]

# country -> (weight, currency, price multiplier vs USD)
COUNTRIES = {
    "US": (30, "USD", Decimal("1")),
    "MX": (20, "MXN", Decimal("10")),
    "BR": (15, "BRL", Decimal("2")),
    "GB": (10, "GBP", Decimal("0.9")),
    "DE": (10, "EUR", Decimal("1")),
    "ES": (10, "EUR", Decimal("0.9")),
    "JP": (5, "JPY", Decimal("100")),
}
# plan -> (weight, monthly price in USD); ordered from cheapest to most expensive
PLANS = {
    "free": (60, None),
    "student": (5, Decimal("5.99")),
    "premium": (25, Decimal("11.99")),
    "family": (10, Decimal("19.99")),
}
GENRES = ["pop", "rock", "hip-hop", "latin", "electronic", "indie", "jazz", "classical"]
DEVICES = {"mobile": 60, "desktop": 25, "smart_speaker": 10, "tv": 5}
WORDS = [
    "midnight",
    "golden",
    "electric",
    "silent",
    "wild",
    "neon",
    "velvet",
    "broken",
    "summer",
    "lunar",
    "echo",
    "river",
    "fire",
    "glass",
    "paper",
    "ocean",
    "desert",
    "city",
    "heart",
    "dream",
    "shadow",
    "light",
    "storm",
    "satellite",
]


@dataclass(frozen=True)
class GeneratorConfig:
    num_users: int = 10_000
    num_artists: int = 1_000
    num_tracks: int = 50_000
    num_listening_events: int = 1_000_000
    seed: int = 42
    start_date: date = date(2026, 1, 1)
    days: int = 30

    def __post_init__(self) -> None:
        for name in ("num_users", "num_artists", "num_tracks", "num_listening_events", "days"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1, got {getattr(self, name)}")

    @property
    def start(self) -> datetime:
        return datetime.combine(self.start_date, datetime.min.time())

    @property
    def end(self) -> datetime:
        return self.start + timedelta(days=self.days)


def _rng(config: GeneratorConfig, stream: str) -> random.Random:
    # An independent stream per dataset: changing the number of events does not change users.
    return random.Random(f"{config.seed}:{stream}")


def _pick(rng: random.Random, weighted: dict[str, Any]) -> str:
    keys = list(weighted)
    weights = [v[0] if isinstance(v, tuple) else v for v in weighted.values()]
    return rng.choices(keys, weights=weights)[0]


def _maybe(rng: random.Random, value: Any, null_rate: float) -> Any:
    return None if rng.random() < null_rate else value


def _long_tail_cum_weights(rng: random.Random, n: int, exponent: float) -> list[float]:
    """Power-law weights (rank ** -exponent) assigned to items in random order."""
    weights = [(rank + 1) ** -exponent for rank in range(n)]
    rng.shuffle(weights)
    return list(accumulate(weights))


def _title(rng: random.Random, words: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(words)).title()


def _price(plan: str, country: str) -> Decimal | None:
    usd = PLANS[plan][1]
    return None if usd is None else (usd * COUNTRIES[country][2]).quantize(Decimal("0.01"))


# --- Generators ------------------------------------------------------------------------------


def generate_users(config: GeneratorConfig) -> list[Row]:
    rng = _rng(config, "users")
    return [
        {
            "user_id": f"u{i:08d}",
            "country": _pick(rng, COUNTRIES),
            # Everyone signs up during the year before the event window starts.
            "signup_date": config.start_date - timedelta(days=rng.randint(1, 365)),
            "birth_year": _maybe(rng, rng.randint(1950, 2010), null_rate=0.2),
        }
        for i in range(1, config.num_users + 1)
    ]


def generate_artists(config: GeneratorConfig) -> list[Row]:
    rng = _rng(config, "artists")
    return [
        {
            "artist_id": f"a{i:07d}",
            "name": _title(rng, 2),
            "country": _maybe(rng, _pick(rng, COUNTRIES), null_rate=0.1),
        }
        for i in range(1, config.num_artists + 1)
    ]


def generate_tracks(config: GeneratorConfig, artists: list[Row]) -> list[Row]:
    rng = _rng(config, "tracks")
    artist_genre = {a["artist_id"]: rng.choice(GENRES) for a in artists}
    tracks = []
    for i in range(1, config.num_tracks + 1):
        artist_id = rng.choice(artists)["artist_id"]
        tracks.append(
            {
                "track_id": f"t{i:08d}",
                "title": _title(rng, rng.randint(1, 3)),
                "artist_id": artist_id,
                "genre": _maybe(rng, artist_genre[artist_id], null_rate=0.05),
                "duration_ms": rng.randint(90_000, 420_000),
                "release_date": _maybe(
                    rng, date(2000, 1, 1) + timedelta(days=rng.randint(0, 9_500)), null_rate=0.03
                ),
                "explicit": rng.random() < 0.15,
            }
        )
    return tracks


def generate_listening_events(
    config: GeneratorConfig, users: list[Row], tracks: list[Row], chunk_size: int = 100_000
) -> Iterator[Row]:
    """Stream listening events; only references users and tracks that exist."""
    rng = _rng(config, "listening_events")
    # Skew: a few heavy listeners and a few hit tracks account for most plays.
    user_weights = _long_tail_cum_weights(rng, len(users), exponent=0.8)
    track_weights = _long_tail_cum_weights(rng, len(tracks), exponent=1.1)
    window_seconds = config.days * 86_400
    countries = list(COUNTRIES)

    event_number = 0
    remaining = config.num_listening_events
    while remaining:
        size = min(chunk_size, remaining)
        chosen_users = rng.choices(users, cum_weights=user_weights, k=size)
        chosen_tracks = rng.choices(tracks, cum_weights=track_weights, k=size)
        for user, track in zip(chosen_users, chosen_tracks, strict=True):
            event_number += 1
            skipped = rng.random() < 0.25
            ms_played = (
                rng.randint(5_000, 30_000) if skipped else rng.randint(30_000, track["duration_ms"])
            )
            # Mostly played from home; sometimes while travelling.
            country = user["country"] if rng.random() < 0.95 else rng.choice(countries)
            yield {
                "event_id": f"le{event_number:010d}",
                "user_id": user["user_id"],
                "track_id": track["track_id"],
                "event_ts": config.start + timedelta(seconds=rng.randrange(window_seconds)),
                "ms_played": min(ms_played, track["duration_ms"]),
                "country": _maybe(rng, country, null_rate=0.01),
                "device_type": _maybe(rng, _pick(rng, DEVICES), null_rate=0.02),
            }
        remaining -= size


def generate_subscription_events(config: GeneratorConfig, users: list[Row]) -> Iterator[Row]:
    """Per user: a `started` event at signup, then monthly renewals, plan changes or a cancel."""
    rng = _rng(config, "subscription_events")
    plan_order = list(PLANS)
    event_number = 0

    def event(user: Row, ts: datetime, event_type: str, plan: str) -> Row:
        nonlocal event_number
        event_number += 1
        price = None if event_type == "cancelled" else _price(plan, user["country"])
        return {
            "event_id": f"se{event_number:09d}",
            "user_id": user["user_id"],
            "event_ts": ts,
            "event_type": event_type,
            "plan": plan,
            "price": price,
            "currency": None if price is None else COUNTRIES[user["country"]][1],
        }

    for user in users:
        plan = _pick(rng, PLANS)
        ts = datetime.combine(user["signup_date"], datetime.min.time()) + timedelta(
            seconds=rng.randrange(86_400)
        )
        yield event(user, ts, "started", plan)
        if plan == "free":
            continue
        ts += timedelta(days=30)
        while ts < config.end:
            roll = rng.random()
            if roll < 0.04:
                yield event(user, ts, "cancelled", plan)
                break
            if roll < 0.10:
                new_plan = rng.choice([p for p in plan_order if p not in ("free", plan)])
                changed = plan_order.index(new_plan) > plan_order.index(plan)
                plan = new_plan
                yield event(user, ts, "upgraded" if changed else "downgraded", plan)
            else:
                yield event(user, ts, "renewed", plan)
            ts += timedelta(days=30)


# --- Writers ---------------------------------------------------------------------------------


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)  # JSON number; Spark parses it into DecimalType exactly
    return value


def _csv_value(value: Any) -> str:
    if value is None:
        return ""  # Spark's CSV reader treats empty fields as null
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(_json_value(value))


def write_dataset(
    rows: Iterable[Row],
    dataset: Dataset,
    output_dir: Path,
    fmt: OutputFormat = "json",
    rows_per_file: int = 250_000,
) -> int:
    """Write rows to ``output_dir/<dataset>/part-NNNNN.<fmt>`` and return the row count.

    The dataset folder is replaced, like Spark's ``mode("overwrite")``.
    """
    target = Path(output_dir) / dataset.name
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    columns = dataset.schema.fieldNames()
    count = 0
    for part, batch in enumerate(batched(rows, rows_per_file, strict=False)):
        path = target / f"part-{part:05d}.{fmt}"
        with path.open("w", encoding="utf-8", newline="") as f:
            if fmt == "json":
                for row in batch:
                    f.write(json.dumps({c: _json_value(row[c]) for c in columns}) + "\n")
            else:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows([_csv_value(row[c]) for c in columns] for row in batch)
        count += len(batch)
    return count


def generate(
    config: GeneratorConfig,
    output_dir: Path,
    fmt: OutputFormat = "json",
    rows_per_file: int = 250_000,
) -> dict[str, int]:
    """Generate every dataset and write it under ``output_dir``. Returns row counts."""
    users = generate_users(config)
    artists = generate_artists(config)
    tracks = generate_tracks(config, artists)
    outputs = [
        (USERS, users),
        (ARTISTS, artists),
        (TRACKS, tracks),
        (LISTENING_EVENTS, generate_listening_events(config, users, tracks)),
        (SUBSCRIPTION_EVENTS, generate_subscription_events(config, users)),
    ]
    return {
        dataset.name: write_dataset(rows, dataset, output_dir, fmt, rows_per_file)
        for dataset, rows in outputs
    }


def main(argv: list[str] | None = None) -> None:
    defaults = GeneratorConfig()
    parser = argparse.ArgumentParser(description="Generate synthetic music streaming data.")
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    parser.add_argument("--users", type=int, default=defaults.num_users)
    parser.add_argument("--artists", type=int, default=defaults.num_artists)
    parser.add_argument("--tracks", type=int, default=defaults.num_tracks)
    parser.add_argument("--events", type=int, default=defaults.num_listening_events)
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--start-date", type=date.fromisoformat, default=defaults.start_date)
    parser.add_argument("--days", type=int, default=defaults.days)
    parser.add_argument("--rows-per-file", type=int, default=250_000)
    args = parser.parse_args(argv)

    config = GeneratorConfig(
        num_users=args.users,
        num_artists=args.artists,
        num_tracks=args.tracks,
        num_listening_events=args.events,
        seed=args.seed,
        start_date=args.start_date,
        days=args.days,
    )
    started = time.perf_counter()
    counts = generate(config, args.output, args.format, args.rows_per_file)
    for name, count in counts.items():
        print(f"{name:<20} {count:>12,}")
    print(f"Wrote {args.format} to {args.output} in {time.perf_counter() - started:.1f}s")
