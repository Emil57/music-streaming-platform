# Source data model

Raw datasets that land in the platform. Schemas are defined in
[`src/music_streaming/schemas.py`](../src/music_streaming/schemas.py), which is the source of truth.

```
artists 1───* tracks 1───* listening_events *───1 users 1───* subscription_events
```

## Datasets

| Dataset | Grain (one row = …) | Primary key | Foreign keys |
|---------|---------------------|-------------|--------------|
| `users` | a registered user | `user_id` | — |
| `artists` | an artist | `artist_id` | — |
| `tracks` | a track in the catalog | `track_id` | `artist_id` → `artists` |
| `listening_events` | one playback (stream) of a track by a user | `event_id` | `user_id` → `users`, `track_id` → `tracks` |
| `subscription_events` | one change in a user's subscription | `event_id` | `user_id` → `users` |

## Fields

Required = `nullable=False` in the schema. Rows missing a required field are rejected at ingestion.

### users
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `user_id` | string | yes | |
| `country` | string | yes | ISO 3166-1 alpha-2 (`MX`, `US`) |
| `signup_date` | date | yes | |
| `birth_year` | int | no | optional profile field |

### artists
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `artist_id` | string | yes | |
| `name` | string | yes | |
| `country` | string | no | |

### tracks
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `track_id` | string | yes | |
| `title` | string | yes | |
| `artist_id` | string | yes | |
| `genre` | string | no | |
| `duration_ms` | int | yes | |
| `release_date` | date | no | |
| `explicit` | boolean | yes | |

### listening_events
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_id` | string | yes | |
| `user_id` | string | yes | |
| `track_id` | string | yes | |
| `event_ts` | timestamp | yes | when playback started |
| `ms_played` | bigint | yes | long: sums over millions of plays overflow int |
| `country` | string | no | where the play happened |
| `device_type` | string | no | `mobile`, `desktop`, `smart_speaker`, … |

### subscription_events
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_id` | string | yes | |
| `user_id` | string | yes | |
| `event_ts` | timestamp | yes | |
| `event_type` | string | yes | `started`, `renewed`, `upgraded`, `downgraded`, `cancelled` |
| `plan` | string | yes | `free`, `premium`, `family`, `student` |
| `price` | decimal(10,2) | no | decimal, never float, for money; null for free plans |
| `currency` | string | no | ISO 4217 (`MXN`, `USD`) |

## Ingestion rules

- **No schema inference.** `read_dataset()` always applies the explicit schema.
- **File readers ignore `nullable=False`.** Spark reads every column from JSON/CSV as nullable,
  so `split_valid_rejected()` enforces required fields after reading.
- **Malformed rows are quarantined, not dropped.** Reading in `PERMISSIVE` mode keeps the raw
  text in `_corrupt_record`; rejected rows carry `_reject_reasons` explaining why.
