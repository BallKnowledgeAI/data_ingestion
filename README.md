# BallKnowledge — Module 1: Data Ingestion

The data plumbing layer for the BallKnowledge tactical soccer analysis system. It produces a single, consistent stream of `DataEvent` objects that all downstream modules consume — regardless of whether data originates from a replayed annotated dataset, a live API feed, or a third-party provider like StatsBomb.

**Core principle: consumers never know where data comes from.** They depend only on the `DataSource` interface and react to `DataEvent` objects.

---

## Features

- **Universal event envelope** — every source emits the same `DataEvent` type; downstream modules are fully source-agnostic
- **Extensible event taxonomy** — 16 canonical event types across ball actions, positional data, match lifecycle, and system signals
- **StatsBomb integration** — stream real open-data matches with rich payloads: passes, shots, carries, lineups, freeze frames
- **Annotated replay** — replay JSON match datasets at any speed with pause, resume, seek, and live annotation injection
- **Pub/sub registry** — wildcard, typed, and source-specific subscriptions; auto-installs on sources added after the fact
- **Composite streams** — merge multiple feeds (e.g. tracking + events) into one unified source
- **Thread-safe by design** — all public APIs are safe to call from any thread

---

## Project Structure

```
src/
└── data_ingestion/
    ├── __init__.py           # Public API surface
    ├── custom_types.py       # DataEvent, EventType, SourceMetadata, SourceState
    ├── base_source.py        # BaseDataSource ABC + DataSource Protocol
    ├── replay_source.py      # AnnotatedReplaySource (dataset replayer)
    ├── statsbomb_source.py   # StatsBombSource (open-data streaming)
    ├── registry.py           # DataSourceRegistry + CompositeSource
    └── simulation_demo.py    # End-to-end demo (2019 UCL Final)
data/
└── sample_match.json         # Sample annotated match dataset
```

---

## Installation

No package manager file exists yet — install dependencies directly:

```bash
pip install statsbombpy pandas
```

The core module (`custom_types`, `base_source`, `registry`, `replay_source`) has no third-party dependencies. `statsbombpy` and `pandas` are only required if you use `StatsBombSource`.

---

## Quick Start

### Run the demo

Streams the 2019 UCL Final (Tottenham 0–2 Liverpool) from StatsBomb open data, demonstrates pause/resume/seek, and prints a full event breakdown:

```bash
python -m src.data_ingestion.simulation_demo
```

### Replay a local dataset

```python
from src.data_ingestion import AnnotatedReplaySource, DataSourceRegistry, EventType

source = AnnotatedReplaySource(
    source_id    = "match_001",
    dataset      = "data/sample_match.json",  # or a pre-loaded list
    speed_factor = 50.0,                       # 50× real-time
)

registry = DataSourceRegistry()
registry.register(source)

registry.subscribe_all(EventType.SHOT, lambda e: print(e.payload))
registry.connect_all()
```

### Stream a StatsBomb match

```python
from src.data_ingestion import StatsBombSource, DataSourceRegistry, EventType

source = StatsBombSource(
    source_id        = "ucl_final_2019",
    match_id         = 22912,
    include_tracking = True,
    tracking_hz      = 5.0,
)

registry = DataSourceRegistry()
registry.register(source)
registry.subscribe_all(None, lambda e: print(e.event_type, e.timestamp_ms))
registry.connect_all()
```

---

## Core Types

### `DataEvent` — the universal envelope

| Field | Type | Description |
|---|---|---|
| `timestamp_ms` | `int` | Match-clock time in milliseconds from kick-off |
| `source_id` | `str` | Which source emitted this event |
| `event_type` | `EventType` | Canonical event category |
| `payload` | `dict` | Domain-specific data for this event type |
| `sequence_number` | `int` | Monotonically increasing per source |
| `match_id` | `str \| None` | Optional match identifier |
| `metadata` | `dict` | Source-specific extras |

### `EventType` — canonical taxonomy

```
Ball actions    PASS · SHOT · DRIBBLE · CLEARANCE · INTERCEPTION
Positional      PLAYER_POSITION · BALL_POSITION
Match lifecycle MATCH_START · MATCH_END · HALF_START · HALF_END · SUBSTITUTION
Spatial         PRESSURE · CARRY
System          SOURCE_CONNECTED · SOURCE_DISCONNECTED · REPLAY_COMPLETE
```

Extending the taxonomy is a one-line addition to the `EventType` enum — no other file changes required.

---

## `AnnotatedReplaySource` — dataset replayer

Replays a sorted list of annotated match events at configurable speed.

### Dataset format

```python
{
  "timestamp_ms": 3712000,
  "event_type":   "pass",       # must match an EventType value
  "payload":      { ... },
  "match_id":     "match_001",  # optional
  "annotations":  [             # optional analyst notes
    { "label": "high press triggered", "author": "analyst_1" }
  ]
}
```

### Runtime controls

| Method / Property | Effect |
|---|---|
| `source.pause()` | Suspend emission; blocks the replay thread |
| `source.resume()` | Unblock and continue |
| `source.seek(target_ms)` | Jump to nearest event at or after `target_ms` — O(log n) binary search |
| `source.inject_annotation(label, timestamp_ms, author)` | Queue a synthetic event mid-replay |
| `source.speed_factor = 2.0` | Adjust speed live without restarting |
| `source.progress` | `float` in [0.0, 1.0] — current playback position |
| `source.current_match_ms` | Match timestamp of the last emitted event |

---

## `DataSourceRegistry` — lifecycle and fan-out

Owns the lifecycle of all active sources and routes events to consumers.

```python
registry = DataSourceRegistry()
registry.register(replay_source)
registry.register(live_api_source)

# Wildcard — all event types from every source
registry.subscribe_all(None, handle_any_event)

# Typed — only shots from every source
registry.subscribe_all(EventType.SHOT, xg_model.on_shot)

# Source-specific
registry.subscribe_source("match_001", EventType.PASS, press_tracker.on_pass)

registry.connect_all()
# ...
registry.disconnect_all()
```

Registry-level subscriptions are automatically installed on sources added later.

---

## `CompositeSource` — unified streams

Wrap multiple sources into one when downstream modules should see them as a single feed:

```python
from src.data_ingestion import CompositeSource

composite = CompositeSource(
    composite_id = "match_001_unified",
    sources      = [tracking_source, event_source],
)
registry.register(composite)
```

---

## Adding a New Source

Subclass `BaseDataSource` and implement three methods:

```python
from src.data_ingestion import BaseDataSource, SourceMetadata, EventType
import threading

class LiveAPISource(BaseDataSource):
    def _build_metadata(self) -> SourceMetadata:
        return SourceMetadata(source_id=self._source_id, source_type="live_api")

    def _start_streaming(self) -> None:
        self._thread = threading.Thread(target=self._ws_loop, daemon=True)
        self._thread.start()

    def _stop_streaming(self) -> None:
        self._ws.close()
        self._thread.join()

    def _ws_loop(self) -> None:
        for raw_msg in self._ws:
            event = self._make_event(
                event_type   = EventType(raw_msg["type"]),
                payload      = raw_msg["data"],
                timestamp_ms = raw_msg["ts"],
            )
            self._emit(event)
```

`BaseDataSource` provides: state machine, thread-safe subscriber registry, sequence numbering, fan-out delivery, and error isolation. Your subclass only handles the data-fetching logic.

---

## Subscription Patterns for Downstream Modules

```python
# Tactical analyser — all events
registry.subscribe_all(None, tactical_analyser.on_event)

# xG model — shots only
registry.subscribe_all(EventType.SHOT, xg_model.on_shot)

# Pressing tracker — pressure + passes
registry.subscribe_all(EventType.PRESSURE, press_tracker.on_pressure)
registry.subscribe_all(EventType.PASS,     press_tracker.on_pass)

# Visualiser — high-frequency positional feed
registry.subscribe_all(EventType.PLAYER_POSITION, visualiser.on_position)
```

Handlers must be **non-blocking**. For heavy work, enqueue onto a `queue.Queue` and process on a dedicated thread.

---

## Thread-Safety Guarantees

| Operation | Safe from any thread |
|---|---|
| `subscribe` / `unsubscribe` | Yes |
| `pause` / `resume` | Yes |
| `seek(target_ms)` | Yes |
| `speed_factor = x` | Yes |
| `inject_annotation(...)` | Yes |
| Internal `_emit` | Yes — iterates over a snapshot copy of subscribers |

---

## What Module 2 Receives

Module 2 (tactical analysis) should depend only on:

- `DataSource` — the Protocol type
- `DataEvent` — the event envelope
- `EventType` — to filter subscriptions
- `DataSourceRegistry` — to wire sources at startup

It should never import from `replay_source.py` or `statsbomb_source.py` directly. All source wiring belongs in the application entry point.
