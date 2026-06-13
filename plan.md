# Module 1 — Data Ingestion: Implementation Strategy

## Overview

Module 1 defines the data plumbing for the entire tactical analysis system. Its job is to produce a single, consistent stream of `DataEvent` objects that all downstream modules consume — regardless of whether the data comes from a replayed annotated dataset, a live API feed, or a third-party provider like StatsBomb.

The core design principle: **consumers never know where data comes from.** They depend only on the `DataSource` interface and react to `DataEvent` objects.

---

## File Structure

```
src/
└── data_ingestion/
    ├── __init__.py          ← public API surface
    ├── types.py             ← DataEvent, EventType, SourceMetadata, SourceState
    ├── base_source.py       ← DataSource protocol + BaseDataSource ABC
    ├── replay_source.py     ← AnnotatedReplaySource (dataset replayer)
    ├── registry.py          ← DataSourceRegistry + CompositeSource
    └── simulation_demo.py   ← synthetic match generator + end-to-end demo
```

---

## Core Abstractions

### `DataEvent` — the universal envelope

Every event in the system is wrapped in a `DataEvent`. Downstream modules only ever see this type.

| Field | Type | Description |
| --- | --- | --- |
| `timestamp_ms` | int | Match-clock time in milliseconds |
| `source_id` | str | Which source emitted this event |
| `event_type` | EventType | Canonical event category (enum) |
| `payload` | dict | Domain-specific data for this event type |
| `sequence_number` | int | Monotonically increasing per source |
| `match_id` | str | None | Optional match identifier |
| `metadata` | dict | Source-specific extras |

### `EventType` — canonical event taxonomy

```
Ball events       PASS · SHOT · DRIBBLE · CLEARANCE · INTERCEPTION
Positional        PLAYER_POSITION · BALL_POSITION
Match lifecycle   MATCH_START · MATCH_END · HALF_START · HALF_END · SUBSTITUTION
Spatial           PRESSURE · CARRY
System            SOURCE_CONNECTED · SOURCE_DISCONNECTED · REPLAY_COMPLETE
```

Extending the taxonomy is a one-line addition to the `EventType` enum. No other file changes are needed.

### `DataSource` — the interface all sources satisfy

```python
class DataSource(Protocol):
    metadata:   SourceMetadata
    state:      SourceState

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def subscribe(self, event_type: EventType | None, handler: Callable) -> str: ...
    def unsubscribe(self, subscription_id: str) -> None: ...
```

This is a structural Protocol — any class that implements these four methods satisfies the interface without inheriting from anything.

---

## `BaseDataSource` — what new sources inherit

`BaseDataSource` handles all shared plumbing so that concrete sources only implement the data-fetching logic:

- Thread-safe subscriber registry with per-event-type fan-out
- State machine (IDLE → CONNECTED → STREAMING → DISCONNECTED)
- `_emit(event)` — delivers to all matching handlers
- `_make_event(...)` — constructs a properly sequenced `DataEvent`
- Error isolation — a crashing handler doesn't kill the source thread

**To add a new source, override three methods:**

```python
class MyNewSource(BaseDataSource):
    def _build_metadata(self) -> SourceMetadata: ...
    def _start_streaming(self) -> None: ...   # launch background thread
    def _stop_streaming(self) -> None: ...    # clean shutdown
```

---

## `AnnotatedReplaySource` — the dataset replayer

Replays a sorted list of annotated match events at configurable speed.

### Instantiation

```python
source = AnnotatedReplaySource(
    source_id    = "replay_1",
    dataset      = dataset_list,   # or Path to a .json file
    speed_factor = 1.0,            # 1.0 = real time, 50.0 = 50× faster
    loop         = False,
    on_progress  = my_callback,    # optional (pct, current_ms, total_ms)
)
```

### Dataset format

Each event in the dataset is a dict:

```python
{
  "timestamp_ms": 3712000,       # match clock (ms from kick-off)
  "event_type":   "pass",        # must match EventType enum values
  "payload":      { ... },       # domain-specific fields
  "match_id":     "match_001",   # optional
  "annotations":  [              # optional analyst notes
    { "label": "high press triggered", "author": "analyst_1" }
  ]
}
```

### Runtime controls

| Method | Effect |
| --- | --- |
| `source.pause()` | Suspend emission; state machine blocks until resumed |
| `source.resume()` | Unblock the replay thread |
| `source.seek(target_ms)` | Jump to nearest event at or after `target_ms` |
| `source.inject_annotation(label, timestamp_ms, author)` | Queue a synthetic event mid-replay |
| `source.speed_factor = 2.0` | Change speed live without restarting |
| `source.progress` | Float [0.0, 1.0] — current playback position |
| `source.current_match_ms` | Match timestamp of the last emitted event |

### Seek algorithm

Binary search on sorted `timestamp_ms` values — O(log n). Safe to call from any thread.

### Speed mechanics

Between consecutive events, the replayer sleeps for:

```
sleep_seconds = (next_ts_ms - current_ts_ms) / 1000 / speed_factor
```

The sleep is interruptible — `stop()`, `seek()`, and `resume()` all wake it immediately.

---

## `DataSourceRegistry` — lifecycle and fan-out

The registry owns the lifecycle of all active sources and routes events to consumers.

```python
registry = DataSourceRegistry()
registry.register(replay_source)
registry.register(live_api_source)

# Wildcard — receives everything from every source
registry.subscribe_all(None, handle_any_event)

# Typed — only shots, from every source
registry.subscribe_all(EventType.SHOT, handle_shot)

# Source-specific — only from one source
registry.subscribe_source("replay_1", EventType.PASS, handle_pass)

registry.connect_all()
# ... system runs ...
registry.disconnect_all()
```

Registry-level subscriptions are automatically installed on sources added later — you register once, it works for all future sources.

---

## `CompositeSource` — merging multiple feeds

When two sources cover the same match (e.g. a tracking feed + an event feed), wrap them in a `CompositeSource` so downstream consumers see one unified stream:

```python
composite = CompositeSource(
    composite_id = "match_001_unified",
    sources      = [tracking_source, event_source],
)
registry.register(composite)
```

---

## Extensibility Playbook

### Adding a live WebSocket source

```python
class LiveAPISource(BaseDataSource):
    def _build_metadata(self) -> SourceMetadata:
        return SourceMetadata(source_id=self._source_id, source_type="live_api")

    def _start_streaming(self) -> None:
        self._ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self._ws_thread.start()

    def _stop_streaming(self) -> None:
        self._ws.close()
        self._ws_thread.join()

    def _ws_loop(self) -> None:
        for raw_msg in self._ws:
            event = self._make_event(
                event_type   = EventType(raw_msg["type"]),
                payload      = raw_msg["data"],
                timestamp_ms = raw_msg["ts"],
            )
            self._emit(event)
```

No changes anywhere else — register it, subscribe to it, done.

### Adding a StatsBomb batch source

```python
class StatsBombSource(BaseDataSource):
    def _start_streaming(self) -> None:
        threading.Thread(target=self._load_and_emit, daemon=True).start()

    def _load_and_emit(self) -> None:
        events = statsbombpy.sb.events(match_id=self._match_id)
        for _, row in events.sort_values("index").iterrows():
            event = self._make_event(
                event_type   = self._map_type(row["type"]),
                payload      = row.to_dict(),
                timestamp_ms = self._to_ms(row["timestamp"]),
            )
            self._emit(event)
```

---

## Subscription patterns for downstream modules

```python
# Tactical analyser — receive all events
registry.subscribe_all(None, tactical_analyser.on_event)

# xG model — only shots
registry.subscribe_all(EventType.SHOT, xg_model.on_shot)

# Pressing tracker — pressure + pass events
registry.subscribe_all(EventType.PRESSURE, press_tracker.on_pressure)
registry.subscribe_all(EventType.PASS, press_tracker.on_pass)

# Visualiser — positions only (high frequency)
registry.subscribe_all(EventType.PLAYER_POSITION, visualiser.on_position)
```

Handlers must be **non-blocking**. If a handler needs to do heavy work, it should enqueue onto a `queue.Queue` and process on a separate thread.

---

## Thread-safety guarantees

| Operation | Safe from any thread? |
| --- | --- |
| `subscribe` / `unsubscribe` | Yes |
| `pause` / `resume` | Yes |
| `seek(target_ms)` | Yes |
| `speed_factor = x` | Yes (atomic float write) |
| `inject_annotation` | Yes |
| `_emit` (internal) | Yes — handlers are called under a snapshot copy |

The `_emit` implementation copies the current subscriber list before calling handlers, so subscribing or unsubscribing mid-emission is safe.

---

## Running the simulation

```bash
# From the project root
python -m src.data_ingestion.simulation_demo
```

This generates a synthetic 15-minute match, runs the replayer at 50× speed, exercises pause/resume/seek/annotation injection, and prints a full event breakdown on completion.

---

## What Module 2 receives

Module 2 (tactical analysis) should depend only on:

- `DataSource` — the Protocol type
- `DataEvent` — the event envelope
- `EventType` — to filter subscriptions
- `DataSourceRegistry` — to wire up sources at startup

It should never import from `replay_source.py` or `registry.py` directly. All wiring happens in the application entry point.