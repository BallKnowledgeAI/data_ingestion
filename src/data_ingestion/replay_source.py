from __future__ import annotations

import bisect
import json
import threading
from pathlib import Path
from typing import Callable

from .base_source import BaseDataSource
from .custom_types import DataEvent, EventType, SourceMetadata

# EventType values that may arrive from external datasets but are not in our
# taxonomy are mapped here rather than silently dropped.
_FALLBACK_TYPE = EventType.PASS


def _parse_event_type(raw: str) -> EventType:
    try:
        return EventType(raw.lower())
    except ValueError:
        return _FALLBACK_TYPE


class AnnotatedReplaySource(BaseDataSource):
    """
    Replays a sorted list of annotated match events at configurable speed.

    Parameters
    ----------
    source_id:
        Unique identifier for this source.
    dataset:
        Either a list of event dicts or a Path to a .json file.
        Each dict must have ``timestamp_ms`` and ``event_type``;
        ``payload``, ``match_id``, and ``annotations`` are optional.
    speed_factor:
        1.0 = real time.  50.0 = 50× faster.  Writable live.
    loop:
        If True the replay restarts from the beginning after REPLAY_COMPLETE.
    on_progress:
        Optional callback(pct: float, current_ms: int, total_ms: int).
    """

    def __init__(
        self,
        source_id: str,
        dataset: list[dict] | Path,
        speed_factor: float = 1.0,
        loop: bool = False,
        on_progress: Callable[[float, int, int], None] | None = None,
    ) -> None:
        super().__init__(source_id)

        if isinstance(dataset, Path):
            with dataset.open(encoding="utf-8") as fh:
                raw = json.load(fh)
        else:
            raw = list(dataset)

        self._events: list[dict] = sorted(raw, key=lambda e: e["timestamp_ms"])
        self._timestamps: list[int] = [e["timestamp_ms"] for e in self._events]

        self._speed_factor = speed_factor
        self._loop = loop
        self._on_progress = on_progress
        self._cursor = 0

        # Threading primitives
        self._stop_event = threading.Event()
        # Wakes the sleeping replay thread on seek() or speed_factor change
        self._wake_event = threading.Event()
        # Cleared by pause(), set by resume()
        self._pause_event = threading.Event()
        self._pause_event.set()  # not paused initially

        self._thread: threading.Thread | None = None

        # Pending annotation injections — written by main thread, drained by replay thread
        self._inject_lock = threading.Lock()
        self._pending_injections: list[dict] = []

    # ------------------------------------------------------------------ #
    # Runtime controls (all thread-safe)                                   #
    # ------------------------------------------------------------------ #

    @property
    def progress(self) -> float:
        if not self._timestamps:
            return 1.0
        return self._cursor / len(self._timestamps)

    @property
    def current_match_ms(self) -> int:
        if not self._timestamps or self._cursor == 0:
            return 0
        idx = min(self._cursor - 1, len(self._timestamps) - 1)
        return self._timestamps[idx]

    @property
    def speed_factor(self) -> float:
        return self._speed_factor

    @speed_factor.setter
    def speed_factor(self, value: float) -> None:
        self._speed_factor = value
        self._wake_event.set()

    def pause(self) -> None:
        """Suspend emission; the replay thread blocks until resume()."""
        self._pause_event.clear()

    def resume(self) -> None:
        """Unblock the replay thread."""
        self._pause_event.set()

    def seek(self, target_ms: int) -> None:
        """Jump to the nearest event at or after target_ms.  O(log n)."""
        idx = bisect.bisect_left(self._timestamps, target_ms)
        self._cursor = min(idx, len(self._timestamps) - 1)
        self._wake_event.set()

    def inject_annotation(
        self,
        label: str,
        timestamp_ms: int,
        author: str = "system",
    ) -> None:
        """Queue a synthetic event to be emitted at the given match time."""
        with self._inject_lock:
            self._pending_injections.append(
                {"timestamp_ms": timestamp_ms, "label": label, "author": author}
            )

    def wait_until_done(self, timeout: float | None = None) -> bool:
        """Block until replay finishes.  Returns True if completed, False on timeout."""
        if self._thread is None:
            return True
        self._thread.join(timeout=timeout)
        return not self._thread.is_alive()

    # ------------------------------------------------------------------ #
    # BaseDataSource overrides                                             #
    # ------------------------------------------------------------------ #

    def _build_metadata(self) -> SourceMetadata:
        return SourceMetadata(
            source_id=self._source_id,
            source_type="annotated_replay",
            description=f"Annotated replay — {len(self._events)} events",
        )

    def _start_streaming(self) -> None:
        self._stop_event.clear()
        self._wake_event.clear()
        self._thread = threading.Thread(
            target=self._replay_loop, name=f"replay-{self._source_id}", daemon=True
        )
        self._thread.start()

    def _stop_streaming(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        self._pause_event.set()  # unblock if paused
        if self._thread:
            self._thread.join(timeout=5.0)

    # ------------------------------------------------------------------ #
    # Replay loop (runs on background thread)                              #
    # ------------------------------------------------------------------ #

    def _replay_loop(self) -> None:
        while not self._stop_event.is_set():
            # --- honor pause ---
            self._pause_event.wait()
            if self._stop_event.is_set():
                break

            # --- all events consumed ---
            if self._cursor >= len(self._events):
                total_ms = self._timestamps[-1] if self._timestamps else 0
                self._emit(
                    self._make_event(EventType.REPLAY_COMPLETE, {}, timestamp_ms=total_ms)
                )
                if self._loop:
                    self._cursor = 0
                    continue
                break

            event_dict = self._events[self._cursor]
            current_ms: int = event_dict["timestamp_ms"]

            # drain any injected annotations due at or before this timestamp
            self._flush_injections(current_ms)

            # build and emit the event
            annotations = event_dict.get("annotations", [])
            event = self._make_event(
                event_type=_parse_event_type(event_dict["event_type"]),
                payload=event_dict.get("payload", {}),
                timestamp_ms=current_ms,
                match_id=event_dict.get("match_id"),
                metadata={"annotations": annotations} if annotations else {},
            )
            self._emit(event)
            self._cursor += 1

            # fire progress callback
            if self._on_progress and self._timestamps:
                total_ms = self._timestamps[-1]
                self._on_progress(self.progress, current_ms, total_ms)

            # sleep until next event, interruptible by wake_event
            if self._cursor < len(self._events):
                next_ms = self._events[self._cursor]["timestamp_ms"]
                sf = max(self._speed_factor, 1e-6)
                delay = (next_ms - current_ms) / 1000.0 / sf
                if delay > 0:
                    self._wake_event.wait(timeout=delay)
                    self._wake_event.clear()
                    # On wake: seek may have updated _cursor, or speed changed.
                    # Either way, loop back to the top and re-evaluate.

    def _flush_injections(self, up_to_ms: int) -> None:
        with self._inject_lock:
            due = [a for a in self._pending_injections if a["timestamp_ms"] <= up_to_ms]
            self._pending_injections = [
                a for a in self._pending_injections if a["timestamp_ms"] > up_to_ms
            ]
        for ann in sorted(due, key=lambda a: a["timestamp_ms"]):
            event = self._make_event(
                event_type=EventType.PASS,
                payload={"label": ann["label"], "author": ann["author"]},
                timestamp_ms=ann["timestamp_ms"],
                metadata={"injected_annotation": True},
            )
            self._emit(event)
