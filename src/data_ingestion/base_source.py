from __future__ import annotations

import threading
import uuid
from abc import ABC, abstractmethod
from typing import Callable, Protocol, runtime_checkable

from .custom_types import DataEvent, EventType, SourceMetadata, SourceState


@runtime_checkable
class DataSource(Protocol):
    """Structural interface all data sources satisfy."""

    metadata: SourceMetadata
    state: SourceState

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def subscribe(
        self,
        event_type: EventType | None,
        handler: Callable[[DataEvent], None],
    ) -> str: ...
    def unsubscribe(self, subscription_id: str) -> None: ...


class BaseDataSource(ABC):
    """
    Shared plumbing for all concrete sources.

    Subclasses implement exactly three methods:
        _build_metadata()  — describe this source
        _start_streaming() — launch background data thread
        _stop_streaming()  — clean shutdown of that thread

    Everything else (state machine, fan-out, sequencing, error isolation)
    lives here.
    """

    def __init__(self, source_id: str) -> None:
        self._source_id = source_id
        self._state = SourceState.IDLE
        self._sequence = 0
        # Protects _subscriptions and _sequence
        self._lock = threading.Lock()
        # subscription_id -> (event_type | None, handler)
        self._subscriptions: dict[str, tuple[EventType | None, Callable]] = {}
        # Lazy — built on first access so subclass __init__ can finish first
        self._metadata: SourceMetadata | None = None

    # ------------------------------------------------------------------ #
    # Public interface (satisfies DataSource Protocol)                     #
    # ------------------------------------------------------------------ #

    @property
    def metadata(self) -> SourceMetadata:
        if self._metadata is None:
            self._metadata = self._build_metadata()
        return self._metadata

    @property
    def state(self) -> SourceState:
        return self._state

    def connect(self) -> None:
        with self._lock:
            if self._state != SourceState.IDLE:
                raise RuntimeError(
                    f"Cannot connect: source '{self._source_id}' is in state {self._state}"
                )
            self._state = SourceState.CONNECTED

        self._emit(self._make_event(EventType.SOURCE_CONNECTED, {}, timestamp_ms=0))
        self._state = SourceState.STREAMING
        self._start_streaming()

    def disconnect(self) -> None:
        self._stop_streaming()
        self._state = SourceState.DISCONNECTED
        self._emit(self._make_event(EventType.SOURCE_DISCONNECTED, {}, timestamp_ms=0))

    def subscribe(
        self,
        event_type: EventType | None,
        handler: Callable[[DataEvent], None],
    ) -> str:
        sub_id = str(uuid.uuid4())
        with self._lock:
            self._subscriptions[sub_id] = (event_type, handler)
        return sub_id

    def unsubscribe(self, subscription_id: str) -> None:
        with self._lock:
            self._subscriptions.pop(subscription_id, None)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _emit(self, event: DataEvent) -> None:
        """Fan-out to all matching subscribers. Crashing handlers are isolated."""
        with self._lock:
            snapshot = list(self._subscriptions.values())
        for event_type, handler in snapshot:
            if event_type is None or event_type == event.event_type:
                try:
                    handler(event)
                except Exception:
                    pass

    def _make_event(
        self,
        event_type: EventType,
        payload: dict,
        timestamp_ms: int,
        match_id: str | None = None,
        metadata: dict | None = None,
    ) -> DataEvent:
        with self._lock:
            self._sequence += 1
            seq = self._sequence
        return DataEvent(
            timestamp_ms=timestamp_ms,
            source_id=self._source_id,
            event_type=event_type,
            payload=payload,
            sequence_number=seq,
            match_id=match_id,
            metadata=metadata or {},
        )

    # ------------------------------------------------------------------ #
    # Abstract — subclasses implement these three                          #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def _build_metadata(self) -> SourceMetadata: ...

    @abstractmethod
    def _start_streaming(self) -> None: ...

    @abstractmethod
    def _stop_streaming(self) -> None: ...
