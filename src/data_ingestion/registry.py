from __future__ import annotations

import uuid
from typing import Callable

from .base_source import DataSource
from .custom_types import DataEvent, EventType, SourceMetadata, SourceState


class DataSourceRegistry:
    """
    Owns the lifecycle of all registered sources and routes events to consumers.

    Registry-level subscriptions are automatically installed on sources added
    later — you subscribe once and it applies to all future registrations.
    """

    def __init__(self) -> None:
        # source_id -> DataSource
        self._sources: dict[str, DataSource] = {}
        # registry sub_id -> (target_source_id | None, event_type | None, handler)
        self._registry_subs: dict[str, tuple[str | None, EventType | None, Callable]] = {}
        # source_id -> list of (source-level sub_id) installed for registry subs
        self._source_sub_ids: dict[str, list[str]] = {}

    def register(self, source: DataSource) -> None:
        sid = source.metadata.source_id
        self._sources[sid] = source
        self._source_sub_ids.setdefault(sid, [])

        # install existing registry subscriptions onto this source
        for _, (target_sid, event_type, handler) in self._registry_subs.items():
            if target_sid is None or target_sid == sid:
                source_sub_id = source.subscribe(event_type, handler)
                self._source_sub_ids[sid].append(source_sub_id)

    def subscribe_all(
        self,
        event_type: EventType | None,
        handler: Callable[[DataEvent], None],
    ) -> str:
        """Subscribe to events from every source (present and future)."""
        reg_sub_id = str(uuid.uuid4())
        self._registry_subs[reg_sub_id] = (None, event_type, handler)

        # install on already-registered sources
        for sid, source in self._sources.items():
            source_sub_id = source.subscribe(event_type, handler)
            self._source_sub_ids[sid].append(source_sub_id)

        return reg_sub_id

    def subscribe_source(
        self,
        source_id: str,
        event_type: EventType | None,
        handler: Callable[[DataEvent], None],
    ) -> str:
        """Subscribe to events from a specific source only."""
        reg_sub_id = str(uuid.uuid4())
        self._registry_subs[reg_sub_id] = (source_id, event_type, handler)

        if source_id in self._sources:
            source_sub_id = self._sources[source_id].subscribe(event_type, handler)
            self._source_sub_ids[source_id].append(source_sub_id)

        return reg_sub_id

    def unsubscribe(self, reg_sub_id: str) -> None:
        self._registry_subs.pop(reg_sub_id, None)

    def connect_all(self) -> None:
        for source in self._sources.values():
            source.connect()

    def disconnect_all(self) -> None:
        for source in self._sources.values():
            source.disconnect()

    @property
    def sources(self) -> dict[str, DataSource]:
        return dict(self._sources)


class CompositeSource:
    """
    Wraps multiple sources into a single unified stream.

    Useful when two feeds cover the same match (e.g. a tracking feed
    and an event-annotation feed).  Downstream consumers subscribe to
    the composite and receive events from all inner sources.
    """

    def __init__(self, composite_id: str, sources: list[DataSource]) -> None:
        self._composite_id = composite_id
        self._sources = list(sources)
        self._state = SourceState.IDLE
        self._metadata = SourceMetadata(
            source_id=composite_id,
            source_type="composite",
            description=f"Composite of {len(sources)} source(s)",
        )
        # composite sub_id -> list of (inner source, inner sub_id)
        self._composite_subs: dict[str, list[tuple[DataSource, str]]] = {}

    @property
    def metadata(self) -> SourceMetadata:
        return self._metadata

    @property
    def state(self) -> SourceState:
        return self._state

    def connect(self) -> None:
        for source in self._sources:
            source.connect()
        self._state = SourceState.STREAMING

    def disconnect(self) -> None:
        for source in self._sources:
            source.disconnect()
        self._state = SourceState.DISCONNECTED

    def subscribe(
        self,
        event_type: EventType | None,
        handler: Callable[[DataEvent], None],
    ) -> str:
        composite_sub_id = str(uuid.uuid4())
        inner_subs: list[tuple[DataSource, str]] = []
        for source in self._sources:
            inner_sub_id = source.subscribe(event_type, handler)
            inner_subs.append((source, inner_sub_id))
        self._composite_subs[composite_sub_id] = inner_subs
        return composite_sub_id

    def unsubscribe(self, subscription_id: str) -> None:
        if subscription_id in self._composite_subs:
            for source, inner_sub_id in self._composite_subs.pop(subscription_id):
                source.unsubscribe(inner_sub_id)
