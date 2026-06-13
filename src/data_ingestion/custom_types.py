from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    # Ball events
    PASS = "pass"
    SHOT = "shot"
    DRIBBLE = "dribble"
    CLEARANCE = "clearance"
    INTERCEPTION = "interception"
    # Positional
    PLAYER_POSITION = "player_position"
    BALL_POSITION = "ball_position"
    # Match lifecycle
    MATCH_START = "match_start"
    MATCH_END = "match_end"
    HALF_START = "half_start"
    HALF_END = "half_end"
    SUBSTITUTION = "substitution"
    # Spatial
    PRESSURE = "pressure"
    CARRY = "carry"
    # System
    SOURCE_CONNECTED = "source_connected"
    SOURCE_DISCONNECTED = "source_disconnected"
    REPLAY_COMPLETE = "replay_complete"


class SourceState(str, Enum):
    IDLE = "idle"
    CONNECTED = "connected"
    STREAMING = "streaming"
    DISCONNECTED = "disconnected"


@dataclass
class SourceMetadata:
    source_id: str
    source_type: str
    description: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class DataEvent:
    timestamp_ms: int
    source_id: str
    event_type: EventType
    payload: dict
    sequence_number: int
    match_id: str | None = None
    metadata: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        mins, secs = divmod(self.timestamp_ms // 1000, 60)
        return (
            f"DataEvent(t={mins:02d}:{secs:02d}, "
            f"type={self.event_type.value}, "
            f"src={self.source_id}, "
            f"seq={self.sequence_number})"
        )
