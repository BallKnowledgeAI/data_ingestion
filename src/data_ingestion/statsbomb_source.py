"""
StatsBombSource — concrete DataSource backed by the StatsBomb open dataset.

Downloads a real match from the StatsBomb open-data API and emits the full
spectrum of DataEvent types, including:

  Action events    PASS · SHOT · CARRY · DRIBBLE · CLEARANCE · INTERCEPTION
                   PRESSURE · SUBSTITUTION · HALF_START · HALF_END · MATCH_START
  Positional       BALL_POSITION  — emitted at every event that carries a location
                   PLAYER_POSITION — emitted from:
                       • Shot freeze frames  (all visible players at shot moment)
                       • Event locations     (the active player's on-ball position)
                       • Carry interpolation (ball-carrier tracked through a carry)
                       • Starting XI         (initial squad lineup)

Downstream modules that only care about positions subscribe to
EventType.BALL_POSITION or EventType.PLAYER_POSITION — they never see
the action events and vice-versa.
"""
from __future__ import annotations

import bisect
import threading
import warnings
from typing import Any, Callable

import pandas as pd

from .base_source import BaseDataSource
from .custom_types import DataEvent, EventType, SourceMetadata

# ── StatsBomb → our EventType mapping ────────────────────────────────────── #

_ACTION_MAP: dict[str, EventType] = {
    "Pass":             EventType.PASS,
    "Shot":             EventType.SHOT,
    "Carry":            EventType.CARRY,
    "Pressure":         EventType.PRESSURE,
    "Dribble":          EventType.DRIBBLE,
    "Clearance":        EventType.CLEARANCE,
    "Interception":     EventType.INTERCEPTION,
    "Substitution":     EventType.SUBSTITUTION,
    "Tactical Shift":   EventType.SUBSTITUTION,
    "Half Start":       EventType.HALF_START,
    "Half End":         EventType.HALF_END,
    "Starting XI":      EventType.MATCH_START,
    # Secondary — mapped to nearest canonical type
    "Block":            EventType.CLEARANCE,
    "Ball Recovery":    EventType.CLEARANCE,
    "Goal Keeper":      EventType.CLEARANCE,
    "Duel":             EventType.PRESSURE,
    "Foul Committed":   EventType.PRESSURE,
    "50/50":            EventType.PRESSURE,
    "Miscontrol":       EventType.PASS,
    "Dispossessed":     EventType.INTERCEPTION,
    "Shield":           EventType.CARRY,
}

# These emit positional data only — no action event is published for them
_POSITIONS_ONLY: frozenset[str] = frozenset({
    "Ball Receipt*",
    "Dribbled Past",
    "Foul Won",
    "Offside",
    "Injury Stoppage",
    "Referee Ball-Drop",
})

# Half offsets in milliseconds (canonical 45-minute halves)
_PERIOD_OFFSET_MS: dict[int, int] = {
    1: 0,
    2: 45 * 60_000,
    3: 90 * 60_000,
    4: 105 * 60_000,
    5: 120 * 60_000,
}


def _ts_to_ms(period: int, timestamp: str) -> int:
    """Convert StatsBomb period + 'HH:MM:SS.mmm' timestamp to absolute match ms."""
    h, m, rest = timestamp.split(":")
    s_str, ms_str = rest.split(".")
    period_ms = int(h) * 3_600_000 + int(m) * 60_000 + int(s_str) * 1000 + int(ms_str)
    return _PERIOD_OFFSET_MS.get(period, 0) + period_ms


def _safe(row: pd.Series, col: str, default: Any = None) -> Any:
    val = row.get(col, default)
    if isinstance(val, (list, dict)):
        return val
    try:
        return default if pd.isna(val) else val
    except (TypeError, ValueError):
        return val


def _loc(row: pd.Series, col: str = "location") -> list[float] | None:
    val = row.get(col)
    return val if isinstance(val, list) else None


# ── Payload builders ──────────────────────────────────────────────────────── #

def _pass_payload(row: pd.Series) -> dict:
    loc = _loc(row) or []
    end = _loc(row, "pass_end_location") or []
    return {
        "player_id":   _safe(row, "player_id"),
        "player_name": _safe(row, "player"),
        "team_id":     _safe(row, "team_id"),
        "team_name":   _safe(row, "team"),
        "start_x": loc[0] if loc else None,
        "start_y": loc[1] if loc else None,
        "end_x":   end[0] if end else None,
        "end_y":   end[1] if end else None,
        "pass_length":    _safe(row, "pass_length"),
        "pass_angle":     _safe(row, "pass_angle"),
        "outcome":        _safe(row, "pass_outcome", "complete"),
        "height":         _safe(row, "pass_height"),
        "cross":          bool(_safe(row, "pass_cross")),
        "switch":         bool(_safe(row, "pass_switch")),
        "through_ball":   bool(_safe(row, "pass_through_ball")),
        "key_pass":       bool(_safe(row, "pass_shot_assist") or _safe(row, "pass_goal_assist")),
        "under_pressure": bool(_safe(row, "under_pressure")),
    }


def _shot_payload(row: pd.Series) -> dict:
    loc = _loc(row) or []
    end = _loc(row, "shot_end_location") or []
    outcome = _safe(row, "shot_outcome", "")
    return {
        "player_id":    _safe(row, "player_id"),
        "player_name":  _safe(row, "player"),
        "team_id":      _safe(row, "team_id"),
        "team_name":    _safe(row, "team"),
        "x": loc[0] if loc else None,
        "y": loc[1] if loc else None,
        "end_x": end[0] if end else None,
        "end_y": end[1] if end else None,
        "end_z": end[2] if len(end) > 2 else None,
        "xg":             _safe(row, "shot_statsbomb_xg", 0.0),
        "shot_technique": _safe(row, "shot_technique"),
        "body_part":      _safe(row, "shot_body_part"),
        "shot_outcome":   outcome,
        "is_goal":        str(outcome).lower() == "goal",
        "first_time":     bool(_safe(row, "shot_first_time")),
        "aerial":         bool(_safe(row, "shot_aerial_won")),
        "under_pressure": bool(_safe(row, "under_pressure")),
    }


def _carry_payload(row: pd.Series) -> dict:
    loc = _loc(row) or []
    end = _loc(row, "carry_end_location") or []
    return {
        "player_id":   _safe(row, "player_id"),
        "player_name": _safe(row, "player"),
        "team_id":     _safe(row, "team_id"),
        "team_name":   _safe(row, "team"),
        "start_x": loc[0] if loc else None,
        "start_y": loc[1] if loc else None,
        "end_x":   end[0] if end else None,
        "end_y":   end[1] if end else None,
        "duration":       _safe(row, "duration", 0.0),
        "under_pressure": bool(_safe(row, "under_pressure")),
    }


def _generic_payload(row: pd.Series) -> dict:
    loc = _loc(row) or []
    return {
        "player_id":   _safe(row, "player_id"),
        "player_name": _safe(row, "player"),
        "team_id":     _safe(row, "team_id"),
        "team_name":   _safe(row, "team"),
        "x": loc[0] if loc else None,
        "y": loc[1] if loc else None,
        "under_pressure": bool(_safe(row, "under_pressure")),
    }


def _sub_payload(row: pd.Series) -> dict:
    return {
        "player_off_id":   _safe(row, "player_id"),
        "player_off_name": _safe(row, "player"),
        "player_on_name":  _safe(row, "substitution_replacement"),
        "team_id":         _safe(row, "team_id"),
        "team_name":       _safe(row, "team"),
        "minute":          _safe(row, "minute"),
    }


_PAYLOAD_BUILDERS: dict[str, Callable[[pd.Series], dict]] = {
    "Pass":         _pass_payload,
    "Shot":         _shot_payload,
    "Carry":        _carry_payload,
    "Substitution": _sub_payload,
    "Tactical Shift": _sub_payload,
}


# ── Source class ─────────────────────────────────────────────────────────── #

class StatsBombSource(BaseDataSource):
    """
    Streams a real StatsBomb open-data match through the DataEvent pipeline.

    Parameters
    ----------
    source_id:
        Unique identifier for this source.
    competition_id:
        StatsBomb competition ID (e.g. 16 = Champions League).
    season_id:
        StatsBomb season ID (e.g. 4 = 2018/19).
    match_id:
        StatsBomb match ID.
    speed_factor:
        Replay speed multiplier (1.0 = real time, 100.0 = 100× faster).
    carry_tracking_hz:
        How many intermediate PLAYER_POSITION + BALL_POSITION events to emit
        per second of carry duration. Set to 0 to disable carry interpolation.
        Default 5 gives ~3-6 positional snapshots per typical carry.
    """

    def __init__(
        self,
        source_id: str,
        competition_id: int,
        season_id: int,
        match_id: int,
        speed_factor: float = 1.0,
        carry_tracking_hz: float = 5.0,
    ) -> None:
        super().__init__(source_id)
        self._competition_id = competition_id
        self._season_id = season_id
        self._match_id = match_id
        self._speed_factor = speed_factor
        self._carry_tracking_hz = carry_tracking_hz
        self._cursor = 0

        # Load and convert on construction so metadata is available immediately
        print(f"  Loading StatsBomb match {match_id} (comp={competition_id}, season={season_id})...")
        self._pending: list[dict] = self._load_and_convert()
        self._timestamps: list[int] = [e["timestamp_ms"] for e in self._pending]
        print(f"  Loaded {len(self._pending)} events ready for emission.")

        # Threading (same pattern as AnnotatedReplaySource)
        self._stop_event  = threading.Event()
        self._wake_event  = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ #
    # Runtime controls                                                     #
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
        return self._timestamps[min(self._cursor - 1, len(self._timestamps) - 1)]

    @property
    def speed_factor(self) -> float:
        return self._speed_factor

    @speed_factor.setter
    def speed_factor(self, value: float) -> None:
        self._speed_factor = value
        self._wake_event.set()

    def pause(self) -> None:
        self._pause_event.clear()

    def resume(self) -> None:
        self._pause_event.set()

    def seek(self, target_ms: int) -> None:
        """Jump to the nearest event at or after target_ms.  O(log n)."""
        idx = bisect.bisect_left(self._timestamps, target_ms)
        self._cursor = min(idx, len(self._timestamps) - 1)
        self._wake_event.set()

    def wait_until_done(self, timeout: float | None = None) -> bool:
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
            source_type="statsbomb",
            description=(
                f"StatsBomb match {self._match_id} "
                f"(comp={self._competition_id}, season={self._season_id}) "
                f"— {len(self._pending)} events"
            ),
            extra={
                "match_id":       self._match_id,
                "competition_id": self._competition_id,
                "season_id":      self._season_id,
            },
        )

    def _start_streaming(self) -> None:
        self._stop_event.clear()
        self._wake_event.clear()
        self._thread = threading.Thread(
            target=self._replay_loop,
            name=f"statsbomb-{self._source_id}",
            daemon=True,
        )
        self._thread.start()

    def _stop_streaming(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        self._pause_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)

    # ------------------------------------------------------------------ #
    # Data loading and conversion                                          #
    # ------------------------------------------------------------------ #

    def _load_and_convert(self) -> list[dict]:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import statsbombpy.sb as sb
            df = sb.events(match_id=self._match_id)

        df = df.sort_values(["period", "index"]).reset_index(drop=True)
        pending: list[dict] = []

        for _, row in df.iterrows():
            ts  = _ts_to_ms(int(row["period"]), str(row["timestamp"]))
            sid = str(self._match_id)
            sub = [0]  # sub-index within same timestamp

            def add(etype: EventType, payload: dict, at: int | None = None, meta: dict | None = None) -> None:
                pending.append({
                    "timestamp_ms": at if at is not None else ts,
                    "sub_index":    sub[0],
                    "event_type":   etype,
                    "payload":      payload,
                    "match_id":     sid,
                    "metadata":     meta or {},
                })
                sub[0] += 1

            sb_type: str = str(row["type"])
            loc = _loc(row)

            # ── 1. Ball position (every event with a spatial location) ── #
            if loc:
                add(EventType.BALL_POSITION, {"x": loc[0], "y": loc[1]})

            # ── 2. Active player position (from event location) ── #
            if loc and _safe(row, "player_id"):
                add(EventType.PLAYER_POSITION, {
                    "player_id":   _safe(row, "player_id"),
                    "player_name": _safe(row, "player"),
                    "team_id":     _safe(row, "team_id"),
                    "team_name":   _safe(row, "team"),
                    "x":           loc[0],
                    "y":           loc[1],
                    "source":      "event_location",
                })

            # ── 3. Action event ── #
            if sb_type not in _POSITIONS_ONLY:
                etype = _ACTION_MAP.get(sb_type)
                if etype is not None:
                    builder = _PAYLOAD_BUILDERS.get(sb_type, _generic_payload)
                    payload = builder(row)
                    meta: dict = {}

                    # Starting XI: attach lineup to metadata + emit player positions
                    if sb_type == "Starting XI":
                        tactics = row.get("tactics") or {}
                        meta["formation"] = tactics.get("formation")
                        for p in tactics.get("lineup", []):
                            add(EventType.PLAYER_POSITION, {
                                "player_id":     p["player"]["id"],
                                "player_name":   p["player"]["name"],
                                "team_id":       _safe(row, "team_id"),
                                "team_name":     _safe(row, "team"),
                                "position_name": p["position"]["name"],
                                "jersey_number": p["jersey_number"],
                                "x": None,
                                "y": None,
                                "source": "lineup",
                            })

                    add(etype, payload, meta=meta)

            # ── 4. Freeze-frame positions (at shot moments) ── #
            if sb_type == "Shot":
                ff = row.get("shot_freeze_frame")
                if isinstance(ff, list):
                    for fp in ff:
                        if not isinstance(fp.get("location"), list):
                            continue
                        add(EventType.PLAYER_POSITION, {
                            "player_id":     fp["player"]["id"],
                            "player_name":   fp["player"]["name"],
                            "team_id":       None,
                            "team_name":     None,
                            "x":             fp["location"][0],
                            "y":             fp["location"][1],
                            "position_name": fp["position"]["name"],
                            "teammate":      fp["teammate"],
                            "source":        "freeze_frame",
                        })
                # Also emit end-location of shot as ball position
                end = _loc(row, "shot_end_location")
                if end:
                    duration_ms = int((_safe(row, "duration") or 0.5) * 1000)
                    add(EventType.BALL_POSITION, {
                        "x": end[0],
                        "y": end[1],
                        "z": end[2] if len(end) > 2 else None,
                    }, at=ts + duration_ms)

            # ── 5. Carry interpolation ── #
            if sb_type == "Carry" and self._carry_tracking_hz > 0:
                start_loc = _loc(row) or []
                end_loc   = _loc(row, "carry_end_location") or []
                duration  = _safe(row, "duration") or 0.0
                if start_loc and end_loc and duration > 0:
                    steps = max(1, int(duration * self._carry_tracking_hz))
                    for i in range(1, steps + 1):
                        frac = i / steps
                        xi = start_loc[0] + (end_loc[0] - start_loc[0]) * frac
                        yi = start_loc[1] + (end_loc[1] - start_loc[1]) * frac
                        t_interp = ts + int(duration * 1000 * frac)
                        add(EventType.BALL_POSITION,   {"x": xi, "y": yi},
                            at=t_interp)
                        add(EventType.PLAYER_POSITION, {
                            "player_id":   _safe(row, "player_id"),
                            "player_name": _safe(row, "player"),
                            "team_id":     _safe(row, "team_id"),
                            "team_name":   _safe(row, "team"),
                            "x": xi,
                            "y": yi,
                            "source": "carry_interpolated",
                        }, at=t_interp)

        pending.sort(key=lambda e: (e["timestamp_ms"], e["sub_index"]))
        return pending

    # ------------------------------------------------------------------ #
    # Replay loop                                                          #
    # ------------------------------------------------------------------ #

    def _replay_loop(self) -> None:
        while not self._stop_event.is_set():
            self._pause_event.wait()
            if self._stop_event.is_set():
                break

            if self._cursor >= len(self._pending):
                total = self._timestamps[-1] if self._timestamps else 0
                self._emit(self._make_event(EventType.REPLAY_COMPLETE, {}, timestamp_ms=total))
                break

            rec = self._pending[self._cursor]
            event = self._make_event(
                event_type=rec["event_type"],
                payload=rec["payload"],
                timestamp_ms=rec["timestamp_ms"],
                match_id=rec["match_id"],
                metadata=rec["metadata"],
            )
            self._emit(event)
            self._cursor += 1

            if self._cursor < len(self._pending):
                next_ts  = self._pending[self._cursor]["timestamp_ms"]
                delta_ms = next_ts - rec["timestamp_ms"]
                if delta_ms > 0:
                    delay = delta_ms / 1000.0 / max(self._speed_factor, 1e-6)
                    self._wake_event.wait(timeout=delay)
                    self._wake_event.clear()
