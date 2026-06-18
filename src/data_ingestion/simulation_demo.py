"""
Module 1 -- Data Ingestion: end-to-end demo.

Uses a real StatsBomb open-data match:
  UEFA Champions League 2018/19 Final
  Tottenham Hotspur 0 - 2 Liverpool FC  (match_id=22912)

Demonstrates:
  * StatsBombSource wiring and download
  * Wildcard subscription (all events)
  * Typed subscription (SHOT only -- xG tracker)
  * Typed subscription (PLAYER_POSITION + BALL_POSITION -- positional tracker)
  * pause / resume
  * seek (jump to second half)
  * Live event breakdown + xG summary on completion

Run:
    python -m src.data_ingestion.simulation_demo
"""
from __future__ import annotations

import sys
import threading
import time
from collections import defaultdict

from .custom_types import DataEvent, EventType
from .registry import DataSourceRegistry
from .statsbomb_source import StatsBombSource

# ── ANSI helpers ─────────────────────────────────────────────────────────── #

_TTY = sys.stdout.isatty()

def _c(code: str, t: str) -> str:
    return f"\033[{code}m{t}\033[0m" if _TTY else t

def _bold(t: str)    -> str: return _c("1",  t)
def _red(t: str)     -> str: return _c("31", t)
def _green(t: str)   -> str: return _c("32", t)
def _yellow(t: str)  -> str: return _c("33", t)
def _cyan(t: str)    -> str: return _c("36", t)
def _magenta(t: str) -> str: return _c("35", t)
def _dim(t: str)     -> str: return _c("2",  t)

_TYPE_COLOUR = {
    EventType.SHOT:                _red,
    EventType.MATCH_START:         _bold,
    EventType.HALF_START:          _bold,
    EventType.HALF_END:            _bold,
    EventType.SUBSTITUTION:        _yellow,
    EventType.REPLAY_COMPLETE:     _magenta,
    EventType.SOURCE_CONNECTED:    _cyan,
    EventType.SOURCE_DISCONNECTED: _cyan,
    EventType.PLAYER_POSITION:     _dim,
    EventType.BALL_POSITION:       _dim,
}

def _fmt_type(et: EventType) -> str:
    return _TYPE_COLOUR.get(et, lambda x: x)(f"{et.value:<22}")

def _fmt_ms(ms: int) -> str:
    mins, rem = divmod(ms // 1000, 60)
    return f"{mins:02d}:{rem:02d}"

def _banner(title: str) -> None:
    w = 54
    print(_bold(f"\n+{'-' * w}+"))
    print(_bold(f"| {title:<{w-1}}|"))
    print(_bold(f"+{'-' * w}+"))


# ── Demo ─────────────────────────────────────────────────────────────────── #

def main() -> None:
    _banner("BallKnowledge -- Module 1: Data Ingestion Demo")
    print(f"  Match   : UCL Final 2018/19  --  Tottenham 0-2 Liverpool")
    print(f"  Source  : StatsBomb open data  (match_id=22912)\n")

    # ── Shared state (written from handler threads, read after join) ── #
    counts:    dict[str, int]      = defaultdict(int)
    positions: dict[str, int]      = defaultdict(int)  # player_name -> position count
    shot_log:  list[DataEvent]     = []
    xg_total                       = 0.0
    lock                           = threading.Lock()

    # ── Handler 1: action events (non-positional) ── #
    def on_action(event: DataEvent) -> None:
        if event.event_type in (EventType.BALL_POSITION, EventType.PLAYER_POSITION):
            return
        t     = _fmt_ms(event.timestamp_ms)
        etype = _fmt_type(event.event_type)
        p     = event.payload
        actor = p.get("player_name") or p.get("player_off_name") or ""
        team  = p.get("team_name", "")
        extra = f"  {_cyan(str(actor))}" if actor else ""
        if team:
            extra += f" [{team}]"
        # Annotate goals
        if p.get("is_goal"):
            extra += _red("  *** GOAL ***")
        print(f"  [{t}] {etype} seq={event.sequence_number:<5}{extra}")
        with lock:
            counts[event.event_type.value] += 1

    # ── Handler 2: xG tracker (shots only) ── #
    def on_shot(event: DataEvent) -> None:
        nonlocal xg_total
        p   = event.payload
        xg  = p.get("xg") or 0.0
        out = p.get("shot_outcome", "?")
        goal_marker = _red("  GOAL") if p.get("is_goal") else ""
        print(
            f"  {_red('>>> xG')}  {p.get('player_name','?'):<30}"
            f"xG={xg:.3f}  outcome={out}{goal_marker}"
        )
        with lock:
            xg_total += xg
            shot_log.append(event)

    # ── Handler 3: positional tracker ── #
    pos_counter = [0]
    def on_position(event: DataEvent) -> None:
        with lock:
            counts[event.event_type.value] += 1
            name = event.payload.get("player_name")
            if name:
                positions[str(name)] += 1
        pos_counter[0] += 1
        # Print a summary line every 500 positional events so the demo
        # stays readable without being drowned by tracking data
        if pos_counter[0] % 500 == 0:
            print(
                _dim(
                    f"  [pos]  {pos_counter[0]:>5} positional events emitted  "
                    f"(match time: {_fmt_ms(event.timestamp_ms)})"
                )
            )

    # Use a threading.Event driven by REPLAY_COMPLETE so we never rely on an
    # arbitrary timeout — we wait exactly as long as the replay needs.
    done_event = threading.Event()

    # ── Wire up ── #
    source = StatsBombSource(
        source_id       = "ucl_final_2019",
        competition_id  = 16,
        season_id       = 4,
        match_id        = 22912,
        speed_factor    = 1.0,        # 90 min in ~11 wall-clock seconds
        carry_tracking_hz = 5.0,
    )
    print(f"  Events  : {len(source._pending):,} (actions + positional)")
    print(f"  Speed   : {source.speed_factor}x\n")

    registry = DataSourceRegistry()
    registry.register(source)

    # Four independent subscriptions showing different downstream patterns
    registry.subscribe_all(None,                      on_action)    # tactical analyser
    registry.subscribe_all(EventType.SHOT,            on_shot)      # xG model
    registry.subscribe_all(EventType.BALL_POSITION,   on_position)  # positional tracker
    registry.subscribe_all(EventType.PLAYER_POSITION, on_position)

    def _on_complete(event: DataEvent) -> None:
        done_event.set()
    registry.subscribe_all(EventType.REPLAY_COMPLETE, _on_complete)

    # ── Start ── #
    registry.connect_all()
    print()

    # ── Demo: pause mid-first-half ── #
    time.sleep(1.5)
    print(_yellow("\n  -- PAUSE --------------------------------------------------"))
    source.pause()
    time.sleep(1.0)
    print(_yellow(
        f"  Paused at match time {_fmt_ms(source.current_match_ms)}  "
        f"({source.progress:.1%} through)"
    ))
    print(_yellow("  -- RESUME -------------------------------------------------\n"))
    source.resume()

    # ── Demo: seek to second half (45:00) ── #
    time.sleep(0.5)
    print(_cyan("\n  -- SEEK -> 45:00 (second half) ----------------------------\n"))
    source.seek(45 * 60_000)

    # ── Wait for REPLAY_COMPLETE signal — no timeout needed ── #
    done_event.wait()

    # ── Event breakdown ── #
    _banner("EVENT BREAKDOWN")
    action_counts = {k: v for k, v in counts.items()
                     if k not in ("ball_position", "player_position")}
    pos_counts    = {k: v for k, v in counts.items()
                     if k in ("ball_position", "player_position")}

    print(_bold("  Action events:"))
    max_c = max(action_counts.values(), default=1)
    for etype, c in sorted(action_counts.items(), key=lambda x: -x[1]):
        bar = "#" * int(c / max_c * 28)
        print(f"    {etype:<25}  {c:>5}  {_green(bar)}")

    print(_bold("\n  Positional events:"))
    for etype, c in sorted(pos_counts.items(), key=lambda x: -x[1]):
        print(f"    {etype:<25}  {c:>5}")

    total_action = sum(action_counts.values())
    total_pos    = sum(pos_counts.values())
    print(f"\n    {'Total action':<25}  {total_action:>5}")
    print(f"    {'Total positional':<25}  {total_pos:>5}")
    print(f"    {'Grand total':<25}  {total_action + total_pos:>5}")

    # ── xG summary ── #
    if shot_log:
        _banner("xG SUMMARY")
        running = 0.0
        for ev in shot_log:
            p    = ev.payload
            xg   = p.get("xg") or 0.0
            running += xg
            goal = _red(" [GOAL]") if p.get("is_goal") else ""
            print(
                f"  {_fmt_ms(ev.timestamp_ms)}  "
                f"{str(p.get('player_name','?')):<28} "
                f"{str(p.get('team_name','?')):<22} "
                f"xG={xg:.3f}  {str(p.get('shot_outcome','?'))}{goal}"
            )
        print(f"\n  Running xG: {_red(f'{xg_total:.3f}')}")

    # ── Top-5 most tracked players ── #
    if positions:
        _banner("MOST TRACKED PLAYERS (by position events)")
        top5 = sorted(positions.items(), key=lambda x: -x[1])[:5]
        for name, c in top5:
            bar = "#" * int(c / top5[0][1] * 30)
            print(f"  {str(name):<30}  {c:>5}  {_cyan(bar)}")

    print()
    registry.disconnect_all()
    print(_bold("  [done]\n"))


if __name__ == "__main__":
    main()
