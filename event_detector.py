from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, asdict, replace
from typing import Any

from config import (
    EVENT_LEAD_SWING_30S,
    EVENT_LEAD_SWING_60S,
    EVENT_COOLDOWN_GAME_SECONDS,
)
from event_taxonomy import event_family, event_is_primary, event_tier

WINDOW_TOLERANCE_SEC = {30: 12, 60: 20}


COMEBACK_MIN_PRIOR_DEFICIT = 3000
MAJOR_COMEBACK_PRIOR_DEFICIT = 8000
KILL_CONFIRMED_LEAD_SWING_GOLD_30S = 2500
KILL_CONFIRMED_LEAD_SWING_KILLS_30S = 2
KILL_BURST_30S = 3
KILL_BURST_MIN_NW_CONFIRMATION = 500
STOMP_THROW_MIN_LEAD = 12_000
STOMP_THROW_MIN_NW_SWING = 2_500
STOMP_THROW_MIN_KILLS = 3
STOMP_THROW_MIN_TIME = 30 * 60
EVENT_DEDUPE_SECONDS = 120
LATE_LEAD_SWING_DEMOTE_TIME = 50 * 60
ULTRA_LATE_LEAD_SWING_DEMOTE_TIME = 60 * 60
LATE_MAJOR_COMEBACK_TIME = 40 * 60
CHAINED_LATE_FIGHT_TIME = 45 * 60
CHAINED_RECOVERY_WINDOW_SEC = 90
LATE_ECONOMIC_CRASH_TIME = 50 * 60

# Composite conversion events: an objective falling in the same observed update as
# a same-direction kill/networth swing is higher quality than a tower-only signal.
# These are emitted in addition to the component events; the signal engine suppresses
# the duplicated tower component before scoring.
CONVERSION_SUPPORT_EVENTS = frozenset({
    "COMEBACK",
    "MAJOR_COMEBACK",
    "LEAD_SWING_60S",
    "LEAD_SWING_30S",
    "EXTREME_LEAD_SWING_30S",
    "KILL_CONFIRMED_LEAD_SWING",
    "KILL_BURST_30S",
    "LATE_GAME_WIPE",
    "ULTRA_LATE_WIPE",
    "STOMP_THROW",
    "LATE_MAJOR_COMEBACK_REPRICE",
    "CHAINED_LATE_FIGHT_RECOVERY",
    "LATE_ECONOMIC_CRASH",
    "ULTRA_LATE_WIPE_CONFIRMED",
    "STOMP_THROW_WITH_OBJECTIVE_RISK",
    "FIGHT_TO_GOLD_CONFIRM_30S",
})
CONVERSION_TOWER_EVENTS = frozenset({
    "T2_TOWER_FALL",
    "T3_TOWER_FALL",
    "MULTIPLE_T3_TOWERS_DOWN",
    "ALL_T3_TOWERS_DOWN",
    "FIRST_T4_TOWER_FALL",
    "SECOND_T4_TOWER_FALL",
    "THRONE_EXPOSED",
    "T3_PLUS_T4_CHAIN",
    "MULTI_STRUCTURE_COLLAPSE",
})

_EVENT_BASE_PRESSURE: dict[str, float] = {
    "THRONE_EXPOSED": 1.00,
    "SECOND_T4_TOWER_FALL": 0.85,
    "T3_PLUS_T4_CHAIN": 0.80,
    "MULTI_STRUCTURE_COLLAPSE": 0.70,
    "ALL_T3_TOWERS_DOWN": 0.65,
    "FIRST_T4_TOWER_FALL": 0.55,
    "OBJECTIVE_CONVERSION_T4": 0.90,
    "OBJECTIVE_CONVERSION_T3": 0.70,
    "OBJECTIVE_CONVERSION_T2": 0.45,
    "ULTRA_LATE_WIPE": 0.60,
    "LATE_GAME_WIPE": 0.50,
    "STOMP_THROW": 0.55,
    "MULTIPLE_T3_TOWERS_DOWN": 0.50,
    "T3_TOWER_FALL": 0.35,
    "MAJOR_COMEBACK": 0.55,
    "LATE_MAJOR_COMEBACK_REPRICE": 0.62,
    "CHAINED_LATE_FIGHT_RECOVERY": 0.58,
    "LATE_ECONOMIC_CRASH": 0.55,
    "ULTRA_LATE_WIPE_CONFIRMED": 0.70,
    "STOMP_THROW_WITH_OBJECTIVE_RISK": 0.62,
    "FIGHT_TO_GOLD_CONFIRM_30S": 0.35,
    "COMEBACK": 0.35,
    "EXTREME_LEAD_SWING_30S": 0.50,
    "KILL_CONFIRMED_LEAD_SWING": 0.40,
    "LEAD_SWING_60S": 0.25,
    "LEAD_SWING_30S": 0.20,
    "KILL_BURST_30S": 0.15,
    "T2_TOWER_FALL": 0.20,
    "MULTIPLE_T2_TOWERS_DOWN": 0.30,
    "ALL_T2_TOWERS_DOWN": 0.35,
}

_EVENT_CONFIDENCE: dict[str, float] = {
    "THRONE_EXPOSED": 1.0,
    "T3_PLUS_T4_CHAIN": 0.85,
    "MULTI_STRUCTURE_COLLAPSE": 0.75,
    "ALL_T3_TOWERS_DOWN": 0.80,
    "OBJECTIVE_CONVERSION_T4": 0.90,
    "OBJECTIVE_CONVERSION_T3": 0.85,
    "OBJECTIVE_CONVERSION_T2": 0.75,
    "SECOND_T4_TOWER_FALL": 0.85,
    "FIRST_T4_TOWER_FALL": 0.70,
    "ULTRA_LATE_WIPE": 0.80,
    "LATE_GAME_WIPE": 0.70,
    "STOMP_THROW": 0.75,
    "MULTIPLE_T3_TOWERS_DOWN": 0.65,
    "T3_TOWER_FALL": 0.50,
    "MAJOR_COMEBACK": 0.80,
    "LATE_MAJOR_COMEBACK_REPRICE": 0.82,
    "CHAINED_LATE_FIGHT_RECOVERY": 0.78,
    "LATE_ECONOMIC_CRASH": 0.72,
    "ULTRA_LATE_WIPE_CONFIRMED": 0.86,
    "STOMP_THROW_WITH_OBJECTIVE_RISK": 0.80,
    "FIGHT_TO_GOLD_CONFIRM_30S": 0.62,
    "COMEBACK": 0.55,
    "EXTREME_LEAD_SWING_30S": 0.70,
    "KILL_CONFIRMED_LEAD_SWING": 0.65,
    "LEAD_SWING_60S": 0.50,
    "LEAD_SWING_30S": 0.45,
    "KILL_BURST_30S": 0.40,
    "T2_TOWER_FALL": 0.40,
    "MULTIPLE_T2_TOWERS_DOWN": 0.50,
    "ALL_T2_TOWERS_DOWN": 0.55,
}

# 11-bit GetTopLiveGame/tower_state side layout:
# top T1/T2/T3, mid T1/T2/T3, bot T1/T2/T3, two T4s.
T1_MASK = (1 << 0) | (1 << 3) | (1 << 6)
T2_MASK = (1 << 1) | (1 << 4) | (1 << 7)
T3_MASK = (1 << 2) | (1 << 5) | (1 << 8)
T4_MASK = (1 << 9) | (1 << 10)
SIDE_MASK = 0x7FF


@dataclass(frozen=True)
class DotaEvent:
    match_id: str
    lobby_id: str | None
    league_id: str | None
    event_type: str
    game_time_sec: int | None
    radiant_team: str | None
    dire_team: str | None
    radiant_lead: int | None
    radiant_score: int | None
    dire_score: int | None
    tower_state: int | None
    previous_value: str | int | float | None
    current_value: str | int | float | None
    delta: int | float | None
    window_sec: int | None
    direction: str | None
    severity: str
    mapping_name: str | None = None
    yes_team: str | None = None
    yes_token_id: str | None = None
    threshold: int | float | None = None
    base_pressure_score: float | None = None
    fight_pressure_score: float | None = None
    economic_pressure_score: float | None = None
    conversion_score: float | None = None
    event_confidence: float | None = None
    event_dedupe_key: str | None = None
    event_is_primary: bool | None = None
    event_tier: str | None = None
    event_family: str | None = None
    event_quality: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EventDetector:
    """Detect fast GetTopLiveGame-compatible Dota events.

    No barracks, Roshan, buyback, or hero-identity assumptions are made here.
    Ancient/game_over is handled outside ACTIVE_EVENTS as a terminal condition.
    """

    def __init__(self, max_history: int = 720):
        self.history: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=max_history))
        self.last_emitted_game_time: dict[tuple[str, str, str | None], int] = {}
        self.last_emitted_dedupe_game_time: dict[str, int] = {}
        self.late_recovery_history: dict[tuple[str, str], deque[tuple[int, int, str]]] = defaultdict(lambda: deque(maxlen=12))

    def observe(self, game: dict, mapping: dict | None = None) -> list[DotaEvent]:
        match_id = str(game.get("match_id") or game.get("lobby_id") or "")
        if not match_id:
            return []

        snapshot = self._snapshot(game)
        hist = self.history[match_id]

        cur_gt = snapshot.get("game_time_sec")
        if hist and cur_gt is not None:
            last_gt = hist[-1].get("game_time_sec")
            if last_gt is not None and cur_gt - last_gt > 60:
                hist.clear()

        previous = hist[-1] if hist else None
        events: list[DotaEvent] = []
        if previous:
            events.extend(self._tower_events(previous, snapshot, mapping))
            events.extend(self._comeback_events(previous, snapshot, mapping))

        events.extend(self._lead_swing_events(hist, snapshot, mapping))
        events.extend(self._score_confirmed_events(hist, snapshot, mapping))
        events.extend(self._strategic_composite_events(events, snapshot, mapping))
        events.extend(self._objective_conversion_events(events, snapshot, mapping))

        events = self._enrich_pressure(events, previous, snapshot)
        events = self._add_event_metadata(events)
        events = self._dedupe_events(events)

        hist.append(snapshot)
        return events

    def _snapshot(self, game: dict) -> dict:
        return {
            "match_id": str(game.get("match_id") or game.get("lobby_id") or ""),
            "lobby_id": game.get("lobby_id"),
            "league_id": game.get("league_id"),
            "game_time_sec": _to_int(game.get("game_time_sec")),
            "radiant_team": game.get("radiant_team"),
            "dire_team": game.get("dire_team"),
            "radiant_lead": _to_int(game.get("radiant_lead")),
            "radiant_score": _to_int(game.get("radiant_score")),
            "dire_score": _to_int(game.get("dire_score")),
            "tower_state": _to_int(game.get("tower_state")),
            "building_state": _to_int(game.get("building_state")),
        }

    def _base_event(self, snap: dict, mapping: dict | None, **kwargs) -> DotaEvent:
        return DotaEvent(
            match_id=snap["match_id"],
            lobby_id=snap.get("lobby_id"),
            league_id=snap.get("league_id"),
            game_time_sec=snap.get("game_time_sec"),
            radiant_team=snap.get("radiant_team"),
            dire_team=snap.get("dire_team"),
            radiant_lead=snap.get("radiant_lead"),
            radiant_score=snap.get("radiant_score"),
            dire_score=snap.get("dire_score"),
            tower_state=snap.get("tower_state"),
            mapping_name=(mapping or {}).get("name"),
            yes_team=(mapping or {}).get("yes_team"),
            yes_token_id=(mapping or {}).get("yes_token_id"),
            **kwargs,
        )

    # ------------------------------------------------------------------ #
    # Pressure metadata enrichment                                       #
    # ------------------------------------------------------------------ #

    def _enrich_pressure(
        self,
        events: list[DotaEvent],
        previous: dict | None,
        cur: dict,
    ) -> list[DotaEvent]:
        if not events:
            return events

        fight_delta: int | None = None
        nw_delta: int | None = None
        if previous is not None:
            prev_lead = previous.get("radiant_lead")
            cur_lead = cur.get("radiant_lead")
            prev_rs = previous.get("radiant_score")
            prev_ds = previous.get("dire_score")
            cur_rs = cur.get("radiant_score")
            cur_ds = cur.get("dire_score")
            if prev_lead is not None and cur_lead is not None:
                nw_delta = cur_lead - prev_lead
            if prev_rs is not None and prev_ds is not None and cur_rs is not None and cur_ds is not None:
                fight_delta = (cur_rs - prev_rs) - (cur_ds - prev_ds)

        enriched: list[DotaEvent] = []
        has_obj_conversion = any(e.event_type.startswith("OBJECTIVE_CONVERSION_") for e in events)

        for evt in events:
            bp = _EVENT_BASE_PRESSURE.get(evt.event_type, 0.3)
            conf = _EVENT_CONFIDENCE.get(evt.event_type, 0.5)

            fp: float | None = None
            ep: float | None = None
            cs: float | None = None

            if fight_delta is not None and evt.direction is not None:
                signed = fight_delta if evt.direction == "radiant" else -fight_delta
                fp = min(max(signed / 5.0, 0.0), 1.0)
                if signed <= 0:
                    fp = 0.0

            if nw_delta is not None and evt.direction is not None:
                signed = nw_delta if evt.direction == "radiant" else -nw_delta
                ep = min(max(signed / 5000.0, 0.0), 1.0)
                if signed <= 0:
                    ep = 0.0

            if fp is not None and ep is not None:
                if fp > 0 and ep > 0:
                    cs = min(math.sqrt(fp * ep), 1.0)
                else:
                    cs = max(fp, ep) * 0.3
            elif fp is not None:
                cs = fp * 0.4 if fp > 0 else None
            elif ep is not None:
                cs = ep * 0.4 if ep > 0 else None

            if has_obj_conversion and evt.event_type.startswith("OBJECTIVE_CONVERSION_"):
                conf = min(conf + 0.12, 1.0)
            if fp is not None and fp > 0.0:
                conf = min(conf + 0.08, 1.0)
            if ep is not None and ep > 0.0:
                conf = min(conf + 0.08, 1.0)

            enriched.append(replace(
                evt,
                base_pressure_score=round(bp, 3),
                fight_pressure_score=round(fp, 3) if fp is not None else None,
                economic_pressure_score=round(ep, 3) if ep is not None else None,
                conversion_score=round(cs, 3) if cs is not None else None,
                event_confidence=round(min(conf, 1.0), 3),
            ))

        return enriched

    def _add_event_metadata(self, events: list[DotaEvent]) -> list[DotaEvent]:
        out = []
        for event in events:
            key = _event_dedupe_key(event)
            quality = _event_quality(event)
            out.append(replace(
                event,
                event_dedupe_key=key,
                event_is_primary=event_is_primary(event.event_type),
                event_tier=event_tier(event.event_type),
                event_family=event_family(event.event_type),
                event_quality=round(quality, 3),
            ))
        return out

    def _dedupe_events(self, events: list[DotaEvent]) -> list[DotaEvent]:
        out = []
        for event in events:
            key = event.event_dedupe_key or _event_dedupe_key(event)
            game_time = event.game_time_sec
            if game_time is not None:
                last = self.last_emitted_dedupe_game_time.get(key)
                if last is not None and game_time - last < EVENT_DEDUPE_SECONDS:
                    continue
                self.last_emitted_dedupe_game_time[key] = game_time
            out.append(event)
        return out

    # ------------------------------------------------------------------ #
    # Strategic composite events                                          #
    # ------------------------------------------------------------------ #

    def _strategic_composite_events(
        self,
        events: list[DotaEvent],
        cur: dict,
        mapping: dict | None,
    ) -> list[DotaEvent]:
        """Promote same-update low-level events into final strategy events.

        These composites remain feed-local: no book price, Roshan, buyback, or
        player-identity assumptions are made here.
        """
        if not events:
            return []

        cur_time = cur.get("game_time_sec")
        if cur_time is None:
            return []

        out: list[DotaEvent] = []
        by_dir: dict[str, list[DotaEvent]] = defaultdict(list)
        for event in events:
            if event.direction:
                by_dir[event.direction].append(event)

        for direction, group in by_dir.items():
            types = {event.event_type for event in group}
            strongest_delta = max((abs(float(e.delta)) for e in group if isinstance(e.delta, (int, float))), default=0.0)

            if (
                cur_time >= LATE_MAJOR_COMEBACK_TIME
                and {"MAJOR_COMEBACK", "LEAD_SWING_60S"} <= types
                and self._cooldown_ok(cur, "LATE_MAJOR_COMEBACK_REPRICE", direction)
            ):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="LATE_MAJOR_COMEBACK_REPRICE",
                    previous_value="MAJOR_COMEBACK+LEAD_SWING_60S",
                    current_value="late_comeback_reprice",
                    delta=strongest_delta, window_sec=60, direction=direction,
                    severity="high",
                ))

            if (
                cur_time >= LATE_ECONOMIC_CRASH_TIME
                and any(e.event_type == "LEAD_SWING_60S" and isinstance(e.delta, (int, float)) and abs(e.delta) >= 10_000 for e in group)
                and (
                    types & {"KILL_CONFIRMED_LEAD_SWING", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE"}
                    or types & CONVERSION_TOWER_EVENTS
                )
                and self._cooldown_ok(cur, "LATE_ECONOMIC_CRASH", direction)
            ):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="LATE_ECONOMIC_CRASH",
                    previous_value="late_lead_swing+confirmation",
                    current_value="late_economic_crash",
                    delta=strongest_delta, window_sec=60, direction=direction,
                    severity="high",
                ))

            if (
                "ULTRA_LATE_WIPE" in types
                and types & CONVERSION_TOWER_EVENTS
                and self._cooldown_ok(cur, "ULTRA_LATE_WIPE_CONFIRMED", direction)
            ):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="ULTRA_LATE_WIPE_CONFIRMED",
                    previous_value="ULTRA_LATE_WIPE+base_pressure",
                    current_value="ultra_late_wipe_confirmed",
                    delta=strongest_delta, window_sec=30, direction=direction,
                    severity="high",
                ))

            if (
                "STOMP_THROW" in types
                and types & CONVERSION_TOWER_EVENTS
                and self._cooldown_ok(cur, "STOMP_THROW_WITH_OBJECTIVE_RISK", direction)
            ):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="STOMP_THROW_WITH_OBJECTIVE_RISK",
                    previous_value="STOMP_THROW+base_pressure",
                    current_value="stomp_throw_objective_risk",
                    delta=strongest_delta, window_sec=30, direction=direction,
                    severity="high",
                ))

            if (
                10 * 60 <= cur_time <= 30 * 60
                and "KILL_CONFIRMED_LEAD_SWING" in types
                and strongest_delta >= 1500
                and self._cooldown_ok(cur, "FIGHT_TO_GOLD_CONFIRM_30S", direction)
            ):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="FIGHT_TO_GOLD_CONFIRM_30S",
                    previous_value="kill_swing+gold_swing",
                    current_value="fight_to_gold_confirm",
                    delta=strongest_delta, window_sec=30, direction=direction,
                    severity="medium",
                ))

            self._append_late_recovery(cur, direction, group, strongest_delta)
            if self._late_recovery_chain_ready(cur, direction):
                if self._cooldown_ok(cur, "CHAINED_LATE_FIGHT_RECOVERY", direction):
                    out.append(self._base_event(
                        cur, mapping,
                        event_type="CHAINED_LATE_FIGHT_RECOVERY",
                        previous_value="late_recovery_sequence",
                        current_value="chained_late_fight_recovery",
                        delta=strongest_delta, window_sec=CHAINED_RECOVERY_WINDOW_SEC,
                        direction=direction, severity="high",
                    ))

        return out

    def _append_late_recovery(self, cur: dict, direction: str, group: list[DotaEvent], strongest_delta: float) -> None:
        cur_time = cur.get("game_time_sec")
        match_id = cur.get("match_id")
        if cur_time is None or not match_id or cur_time < CHAINED_LATE_FIGHT_TIME:
            return
        types = {event.event_type for event in group}
        qualifies = (
            types & {"KILL_CONFIRMED_LEAD_SWING", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE", "LATE_ECONOMIC_CRASH"}
            or any(e.event_type == "LEAD_SWING_60S" and isinstance(e.delta, (int, float)) and abs(e.delta) >= 5000 for e in group)
        )
        if not qualifies:
            return
        key = (str(match_id), direction)
        hist = self.late_recovery_history[key]
        if hist and hist[-1][0] == cur_time:
            return
        hist.append((int(cur_time), int(strongest_delta), "+".join(sorted(types))))

    def _late_recovery_chain_ready(self, cur: dict, direction: str) -> bool:
        cur_time = cur.get("game_time_sec")
        match_id = cur.get("match_id")
        if cur_time is None or not match_id:
            return False
        hist = self.late_recovery_history.get((str(match_id), direction))
        if not hist or len(hist) < 2:
            return False
        previous = list(hist)[:-1]
        return any(0 < int(cur_time) - ts <= CHAINED_RECOVERY_WINDOW_SEC for ts, _, _ in previous)

    # ------------------------------------------------------------------ #
    # Tower / structure events                                            #
    # ------------------------------------------------------------------ #

    def _tower_events(self, prev: dict, cur: dict, mapping: dict | None) -> list[DotaEvent]:
        prev_bs = prev.get("building_state")
        cur_bs = cur.get("building_state")
        
        if prev_bs is None or cur_bs is None or prev_bs == cur_bs:
            return []

        # GetTopLiveGame building_state decoding:
        # It is a 22-bit integer where 1=ALIVE, 0=DESTROYED.
        # Bits 0-10: Radiant buildings
        # Bits 11-21: Dire buildings
        # Standard 11-bit layout per side:
        # 0: Top T1, 1: Top T2, 2: Top T3
        # 3: Mid T1, 4: Mid T2, 5: Mid T3
        # 6: Bot T1, 7: Bot T2, 8: Bot T3
        # 9: T4 Left, 10: T4 Right

        # Radiant buildings falling (1 -> 0)
        prev_rad_alive = prev_bs & 0x7FF
        cur_rad_alive = cur_bs & 0x7FF
        rad_fallen = prev_rad_alive & ~cur_rad_alive

        # Dire buildings falling (1 -> 0)
        prev_dire_alive = (prev_bs >> 11) & 0x7FF
        cur_dire_alive = (cur_bs >> 11) & 0x7FF
        dire_fallen = prev_dire_alive & ~cur_dire_alive

        events: list[DotaEvent] = []

        if rad_fallen:
            events.extend(self._side_tower_events(
                prev_side_bits=prev_rad_alive,
                cur_side_bits=cur_rad_alive,
                fallen_side_bits=rad_fallen,
                cur=cur,
                mapping=mapping,
                direction="dire",
                previous_value=prev_bs,
                current_value=cur_bs,
            ))

        if dire_fallen:
            events.extend(self._side_tower_events(
                prev_side_bits=prev_dire_alive,
                cur_side_bits=cur_dire_alive,
                fallen_side_bits=dire_fallen,
                cur=cur,
                mapping=mapping,
                direction="radiant",
                previous_value=prev_bs,
                current_value=cur_bs,
            ))

        return events

    def _side_tower_events(
        self,
        prev_side_bits: int,
        cur_side_bits: int,
        fallen_side_bits: int,
        cur: dict,
        mapping: dict | None,
        direction: str,
        previous_value: int,
        current_value: int,
    ) -> list[DotaEvent]:
        out: list[DotaEvent] = []

        t4_count = _bit_count(fallen_side_bits & T4_MASK)
        t3_count = _bit_count(fallen_side_bits & T3_MASK)
        t2_count = _bit_count(fallen_side_bits & T2_MASK)

        tier_types_falling: set[str] = set()
        if t4_count:
            tier_types_falling.add("t4")
        if t3_count:
            tier_types_falling.add("t3")
        if t2_count:
            tier_types_falling.add("t2")

        # T4 tier
        if t4_count:
            prev_t4_alive = _bit_count(prev_side_bits & T4_MASK)
            cur_t4_alive = _bit_count(cur_side_bits & T4_MASK)
            event_type = "SECOND_T4_TOWER_FALL" if cur_t4_alive == 0 else "FIRST_T4_TOWER_FALL"
            if self._cooldown_ok(cur, event_type, direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type=event_type,
                    previous_value=previous_value, current_value=current_value,
                    delta=t4_count, window_sec=None, direction=direction,
                    severity="high",
                ))

            if cur_t4_alive == 0 and self._cooldown_ok(cur, "THRONE_EXPOSED", direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="THRONE_EXPOSED",
                    previous_value=previous_value, current_value=current_value,
                    delta=t4_count, window_sec=None, direction=direction,
                    severity="high",
                ))

        # T3 tier
        if t3_count:
            prev_t3_alive = _bit_count(prev_side_bits & T3_MASK)
            cur_t3_alive = _bit_count(cur_side_bits & T3_MASK)
            t3_dead_after = 3 - cur_t3_alive
            event_type = "MULTIPLE_T3_TOWERS_DOWN" if t3_dead_after >= 2 else "T3_TOWER_FALL"
            if self._cooldown_ok(cur, event_type, direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type=event_type,
                    previous_value=previous_value, current_value=current_value,
                    delta=t3_count, window_sec=None, direction=direction,
                    severity="high" if event_type == "MULTIPLE_T3_TOWERS_DOWN" else "medium",
                ))

            if cur_t3_alive == 0 and self._cooldown_ok(cur, "ALL_T3_TOWERS_DOWN", direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="ALL_T3_TOWERS_DOWN",
                    previous_value=previous_value, current_value=current_value,
                    delta=t3_dead_after, window_sec=None, direction=direction,
                    severity="high",
                ))

        # T2 tier
        if t2_count:
            if self._cooldown_ok(cur, "T2_TOWER_FALL", direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="T2_TOWER_FALL",
                    previous_value=previous_value, current_value=current_value,
                    delta=t2_count, window_sec=None, direction=direction,
                    severity="medium",
                ))

            cur_t2_alive = _bit_count(cur_side_bits & T2_MASK)
            t2_dead_after = 3 - cur_t2_alive
            context_type = None
            if cur_t2_alive == 0:
                context_type = "ALL_T2_TOWERS_DOWN"
            elif t2_dead_after >= 2:
                context_type = "MULTIPLE_T2_TOWERS_DOWN"

            if context_type and self._cooldown_ok(cur, context_type, direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type=context_type,
                    previous_value=previous_value, current_value=current_value,
                    delta=t2_dead_after, window_sec=None, direction=direction,
                    severity="medium",
                ))

        # Chain / cascade events: T3 + T4 falling together
        if "t3" in tier_types_falling and "t4" in tier_types_falling:
            if self._cooldown_ok(cur, "T3_PLUS_T4_CHAIN", direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="T3_PLUS_T4_CHAIN",
                    previous_value=previous_value, current_value=current_value,
                    delta=t3_count + t4_count, window_sec=None, direction=direction,
                    severity="high",
                ))

        # Multi-structure collapse: 2+ different tier types falling in same update
        if len(tier_types_falling) >= 2:
            if self._cooldown_ok(cur, "MULTI_STRUCTURE_COLLAPSE", direction):
                total_fallen = t2_count + t3_count + t4_count
                collapse_severity = "high" if len(tier_types_falling) >= 3 else "medium"
                out.append(self._base_event(
                    cur, mapping,
                    event_type="MULTI_STRUCTURE_COLLAPSE",
                    previous_value=previous_value, current_value=current_value,
                    delta=total_fallen, window_sec=None, direction=direction,
                    severity=collapse_severity,
                ))

        # T1s are intentionally ignored: not part of the final live trade model.
        return out

    # ------------------------------------------------------------------ #
    # Comeback events                                                     #
    # ------------------------------------------------------------------ #

    def _comeback_events(self, prev: dict, cur: dict, mapping: dict | None) -> list[DotaEvent]:
        prev_lead = prev.get("radiant_lead")
        cur_lead = cur.get("radiant_lead")
        if prev_lead is None or cur_lead is None:
            return []
        if prev_lead == 0 or cur_lead == 0 or (prev_lead > 0) == (cur_lead > 0):
            return []

        previous_deficit = abs(prev_lead)
        if previous_deficit < COMEBACK_MIN_PRIOR_DEFICIT:
            return []

        direction = "radiant" if cur_lead > 0 else "dire"
        event_type = "MAJOR_COMEBACK" if previous_deficit >= MAJOR_COMEBACK_PRIOR_DEFICIT else "COMEBACK"
        if not self._cooldown_ok(cur, event_type, direction):
            return []

        return [self._base_event(
            cur, mapping,
            event_type=event_type,
            previous_value=prev_lead, current_value=cur_lead,
            delta=cur_lead - prev_lead, window_sec=None, direction=direction,
            severity="high",
        )]

    # ------------------------------------------------------------------ #
    # Lead swing events                                                   #
    # ------------------------------------------------------------------ #

    def _lead_swing_events(self, hist: deque[dict], cur: dict, mapping: dict | None) -> list[DotaEvent]:
        cur_lead = cur.get("radiant_lead")
        cur_time = cur.get("game_time_sec")
        if cur_lead is None or cur_time is None or not hist:
            return []

        specs = [(60, _lead_swing_threshold(60, cur_time)), (30, _lead_swing_threshold(30, cur_time))]
        out: list[DotaEvent] = []
        for window_sec, threshold in specs:
            past = self._find_past_snapshot(hist, cur_time - window_sec, window_sec)
            if not past or past.get("radiant_lead") is None:
                continue
            delta = cur_lead - past["radiant_lead"]
            if abs(delta) < threshold:
                continue
            direction = "radiant" if delta > 0 else "dire"
            event_type = f"LEAD_SWING_{window_sec}S"
            severity = "high" if abs(delta) >= threshold * 2 else "medium"
            if window_sec == 30 and abs(delta) >= threshold * 3:
                event_type = "EXTREME_LEAD_SWING_30S"
                severity = "high"
            if _late_lead_swing_is_noise(cur_time, delta, threshold):
                continue
            if not self._cooldown_ok(cur, event_type, direction):
                continue
            out.append(self._base_event(
                cur, mapping,
                event_type=event_type,
                previous_value=past["radiant_lead"], current_value=cur_lead,
                delta=delta, window_sec=window_sec, direction=direction,
                severity=severity, threshold=threshold,
            ))
        return out

    def _score_confirmed_events(self, hist: deque[dict], cur: dict, mapping: dict | None) -> list[DotaEvent]:
        cur_time = cur.get("game_time_sec")
        cur_lead = cur.get("radiant_lead")
        cur_r_score = cur.get("radiant_score")
        cur_d_score = cur.get("dire_score")
        if cur_time is None or cur_lead is None or cur_r_score is None or cur_d_score is None or not hist:
            return []

        past = self._find_past_snapshot(hist, cur_time - 30, 30)
        if not past:
            return []
        past_lead = past.get("radiant_lead")
        past_r_score = past.get("radiant_score")
        past_d_score = past.get("dire_score")
        if past_lead is None or past_r_score is None or past_d_score is None:
            return []

        lead_delta = cur_lead - past_lead
        radiant_kills = cur_r_score - past_r_score
        dire_kills = cur_d_score - past_d_score
        kill_diff_delta = radiant_kills - dire_kills
        out: list[DotaEvent] = []

        # Stomp throw: a big favorite gives up kills and net worth to the trailing team.
        if cur_time >= STOMP_THROW_MIN_TIME and abs(past_lead) >= STOMP_THROW_MIN_LEAD:
            if past_lead > 0:
                throw_direction = "dire"
                score_ok = kill_diff_delta <= -STOMP_THROW_MIN_KILLS
                nw_ok = lead_delta <= -STOMP_THROW_MIN_NW_SWING
            else:
                throw_direction = "radiant"
                score_ok = kill_diff_delta >= STOMP_THROW_MIN_KILLS
                nw_ok = lead_delta >= STOMP_THROW_MIN_NW_SWING
            if score_ok and nw_ok and self._cooldown_ok(cur, "STOMP_THROW", throw_direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="STOMP_THROW",
                    previous_value=past_lead, current_value=cur_lead,
                    delta=lead_delta, window_sec=30, direction=throw_direction,
                    severity="high",
                ))

        if abs(lead_delta) >= KILL_CONFIRMED_LEAD_SWING_GOLD_30S:
            direction = "radiant" if lead_delta > 0 else "dire"
            same_direction_kills = (
                kill_diff_delta >= KILL_CONFIRMED_LEAD_SWING_KILLS_30S
                if direction == "radiant"
                else kill_diff_delta <= -KILL_CONFIRMED_LEAD_SWING_KILLS_30S
            )
            if same_direction_kills and self._cooldown_ok(cur, "KILL_CONFIRMED_LEAD_SWING", direction):
                out.append(self._base_event(
                    cur, mapping,
                    event_type="KILL_CONFIRMED_LEAD_SWING",
                    previous_value=past_lead, current_value=cur_lead,
                    delta=lead_delta, window_sec=30, direction=direction,
                    severity="high" if abs(lead_delta) >= EVENT_LEAD_SWING_30S * 2 else "medium",
                ))

        if abs(kill_diff_delta) >= KILL_BURST_30S:
            direction = "radiant" if kill_diff_delta > 0 else "dire"
            lead_same_direction = (
                lead_delta >= KILL_BURST_MIN_NW_CONFIRMATION if direction == "radiant"
                else lead_delta <= -KILL_BURST_MIN_NW_CONFIRMATION
            )
            if lead_same_direction:
                kills = abs(kill_diff_delta)
                if (cur_time >= 3000 and kills >= 4) or (cur_time >= 3300 and kills >= 3):
                    event_type = "ULTRA_LATE_WIPE"
                    severity = "high"
                elif cur_time >= 2400 and kills >= 3:
                    event_type = "LATE_GAME_WIPE"
                    severity = "high"
                else:
                    event_type = "KILL_BURST_30S"
                    severity = "high" if kills >= 5 else "medium"

                if self._cooldown_ok(cur, event_type, direction):
                    out.append(self._base_event(
                        cur, mapping,
                        event_type=event_type,
                        previous_value=f"{past_r_score}-{past_d_score}",
                        current_value=f"{cur_r_score}-{cur_d_score}",
                        delta=kill_diff_delta, window_sec=30, direction=direction,
                        severity=severity,
                    ))

        return out

    # ------------------------------------------------------------------ #
    # Composite objective-conversion events                               #
    # ------------------------------------------------------------------ #

    def _objective_conversion_events(
        self,
        events: list[DotaEvent],
        cur: dict,
        mapping: dict | None,
    ) -> list[DotaEvent]:
        """Emit a higher-confidence objective-conversion event.

        From the allowed live feed we cannot see Roshan, buybacks, or fight
        location. The strongest proxy is an objective falling while the same
        side also gained net worth and/or kills in the short lookback window.
        That combination should be scored differently from a raw split-push
        tower fall.
        """
        if not events:
            return []

        out: list[DotaEvent] = []
        by_dir: dict[str, list[DotaEvent]] = defaultdict(list)
        for evt in events:
            if evt.direction:
                by_dir[evt.direction].append(evt)

        for direction, group in by_dir.items():
            tower_events = [e for e in group if e.event_type in CONVERSION_TOWER_EVENTS]
            support_events = [e for e in group if e.event_type in CONVERSION_SUPPORT_EVENTS]
            if not tower_events or not support_events:
                continue

            event_type = self._conversion_event_type(tower_events)
            if event_type is None:
                continue
            if not self._cooldown_ok(cur, event_type, direction):
                continue

            strongest_tower = max(tower_events, key=lambda e: _conversion_tower_rank(e.event_type))
            strongest_support = max(support_events, key=lambda e: _conversion_support_rank(e.event_type))
            severity = "high" if event_type != "OBJECTIVE_CONVERSION_T2" else (
                "high" if strongest_support.event_type in {"LATE_GAME_WIPE", "ULTRA_LATE_WIPE", "EXTREME_LEAD_SWING_30S", "MAJOR_COMEBACK", "STOMP_THROW"} else "medium"
            )

            out.append(self._base_event(
                cur, mapping,
                event_type=event_type,
                previous_value=f"{strongest_support.event_type}+{strongest_tower.event_type}",
                current_value="same_direction_objective_conversion",
                delta=strongest_tower.delta,
                window_sec=30,
                direction=direction,
                severity=severity,
            ))

        return out

    @staticmethod
    def _conversion_event_type(tower_events: list[DotaEvent]) -> str | None:
        event_types = {e.event_type for e in tower_events}
        if event_types & {"THRONE_EXPOSED", "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL", "T3_PLUS_T4_CHAIN"}:
            return "OBJECTIVE_CONVERSION_T4"
        if event_types & {"MULTI_STRUCTURE_COLLAPSE"}:
            if event_types & {"FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL", "THRONE_EXPOSED"}:
                return "OBJECTIVE_CONVERSION_T4"
            return "OBJECTIVE_CONVERSION_T3"
        if event_types & {"ALL_T3_TOWERS_DOWN", "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN"}:
            return "OBJECTIVE_CONVERSION_T3"
        if "T2_TOWER_FALL" in event_types:
            return "OBJECTIVE_CONVERSION_T2"
        return None

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #

    def _find_past_snapshot(
        self,
        hist: deque[dict],
        target_game_time: int,
        window_sec: int | None = None,
    ) -> dict | None:
        candidates = [
            s for s in hist
            if s.get("game_time_sec") is not None and s["game_time_sec"] <= target_game_time
        ]
        if not candidates:
            return None
        past = max(candidates, key=lambda s: s["game_time_sec"])
        if window_sec is not None:
            tolerance = WINDOW_TOLERANCE_SEC.get(window_sec, max(5, int(window_sec * 0.35)))
            if target_game_time - int(past["game_time_sec"]) > tolerance:
                return None
        return past

    def _cooldown_ok(self, snap: dict, event_type: str, direction: str | None) -> bool:
        match_id = snap["match_id"]
        game_time = snap.get("game_time_sec")
        if game_time is None:
            return True
        key = (match_id, event_type, direction)
        last = self.last_emitted_game_time.get(key)
        if last is not None and game_time - last < EVENT_COOLDOWN_GAME_SECONDS:
            return False
        self.last_emitted_game_time[key] = game_time
        return True


def _conversion_tower_rank(event_type: str) -> int:
    ranks = {
        "T2_TOWER_FALL": 1,
        "T3_TOWER_FALL": 2,
        "MULTIPLE_T3_TOWERS_DOWN": 3,
        "FIRST_T4_TOWER_FALL": 4,
        "SECOND_T4_TOWER_FALL": 5,
    }
    return ranks.get(event_type, 0)


def _conversion_support_rank(event_type: str) -> int:
    ranks = {
        "KILL_BURST_30S": 1,
        "LEAD_SWING_30S": 2,
        "LEAD_SWING_60S": 2,
        "KILL_CONFIRMED_LEAD_SWING": 3,
        "COMEBACK": 3,
        "EXTREME_LEAD_SWING_30S": 4,
        "LATE_GAME_WIPE": 5,
        "ULTRA_LATE_WIPE": 6,
        "MAJOR_COMEBACK": 6,
        "STOMP_THROW": 6,
    }
    return ranks.get(event_type, 0)


def _lead_swing_threshold(window_sec: int, duration_sec: int) -> int:
    minute = duration_sec / 60.0
    if window_sec == 30:
        if minute < 20:
            return 1500
        if minute < 35:
            return 2500
        if minute < 50:
            return 4000
        return 6000
    if minute < 20:
        return 2000
    if minute < 35:
        return 3500
    if minute < 50:
        return 5000
    return 7500


def _late_lead_swing_is_noise(cur_time: int, delta: int, threshold: int) -> bool:
    if cur_time >= ULTRA_LATE_LEAD_SWING_DEMOTE_TIME:
        return abs(delta) < threshold * 3
    if cur_time >= LATE_LEAD_SWING_DEMOTE_TIME:
        return abs(delta) < threshold * 2
    return False


def _event_dedupe_key(event: DotaEvent) -> str:
    return "|".join(str(part) for part in (
        event.match_id,
        event.event_type,
        event.direction,
        event.previous_value,
        event.current_value,
        event.delta,
        event.window_sec,
    ))


def _event_quality(event: DotaEvent) -> float:
    base = float(event.base_pressure_score or 0.0)
    conversion = float(event.conversion_score or 0.0)
    fight = float(event.fight_pressure_score or 0.0)
    economy = float(event.economic_pressure_score or 0.0)
    return (0.35 * base) + (0.25 * conversion) + (0.20 * fight) + (0.20 * economy)


def _bit_count(value: int) -> int:
    return int(value).bit_count()


def _to_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
