from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, replace
from typing import Any

from config import EVENT_COOLDOWN_GAME_SECONDS
from event_taxonomy import EVENT_SCHEMA_VERSION, event_family, event_is_primary, event_tier

COMEBACK_MIN_PRIOR_DEFICIT = 3000
MAJOR_COMEBACK_PRIOR_DEFICIT = 8000
COMEBACK_RECOVERY_MIN_SWING = 1800
MAJOR_COMEBACK_RECOVERY_MIN_SWING = 3500
STOMP_THROW_MIN_LEAD = 12_000
STOMP_THROW_MIN_NW_SWING = 2_500
STOMP_THROW_MIN_KILLS = 2
STOMP_THROW_MIN_TIME = 30 * 60
LATE_FIGHT_TIME = 40 * 60
ULTRA_LATE_FIGHT_TIME = 50 * 60
EVENT_DEDUPE_SECONDS = 120

DIRECT_GAP_SEC = 20
NORMAL_GAP_SEC = 75
STALE_GAP_SEC = 150
MAX_FIGHT_GAP_SEC = 90

T1_MASK = (1 << 0) | (1 << 3) | (1 << 6)
T2_MASK = (1 << 1) | (1 << 4) | (1 << 7)
T3_MASK = (1 << 2) | (1 << 5) | (1 << 8)
T4_MASK = (1 << 9) | (1 << 10)
SIDE_MASK = 0x7FF

TACTICAL_PRIORITY: dict[str, int] = {
    "OBJECTIVE_CONVERSION_T4": 120,
    "THRONE_EXPOSED": 110,
    "OBJECTIVE_CONVERSION_T3": 100,
    "POLL_ULTRA_LATE_FIGHT_FLIP": 90,
    "POLL_STOMP_THROW_CONFIRMED": 80,
    "POLL_LATE_FIGHT_FLIP": 70,
    "POLL_LEAD_FLIP_WITH_KILLS": 60,
    "POLL_MAJOR_COMEBACK_RECOVERY": 50,
    "POLL_KILL_BURST_CONFIRMED": 40,
    "POLL_FIGHT_SWING": 30,
    "POLL_COMEBACK_RECOVERY": 20,
    "OBJECTIVE_CONVERSION_T2": 10,
    "BASE_PRESSURE_T4": 8,
    "BASE_PRESSURE_T3_COLLAPSE": 6,
    "BLOODY_EVEN_FIGHT": 1,
}

CONVERSION_TOWER_COMPONENTS = frozenset({
    "T2_TOWER_FALL",
    "MULTIPLE_T2_TOWERS_DOWN",
    "ALL_T2_TOWERS_DOWN",
    "T3_TOWER_FALL",
    "MULTIPLE_T3_TOWERS_DOWN",
    "ALL_T3_TOWERS_DOWN",
    "FIRST_T4_TOWER_FALL",
    "SECOND_T4_TOWER_FALL",
    "T3_PLUS_T4_CHAIN",
    "MULTI_STRUCTURE_COLLAPSE",
    "THRONE_EXPOSED_COMPONENT",
})

TACTICAL_SUPPORT_COMPONENTS = frozenset({
    "POLL_FIGHT_SWING",
    "POLL_KILL_BURST_CONFIRMED",
    "POLL_LEAD_FLIP_WITH_KILLS",
    "POLL_COMEBACK_RECOVERY",
    "POLL_MAJOR_COMEBACK_RECOVERY",
    "POLL_STOMP_THROW_CONFIRMED",
    "POLL_LATE_FIGHT_FLIP",
    "POLL_ULTRA_LATE_FIGHT_FLIP",
})

_EVENT_BASE_PRESSURE: dict[str, float] = {
    "OBJECTIVE_CONVERSION_T4": 0.90,
    "THRONE_EXPOSED": 1.00,
    "OBJECTIVE_CONVERSION_T3": 0.70,
    "POLL_ULTRA_LATE_FIGHT_FLIP": 0.72,
    "POLL_STOMP_THROW_CONFIRMED": 0.62,
    "POLL_LATE_FIGHT_FLIP": 0.58,
    "POLL_LEAD_FLIP_WITH_KILLS": 0.55,
    "POLL_MAJOR_COMEBACK_RECOVERY": 0.50,
    "POLL_KILL_BURST_CONFIRMED": 0.38,
    "POLL_FIGHT_SWING": 0.32,
    "POLL_COMEBACK_RECOVERY": 0.34,
    "OBJECTIVE_CONVERSION_T2": 0.45,
    "BASE_PRESSURE_T4": 0.72,
    "BASE_PRESSURE_T3_COLLAPSE": 0.55,
    "BLOODY_EVEN_FIGHT": 0.12,
}

_EVENT_CONFIDENCE: dict[str, float] = {
    "OBJECTIVE_CONVERSION_T4": 0.90,
    "THRONE_EXPOSED": 1.00,
    "OBJECTIVE_CONVERSION_T3": 0.84,
    "POLL_ULTRA_LATE_FIGHT_FLIP": 0.84,
    "POLL_STOMP_THROW_CONFIRMED": 0.80,
    "POLL_LATE_FIGHT_FLIP": 0.76,
    "POLL_LEAD_FLIP_WITH_KILLS": 0.78,
    "POLL_MAJOR_COMEBACK_RECOVERY": 0.76,
    "POLL_KILL_BURST_CONFIRMED": 0.68,
    "POLL_FIGHT_SWING": 0.62,
    "POLL_COMEBACK_RECOVERY": 0.62,
    "OBJECTIVE_CONVERSION_T2": 0.70,
    "BASE_PRESSURE_T4": 0.78,
    "BASE_PRESSURE_T3_COLLAPSE": 0.68,
    "BLOODY_EVEN_FIGHT": 0.35,
}


@dataclass(frozen=True)
class EventComponent:
    component_type: str
    direction: str | None
    delta: int | float | None
    window_sec: int | None
    previous_value: str | int | float | None = None
    current_value: str | int | float | None = None


@dataclass(frozen=True)
class SnapshotDelta:
    previous: dict
    current: dict
    snapshot_gap_sec: int
    source_cadence_quality: str
    networth_delta: int | None
    radiant_kills_delta: int | None
    dire_kills_delta: int | None
    kill_diff_delta: int | None
    total_kills_delta: int | None
    lead_flipped: bool

    @property
    def networth_delta_per_30s(self) -> float | None:
        if self.networth_delta is None or self.snapshot_gap_sec <= 0:
            return None
        return self.networth_delta * 30.0 / self.snapshot_gap_sec

    @property
    def kill_diff_delta_per_30s(self) -> float | None:
        if self.kill_diff_delta is None or self.snapshot_gap_sec <= 0:
            return None
        return self.kill_diff_delta * 30.0 / self.snapshot_gap_sec


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
    component_event_types: str | None = None
    component_deltas: str | None = None
    component_window_sec: str | None = None
    event_schema_version: str = EVENT_SCHEMA_VERSION
    snapshot_gap_sec: int | None = None
    actual_window_sec: int | None = None
    networth_delta: int | None = None
    kill_diff_delta: int | None = None
    total_kills_delta: int | None = None
    networth_delta_per_30s: float | None = None
    kill_diff_delta_per_30s: float | None = None
    source_cadence_quality: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EventDetector:
    """Cadence-aware TopLive event detector.

    Events are built from the immediately previous valid snapshot. Fixed 30s/60s
    event names are retired as primary outputs; any old-style evidence is kept in
    component metadata for calibration.
    """

    def __init__(self, max_history: int = 720):
        self.history: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=max_history))
        self.last_emitted_game_time: dict[tuple[str, str, str | None], int] = {}
        self.last_emitted_dedupe_game_time: dict[str, int] = {}

    def observe(self, game: dict, mapping: dict | None = None) -> list[DotaEvent]:
        match_id = str(game.get("match_id") or game.get("lobby_id") or "")
        if not match_id:
            return []

        snapshot = self._snapshot(game)
        hist = self.history[match_id]
        previous = hist[-1] if hist else None
        events: list[DotaEvent] = []

        if previous:
            delta = self._snapshot_delta(previous, snapshot)
            if delta is not None:
                components = self._build_components(delta)
                events = self._build_tactical_events(delta, components, mapping)
                events = self._enrich_pressure(events, delta)
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
            "data_source": game.get("data_source"),
        }

    def _snapshot_delta(self, previous: dict, current: dict) -> SnapshotDelta | None:
        prev_time = previous.get("game_time_sec")
        cur_time = current.get("game_time_sec")
        if prev_time is None or cur_time is None or cur_time < prev_time:
            return None
        gap = int(cur_time - prev_time)
        if gap <= 0:
            return None

        prev_lead = previous.get("radiant_lead")
        cur_lead = current.get("radiant_lead")
        networth_delta = cur_lead - prev_lead if prev_lead is not None and cur_lead is not None else None

        prev_rs = previous.get("radiant_score")
        prev_ds = previous.get("dire_score")
        cur_rs = current.get("radiant_score")
        cur_ds = current.get("dire_score")
        radiant_kills_delta = cur_rs - prev_rs if prev_rs is not None and cur_rs is not None else None
        dire_kills_delta = cur_ds - prev_ds if prev_ds is not None and cur_ds is not None else None
        kill_diff_delta = None
        total_kills_delta = None
        if radiant_kills_delta is not None and dire_kills_delta is not None:
            kill_diff_delta = radiant_kills_delta - dire_kills_delta
            total_kills_delta = radiant_kills_delta + dire_kills_delta

        lead_flipped = (
            prev_lead is not None
            and cur_lead is not None
            and prev_lead != 0
            and cur_lead != 0
            and (prev_lead > 0) != (cur_lead > 0)
        )
        return SnapshotDelta(
            previous=previous,
            current=current,
            snapshot_gap_sec=gap,
            source_cadence_quality=_cadence_quality(gap),
            networth_delta=networth_delta,
            radiant_kills_delta=radiant_kills_delta,
            dire_kills_delta=dire_kills_delta,
            kill_diff_delta=kill_diff_delta,
            total_kills_delta=total_kills_delta,
            lead_flipped=lead_flipped,
        )

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

    def _event_from_components(
        self,
        event_type: str,
        direction: str | None,
        delta: SnapshotDelta,
        mapping: dict | None,
        components: list[EventComponent],
        *,
        previous_value: str | int | float | None = None,
        current_value: str | int | float | None = None,
        event_delta: int | float | None = None,
        threshold: int | float | None = None,
        severity: str = "medium",
    ) -> DotaEvent:
        return self._base_event(
            delta.current,
            mapping,
            event_type=event_type,
            previous_value=previous_value,
            current_value=current_value,
            delta=event_delta,
            window_sec=delta.snapshot_gap_sec,
            direction=direction,
            severity=severity,
            threshold=threshold,
            snapshot_gap_sec=delta.snapshot_gap_sec,
            actual_window_sec=delta.snapshot_gap_sec,
            networth_delta=delta.networth_delta,
            kill_diff_delta=delta.kill_diff_delta,
            total_kills_delta=delta.total_kills_delta,
            networth_delta_per_30s=_round_optional(delta.networth_delta_per_30s),
            kill_diff_delta_per_30s=_round_optional(delta.kill_diff_delta_per_30s),
            source_cadence_quality=delta.source_cadence_quality,
            **_component_metadata(components),
        )

    def _build_components(self, delta: SnapshotDelta) -> list[EventComponent]:
        components: list[EventComponent] = []
        prev = delta.previous
        cur = delta.current
        gap = delta.snapshot_gap_sec

        if delta.networth_delta is not None and delta.networth_delta != 0:
            components.append(EventComponent(
                "NETWORTH_DELTA",
                _direction_from_delta(delta.networth_delta),
                delta.networth_delta,
                gap,
                prev.get("radiant_lead"),
                cur.get("radiant_lead"),
            ))

        if delta.kill_diff_delta is not None and delta.kill_diff_delta != 0:
            components.append(EventComponent(
                "KILL_DIFF_DELTA",
                _direction_from_delta(delta.kill_diff_delta),
                delta.kill_diff_delta,
                gap,
                _score_value(prev),
                _score_value(cur),
            ))

        if delta.lead_flipped:
            components.append(EventComponent(
                "LEAD_FLIP",
                "radiant" if (cur.get("radiant_lead") or 0) > 0 else "dire",
                delta.networth_delta,
                gap,
                prev.get("radiant_lead"),
                cur.get("radiant_lead"),
            ))

        components.extend(self._structure_components(delta))
        if (
            gap <= NORMAL_GAP_SEC
            and delta.source_cadence_quality != "invalid_gap"
            and delta.total_kills_delta is not None
            and delta.kill_diff_delta is not None
            and delta.total_kills_delta >= 4
            and abs(delta.kill_diff_delta) <= 1
            and abs(delta.networth_delta or 0) < 1000
        ):
            components.append(EventComponent(
                "BLOODY_EVEN_FIGHT",
                None,
                delta.kill_diff_delta,
                gap,
                _score_value(prev),
                _score_value(cur),
            ))
        return components

    def _structure_components(self, delta: SnapshotDelta) -> list[EventComponent]:
        prev = delta.previous
        cur = delta.current
        if cur.get("data_source") == "top_live" and cur.get("tower_state") is None:
            return []

        prev_bs = prev.get("tower_state")
        cur_bs = cur.get("tower_state")
        if prev_bs is None:
            prev_bs = prev.get("building_state")
        if cur_bs is None:
            cur_bs = cur.get("building_state")
        if prev_bs is None or cur_bs is None or prev_bs == cur_bs:
            return []

        prev_rad_alive = prev_bs & SIDE_MASK
        cur_rad_alive = cur_bs & SIDE_MASK
        rad_fallen = prev_rad_alive & ~cur_rad_alive

        prev_dire_alive = (prev_bs >> 11) & SIDE_MASK
        cur_dire_alive = (cur_bs >> 11) & SIDE_MASK
        dire_fallen = prev_dire_alive & ~cur_dire_alive

        components: list[EventComponent] = []
        if rad_fallen:
            components.extend(self._side_structure_components(
                prev_rad_alive, cur_rad_alive, rad_fallen, "dire", prev_bs, cur_bs, delta.snapshot_gap_sec
            ))
        if dire_fallen:
            components.extend(self._side_structure_components(
                prev_dire_alive, cur_dire_alive, dire_fallen, "radiant", prev_bs, cur_bs, delta.snapshot_gap_sec
            ))
        return components

    def _side_structure_components(
        self,
        prev_side_bits: int,
        cur_side_bits: int,
        fallen_side_bits: int,
        direction: str,
        previous_value: int,
        current_value: int,
        gap: int,
    ) -> list[EventComponent]:
        t4_count = _bit_count(fallen_side_bits & T4_MASK)
        t3_count = _bit_count(fallen_side_bits & T3_MASK)
        t2_count = _bit_count(fallen_side_bits & T2_MASK)
        components: list[EventComponent] = []

        if t4_count:
            cur_t4_alive = _bit_count(cur_side_bits & T4_MASK)
            components.append(EventComponent(
                "SECOND_T4_TOWER_FALL" if cur_t4_alive == 0 else "FIRST_T4_TOWER_FALL",
                direction,
                t4_count,
                gap,
                previous_value,
                current_value,
            ))
            if cur_t4_alive == 0:
                components.append(EventComponent(
                    "THRONE_EXPOSED_COMPONENT",
                    direction,
                    t4_count,
                    gap,
                    previous_value,
                    current_value,
                ))

        if t3_count:
            cur_t3_alive = _bit_count(cur_side_bits & T3_MASK)
            t3_dead_after = 3 - cur_t3_alive
            components.append(EventComponent(
                "MULTIPLE_T3_TOWERS_DOWN" if t3_dead_after >= 2 else "T3_TOWER_FALL",
                direction,
                t3_count,
                gap,
                previous_value,
                current_value,
            ))
            if cur_t3_alive == 0:
                components.append(EventComponent(
                    "ALL_T3_TOWERS_DOWN",
                    direction,
                    t3_dead_after,
                    gap,
                    previous_value,
                    current_value,
                ))

        if t2_count:
            components.append(EventComponent("T2_TOWER_FALL", direction, t2_count, gap, previous_value, current_value))
            cur_t2_alive = _bit_count(cur_side_bits & T2_MASK)
            t2_dead_after = 3 - cur_t2_alive
            if cur_t2_alive == 0:
                components.append(EventComponent("ALL_T2_TOWERS_DOWN", direction, t2_dead_after, gap, previous_value, current_value))
            elif t2_dead_after >= 2:
                components.append(EventComponent("MULTIPLE_T2_TOWERS_DOWN", direction, t2_dead_after, gap, previous_value, current_value))

        tiers = set()
        if t2_count:
            tiers.add("t2")
        if t3_count:
            tiers.add("t3")
        if t4_count:
            tiers.add("t4")
        if "t3" in tiers and "t4" in tiers:
            components.append(EventComponent("T3_PLUS_T4_CHAIN", direction, t3_count + t4_count, gap, previous_value, current_value))
        if len(tiers) >= 2:
            components.append(EventComponent("MULTI_STRUCTURE_COLLAPSE", direction, t2_count + t3_count + t4_count, gap, previous_value, current_value))
        return components

    def _build_tactical_events(
        self,
        delta: SnapshotDelta,
        components: list[EventComponent],
        mapping: dict | None,
    ) -> list[DotaEvent]:
        candidates: list[DotaEvent] = []
        candidates.extend(self._fight_candidates(delta, components, mapping))
        candidates.extend(self._comeback_candidates(delta, components, mapping))
        candidates.extend(self._base_pressure_candidates(delta, components, mapping))
        candidates.extend(self._objective_conversion_candidates(delta, components, candidates, mapping))

        # Bloody-even is research-only and directionless; keep it outside ranking.
        if any(c.component_type == "BLOODY_EVEN_FIGHT" for c in components):
            bloody = [c for c in components if c.component_type == "BLOODY_EVEN_FIGHT"]
            candidates.append(self._event_from_components(
                "BLOODY_EVEN_FIGHT",
                None,
                delta,
                mapping,
                bloody,
                previous_value=bloody[0].previous_value,
                current_value=bloody[0].current_value,
                event_delta=bloody[0].delta,
                severity="medium",
            ))

        ranked: list[DotaEvent] = []
        for direction, group in _group_events_by_direction(candidates).items():
            if direction is None:
                ranked.extend(group)
                continue
            primary = max(group, key=lambda e: (TACTICAL_PRIORITY.get(e.event_type, 0), float(e.event_quality or 0.0)))
            if not self._cooldown_ok(delta.current, primary.event_type, primary.direction):
                continue
            lower = [e for e in group if e is not primary]
            ranked.append(self._merge_components(primary, lower))
        return ranked

    def _fight_candidates(
        self,
        delta: SnapshotDelta,
        components: list[EventComponent],
        mapping: dict | None,
    ) -> list[DotaEvent]:
        if delta.networth_delta is None or delta.kill_diff_delta is None:
            return []
        gap = delta.snapshot_gap_sec
        if gap > MAX_FIGHT_GAP_SEC:
            return []

        net_dir = _direction_from_delta(delta.networth_delta)
        kill_dir = _direction_from_delta(delta.kill_diff_delta)
        agrees = net_dir is not None and kill_dir is not None and net_dir == kill_dir
        if not agrees:
            return []

        abs_nw = abs(delta.networth_delta)
        abs_kill = abs(delta.kill_diff_delta)
        base_components = _components_for_direction(components, net_dir, {"NETWORTH_DELTA", "KILL_DIFF_DELTA", "LEAD_FLIP"})
        out: list[DotaEvent] = []

        if gap <= NORMAL_GAP_SEC and abs_kill >= 2 and abs_nw >= 1000:
            out.append(self._event_from_components(
                "POLL_FIGHT_SWING",
                net_dir,
                delta,
                mapping,
                base_components,
                previous_value=delta.previous.get("radiant_lead"),
                current_value=delta.current.get("radiant_lead"),
                event_delta=delta.networth_delta,
                threshold=1000,
                severity="high" if abs_kill >= 3 or abs_nw >= 2500 else "medium",
            ))

        if gap <= NORMAL_GAP_SEC and abs_kill >= 3 and abs_nw >= 500:
            out.append(self._event_from_components(
                "POLL_KILL_BURST_CONFIRMED",
                net_dir,
                delta,
                mapping,
                base_components,
                previous_value=_score_value(delta.previous),
                current_value=_score_value(delta.current),
                event_delta=delta.kill_diff_delta,
                threshold=3,
                severity="high",
            ))

        if (
            delta.lead_flipped
            and abs(delta.previous.get("radiant_lead") or 0) >= 1500
            and abs_nw >= 1500
            and kill_dir == ("radiant" if (delta.current.get("radiant_lead") or 0) > 0 else "dire")
        ):
            out.append(self._event_from_components(
                "POLL_LEAD_FLIP_WITH_KILLS",
                net_dir,
                delta,
                mapping,
                base_components,
                previous_value=delta.previous.get("radiant_lead"),
                current_value=delta.current.get("radiant_lead"),
                event_delta=delta.networth_delta,
                threshold=1500,
                severity="high",
            ))

        cur_time = delta.current.get("game_time_sec") or 0
        if cur_time >= LATE_FIGHT_TIME and abs_kill >= 3 and abs_nw >= 2500:
            out.append(self._event_from_components(
                "POLL_LATE_FIGHT_FLIP",
                net_dir,
                delta,
                mapping,
                base_components,
                previous_value=delta.previous.get("radiant_lead"),
                current_value=delta.current.get("radiant_lead"),
                event_delta=delta.networth_delta,
                threshold=2500,
                severity="high",
            ))

        if cur_time >= ULTRA_LATE_FIGHT_TIME and abs_kill >= 3 and (abs_nw >= 3000 or delta.lead_flipped):
            out.append(self._event_from_components(
                "POLL_ULTRA_LATE_FIGHT_FLIP",
                net_dir,
                delta,
                mapping,
                base_components,
                previous_value=delta.previous.get("radiant_lead"),
                current_value=delta.current.get("radiant_lead"),
                event_delta=delta.networth_delta,
                threshold=3000,
                severity="high",
            ))

        prev_lead = delta.previous.get("radiant_lead")
        if cur_time >= STOMP_THROW_MIN_TIME and prev_lead is not None and abs(prev_lead) >= STOMP_THROW_MIN_LEAD:
            trailing = "dire" if prev_lead > 0 else "radiant"
            trailing_nw = -delta.networth_delta if trailing == "dire" else delta.networth_delta
            trailing_kills = -delta.kill_diff_delta if trailing == "dire" else delta.kill_diff_delta
            if trailing_nw >= STOMP_THROW_MIN_NW_SWING and trailing_kills >= STOMP_THROW_MIN_KILLS:
                out.append(self._event_from_components(
                    "POLL_STOMP_THROW_CONFIRMED",
                    trailing,
                    delta,
                    mapping,
                    base_components,
                    previous_value=prev_lead,
                    current_value=delta.current.get("radiant_lead"),
                    event_delta=trailing_nw,
                    threshold=STOMP_THROW_MIN_NW_SWING,
                    severity="high",
                ))
        return out

    def _comeback_candidates(
        self,
        delta: SnapshotDelta,
        components: list[EventComponent],
        mapping: dict | None,
    ) -> list[DotaEvent]:
        if delta.networth_delta is None or delta.snapshot_gap_sec > MAX_FIGHT_GAP_SEC:
            return []
        prev_lead = delta.previous.get("radiant_lead")
        cur_lead = delta.current.get("radiant_lead")
        if prev_lead is None or cur_lead is None or prev_lead == 0 or cur_lead == 0:
            return []

        direction = None
        recovered = 0
        if prev_lead < 0 and cur_lead < 0 and delta.networth_delta > 0:
            direction = "radiant"
            recovered = delta.networth_delta
        elif prev_lead > 0 and cur_lead > 0 and delta.networth_delta < 0:
            direction = "dire"
            recovered = -delta.networth_delta
        else:
            return []

        prior_deficit = abs(prev_lead)
        if prior_deficit < COMEBACK_MIN_PRIOR_DEFICIT:
            return []
        if prior_deficit >= MAJOR_COMEBACK_PRIOR_DEFICIT:
            event_type = "POLL_MAJOR_COMEBACK_RECOVERY"
            threshold = MAJOR_COMEBACK_RECOVERY_MIN_SWING
        else:
            event_type = "POLL_COMEBACK_RECOVERY"
            threshold = COMEBACK_RECOVERY_MIN_SWING
        if recovered < threshold:
            return []

        comps = _components_for_direction(components, direction, {"NETWORTH_DELTA", "KILL_DIFF_DELTA"})
        return [self._event_from_components(
            event_type,
            direction,
            delta,
            mapping,
            comps,
            previous_value=prev_lead,
            current_value=cur_lead,
            event_delta=delta.networth_delta,
            threshold=threshold,
            severity="high" if recovered >= threshold * 1.5 else "medium",
        )]

    def _base_pressure_candidates(
        self,
        delta: SnapshotDelta,
        components: list[EventComponent],
        mapping: dict | None,
    ) -> list[DotaEvent]:
        out: list[DotaEvent] = []
        by_dir: dict[str, list[EventComponent]] = defaultdict(list)
        for comp in components:
            if comp.direction and comp.component_type in CONVERSION_TOWER_COMPONENTS:
                by_dir[comp.direction].append(comp)

        for direction, comps in by_dir.items():
            types = {c.component_type for c in comps}
            source = delta.current.get("data_source")
            t4_reliable = source != "top_live"
            if "THRONE_EXPOSED_COMPONENT" in types and t4_reliable:
                out.append(self._event_from_components(
                    "THRONE_EXPOSED", direction, delta, mapping, comps,
                    previous_value=comps[0].previous_value, current_value=comps[0].current_value,
                    event_delta=max((abs(float(c.delta or 0)) for c in comps), default=0.0),
                    severity="high",
                ))
            elif ({"FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL"} & types) and t4_reliable:
                out.append(self._event_from_components(
                    "BASE_PRESSURE_T4", direction, delta, mapping, comps,
                    previous_value=comps[0].previous_value, current_value=comps[0].current_value,
                    event_delta=max((abs(float(c.delta or 0)) for c in comps), default=0.0),
                    severity="high",
                ))
            elif "ALL_T3_TOWERS_DOWN" in types or "MULTIPLE_T3_TOWERS_DOWN" in types:
                out.append(self._event_from_components(
                    "BASE_PRESSURE_T3_COLLAPSE", direction, delta, mapping, comps,
                    previous_value=comps[0].previous_value, current_value=comps[0].current_value,
                    event_delta=max((abs(float(c.delta or 0)) for c in comps), default=0.0),
                    severity="high" if "ALL_T3_TOWERS_DOWN" in types else "medium",
                ))
        return out

    def _objective_conversion_candidates(
        self,
        delta: SnapshotDelta,
        components: list[EventComponent],
        tactical_candidates: list[DotaEvent],
        mapping: dict | None,
    ) -> list[DotaEvent]:
        out: list[DotaEvent] = []
        for direction in {c.direction for c in components if c.direction}:
            tower = [
                c for c in components
                if c.direction == direction and c.component_type in CONVERSION_TOWER_COMPONENTS
            ]
            support = [
                e for e in tactical_candidates
                if e.direction == direction and e.event_type in TACTICAL_SUPPORT_COMPONENTS
            ]
            if not tower or not support:
                continue

            event_type = _conversion_event_type(tower)
            if event_type is None:
                continue
            strongest_support = max(support, key=lambda e: TACTICAL_PRIORITY.get(e.event_type, 0))
            conv_components = list(tower)
            conv_components.extend(EventComponent(
                strongest_support.event_type,
                direction,
                strongest_support.delta,
                strongest_support.window_sec,
                strongest_support.previous_value,
                strongest_support.current_value,
            ) for _ in [0])
            out.append(self._event_from_components(
                event_type,
                direction,
                delta,
                mapping,
                conv_components,
                previous_value=f"{strongest_support.event_type}+{max(tower, key=_conversion_tower_rank).component_type}",
                current_value="same_direction_objective_conversion",
                event_delta=max((abs(float(c.delta or 0)) for c in tower), default=0.0),
                severity="high" if event_type != "OBJECTIVE_CONVERSION_T2" else "medium",
            ))
        return out

    def _merge_components(self, primary: DotaEvent, lower: list[DotaEvent]) -> DotaEvent:
        if not lower:
            return primary
        component_types = [primary.component_event_types or ""]
        component_deltas = [primary.component_deltas or ""]
        component_windows = [primary.component_window_sec or ""]
        for event in lower:
            component_types.append(event.event_type)
            if event.component_event_types:
                component_types.append(event.component_event_types)
            component_deltas.append("" if event.delta is None else str(event.delta))
            if event.component_deltas:
                component_deltas.append(event.component_deltas)
            component_windows.append("" if event.window_sec is None else str(event.window_sec))
            if event.component_window_sec:
                component_windows.append(event.component_window_sec)
        return replace(
            primary,
            component_event_types="+".join(x for x in component_types if x),
            component_deltas="+".join(x for x in component_deltas if x != ""),
            component_window_sec="+".join(x for x in component_windows if x != ""),
        )

    def _enrich_pressure(self, events: list[DotaEvent], delta: SnapshotDelta) -> list[DotaEvent]:
        enriched: list[DotaEvent] = []
        for evt in events:
            bp = _EVENT_BASE_PRESSURE.get(evt.event_type, 0.3)
            conf = _EVENT_CONFIDENCE.get(evt.event_type, 0.5)
            fp: float | None = None
            ep: float | None = None
            cs: float | None = None

            if delta.kill_diff_delta is not None and evt.direction is not None:
                signed = delta.kill_diff_delta if evt.direction == "radiant" else -delta.kill_diff_delta
                fp = max(0.0, min(signed / 5.0, 1.0))
            if delta.networth_delta is not None and evt.direction is not None:
                signed = delta.networth_delta if evt.direction == "radiant" else -delta.networth_delta
                ep = max(0.0, min(signed / 5000.0, 1.0))
            if fp is not None and ep is not None:
                cs = min(math.sqrt(fp * ep), 1.0) if fp > 0 and ep > 0 else max(fp, ep) * 0.3
            elif fp is not None:
                cs = fp * 0.4 if fp > 0 else None
            elif ep is not None:
                cs = ep * 0.4 if ep > 0 else None

            if delta.source_cadence_quality == "direct":
                conf = min(conf + 0.05, 1.0)
            elif delta.source_cadence_quality == "stale_gap":
                conf = max(conf - 0.12, 0.0)
            elif delta.source_cadence_quality == "invalid_gap":
                conf = max(conf - 0.25, 0.0)
            if evt.event_type.startswith("OBJECTIVE_CONVERSION_"):
                conf = min(conf + 0.08, 1.0)
            if fp and fp > 0:
                conf = min(conf + 0.05, 1.0)
            if ep and ep > 0:
                conf = min(conf + 0.05, 1.0)

            enriched.append(replace(
                evt,
                base_pressure_score=round(bp, 3),
                fight_pressure_score=round(fp, 3) if fp is not None else None,
                economic_pressure_score=round(ep, 3) if ep is not None else None,
                conversion_score=round(cs, 3) if cs is not None else None,
                event_confidence=round(conf, 3),
            ))
        return enriched

    def _add_event_metadata(self, events: list[DotaEvent]) -> list[DotaEvent]:
        out = []
        for event in events:
            out.append(replace(
                event,
                event_dedupe_key=_event_dedupe_key(event),
                event_is_primary=event_is_primary(event.event_type),
                event_tier=event_tier(event.event_type),
                event_family=event_family(event.event_type),
                event_quality=round(_event_quality(event), 3),
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


def _group_events_by_direction(events: list[DotaEvent]) -> dict[str | None, list[DotaEvent]]:
    grouped: dict[str | None, list[DotaEvent]] = defaultdict(list)
    for event in events:
        grouped[event.direction].append(event)
    return grouped


def _components_for_direction(
    components: list[EventComponent],
    direction: str,
    allowed: set[str],
) -> list[EventComponent]:
    return [
        comp for comp in components
        if comp.component_type in allowed and (comp.direction in (direction, None))
    ]


def _direction_from_delta(value: int | float | None) -> str | None:
    if value is None or value == 0:
        return None
    return "radiant" if value > 0 else "dire"


def _cadence_quality(gap: int) -> str:
    if gap <= DIRECT_GAP_SEC:
        return "direct"
    if gap <= NORMAL_GAP_SEC:
        return "normal"
    if gap <= STALE_GAP_SEC:
        return "stale_gap"
    return "invalid_gap"


def _score_value(snapshot: dict) -> str:
    return f"{snapshot.get('radiant_score')}-{snapshot.get('dire_score')}"


def _conversion_event_type(tower_components: list[EventComponent]) -> str | None:
    types = {component.component_type for component in tower_components}
    if types & {"THRONE_EXPOSED_COMPONENT", "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL", "T3_PLUS_T4_CHAIN"}:
        return "OBJECTIVE_CONVERSION_T4"
    if types & {"ALL_T3_TOWERS_DOWN", "MULTIPLE_T3_TOWERS_DOWN", "T3_TOWER_FALL", "MULTI_STRUCTURE_COLLAPSE"}:
        return "OBJECTIVE_CONVERSION_T3"
    if types & {"T2_TOWER_FALL", "MULTIPLE_T2_TOWERS_DOWN", "ALL_T2_TOWERS_DOWN"}:
        return "OBJECTIVE_CONVERSION_T2"
    return None


def _conversion_tower_rank(component: EventComponent) -> int:
    ranks = {
        "T2_TOWER_FALL": 1,
        "MULTIPLE_T2_TOWERS_DOWN": 2,
        "ALL_T2_TOWERS_DOWN": 3,
        "T3_TOWER_FALL": 4,
        "MULTIPLE_T3_TOWERS_DOWN": 5,
        "ALL_T3_TOWERS_DOWN": 6,
        "FIRST_T4_TOWER_FALL": 7,
        "SECOND_T4_TOWER_FALL": 8,
        "THRONE_EXPOSED_COMPONENT": 9,
        "T3_PLUS_T4_CHAIN": 10,
        "MULTI_STRUCTURE_COLLAPSE": 11,
    }
    return ranks.get(component.component_type, 0)


def _component_metadata(components: list[EventComponent]) -> dict[str, str | None]:
    if not components:
        return {
            "component_event_types": None,
            "component_deltas": None,
            "component_window_sec": None,
        }
    return {
        "component_event_types": "+".join(component.component_type for component in components),
        "component_deltas": "+".join("" if component.delta is None else str(component.delta) for component in components),
        "component_window_sec": "+".join("" if component.window_sec is None else str(component.window_sec) for component in components),
    }


def _event_dedupe_key(event: DotaEvent) -> str:
    return "|".join(str(part) for part in (
        event.match_id,
        event.event_type,
        event.direction,
        event.previous_value,
        event.current_value,
        event.delta,
        event.actual_window_sec,
    ))


def _event_quality(event: DotaEvent) -> float:
    base = float(event.base_pressure_score or 0.0)
    conversion = float(event.conversion_score or 0.0)
    fight = float(event.fight_pressure_score or 0.0)
    economy = float(event.economic_pressure_score or 0.0)
    confidence = float(event.event_confidence or 0.0)
    return (0.30 * base) + (0.20 * conversion) + (0.18 * fight) + (0.18 * economy) + (0.14 * confidence)


def _round_optional(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


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
