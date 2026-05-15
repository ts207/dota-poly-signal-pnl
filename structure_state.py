from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SIDE_MASK = 0x7FF

T1_MASK = (1 << 0) | (1 << 3) | (1 << 6)
T2_MASK = (1 << 1) | (1 << 4) | (1 << 7)
T3_MASK = (1 << 2) | (1 << 5) | (1 << 8)
T4_MASK = (1 << 9) | (1 << 10)


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _count(bits: int, mask: int) -> int:
    return int((bits & mask).bit_count())


@dataclass(frozen=True)
class StructureState:
    match_id: str
    game_time_sec: int | None
    source_field: str
    schema: str
    raw_value: int | None

    radiant_t1_alive: int | None
    radiant_t2_alive: int | None
    radiant_t3_alive: int | None
    radiant_t4_alive: int | None

    dire_t1_alive: int | None
    dire_t2_alive: int | None
    dire_t3_alive: int | None
    dire_t4_alive: int | None

    confidence: float
    reason: str = ""

    def total_alive(self) -> int | None:
        vals = [
            self.radiant_t1_alive,
            self.radiant_t2_alive,
            self.radiant_t3_alive,
            self.radiant_t4_alive,
            self.dire_t1_alive,
            self.dire_t2_alive,
            self.dire_t3_alive,
            self.dire_t4_alive,
        ]
        if any(v is None for v in vals):
            return None
        return int(sum(vals))


@dataclass(frozen=True)
class StructureDelta:
    valid: bool
    reason: str
    source_field: str
    schema: str
    confidence: float

    radiant_t2_before: int | None = None
    radiant_t2_after: int | None = None
    radiant_t3_before: int | None = None
    radiant_t3_after: int | None = None
    radiant_t4_before: int | None = None
    radiant_t4_after: int | None = None

    dire_t2_before: int | None = None
    dire_t2_after: int | None = None
    dire_t3_before: int | None = None
    dire_t3_after: int | None = None
    dire_t4_before: int | None = None
    dire_t4_after: int | None = None

    radiant_t2_fallen: int = 0
    radiant_t3_fallen: int = 0
    radiant_t4_fallen: int = 0

    dire_t2_fallen: int = 0
    dire_t3_fallen: int = 0
    dire_t4_fallen: int = 0


def decode_structure_state(snapshot: dict) -> StructureState:
    match_id = str(snapshot.get("match_id") or snapshot.get("lobby_id") or "")
    game_time_sec = _to_int(snapshot.get("game_time_sec"))

    tower_state = _to_int(snapshot.get("tower_state"))
    if tower_state is None:
        if snapshot.get("building_state") not in (None, ""):
            return StructureState(
                match_id=match_id,
                game_time_sec=game_time_sec,
                source_field="building_state",
                schema="building_unknown",
                raw_value=_to_int(snapshot.get("building_state")),
                radiant_t1_alive=None,
                radiant_t2_alive=None,
                radiant_t3_alive=None,
                radiant_t4_alive=None,
                dire_t1_alive=None,
                dire_t2_alive=None,
                dire_t3_alive=None,
                dire_t4_alive=None,
                confidence=0.0,
                reason="building_state_not_decoded",
            )

        return StructureState(
            match_id=match_id,
            game_time_sec=game_time_sec,
            source_field="none",
            schema="missing",
            raw_value=None,
            radiant_t1_alive=None,
            radiant_t2_alive=None,
            radiant_t3_alive=None,
            radiant_t4_alive=None,
            dire_t1_alive=None,
            dire_t2_alive=None,
            dire_t3_alive=None,
            dire_t4_alive=None,
            confidence=0.0,
            reason="missing_tower_state",
        )

    radiant_bits = tower_state & SIDE_MASK
    dire_bits = (tower_state >> 11) & SIDE_MASK

    return StructureState(
        match_id=match_id,
        game_time_sec=game_time_sec,
        source_field="tower_state",
        schema="tower_22bit_v1",
        raw_value=tower_state,
        radiant_t1_alive=_count(radiant_bits, T1_MASK),
        radiant_t2_alive=_count(radiant_bits, T2_MASK),
        radiant_t3_alive=_count(radiant_bits, T3_MASK),
        radiant_t4_alive=_count(radiant_bits, T4_MASK),
        dire_t1_alive=_count(dire_bits, T1_MASK),
        dire_t2_alive=_count(dire_bits, T2_MASK),
        dire_t3_alive=_count(dire_bits, T3_MASK),
        dire_t4_alive=_count(dire_bits, T4_MASK),
        confidence=1.0,
    )


def diff_structure_state(prev: StructureState, cur: StructureState) -> StructureDelta:
    if prev.match_id != cur.match_id:
        return _invalid(cur, "match_id_changed")

    if prev.game_time_sec is not None and cur.game_time_sec is not None:
        if cur.game_time_sec < prev.game_time_sec:
            return _invalid(cur, "game_time_moved_backward")

    if prev.source_field != cur.source_field or prev.schema != cur.schema:
        return _invalid(cur, "structure_schema_changed")

    if prev.confidence < 1.0 or cur.confidence < 1.0:
        return _invalid(cur, cur.reason or prev.reason or "low_structure_confidence")

    prev_total = prev.total_alive()
    cur_total = cur.total_alive()
    if prev_total is not None and cur_total is not None and cur_total > prev_total:
        return _invalid(cur, "structure_count_increased")

    radiant_t2_fallen = _fallen(prev.radiant_t2_alive, cur.radiant_t2_alive)
    radiant_t3_fallen = _fallen(prev.radiant_t3_alive, cur.radiant_t3_alive)
    radiant_t4_fallen = _fallen(prev.radiant_t4_alive, cur.radiant_t4_alive)

    dire_t2_fallen = _fallen(prev.dire_t2_alive, cur.dire_t2_alive)
    dire_t3_fallen = _fallen(prev.dire_t3_alive, cur.dire_t3_alive)
    dire_t4_fallen = _fallen(prev.dire_t4_alive, cur.dire_t4_alive)

    if radiant_t4_fallen and prev.radiant_t3_alive == 3:
        return _invalid(cur, "radiant_t4_fell_while_all_t3_alive")
    if dire_t4_fallen and prev.dire_t3_alive == 3:
        return _invalid(cur, "dire_t4_fell_while_all_t3_alive")

    if not any([
        radiant_t2_fallen, radiant_t3_fallen, radiant_t4_fallen,
        dire_t2_fallen, dire_t3_fallen, dire_t4_fallen,
    ]):
        return _invalid(cur, "no_tower_delta")

    return StructureDelta(
        valid=True,
        reason="ok",
        source_field=cur.source_field,
        schema=cur.schema,
        confidence=min(prev.confidence, cur.confidence),

        radiant_t2_before=prev.radiant_t2_alive,
        radiant_t2_after=cur.radiant_t2_alive,
        radiant_t3_before=prev.radiant_t3_alive,
        radiant_t3_after=cur.radiant_t3_alive,
        radiant_t4_before=prev.radiant_t4_alive,
        radiant_t4_after=cur.radiant_t4_alive,

        dire_t2_before=prev.dire_t2_alive,
        dire_t2_after=cur.dire_t2_alive,
        dire_t3_before=prev.dire_t3_alive,
        dire_t3_after=cur.dire_t3_alive,
        dire_t4_before=prev.dire_t4_alive,
        dire_t4_after=cur.dire_t4_alive,

        radiant_t2_fallen=radiant_t2_fallen,
        radiant_t3_fallen=radiant_t3_fallen,
        radiant_t4_fallen=radiant_t4_fallen,

        dire_t2_fallen=dire_t2_fallen,
        dire_t3_fallen=dire_t3_fallen,
        dire_t4_fallen=dire_t4_fallen,
    )


def _fallen(before: int | None, after: int | None) -> int:
    if before is None or after is None:
        return 0
    return max(0, before - after)


def _invalid(cur: StructureState, reason: str) -> StructureDelta:
    return StructureDelta(
        valid=False,
        reason=reason,
        source_field=cur.source_field,
        schema=cur.schema,
        confidence=cur.confidence,
    )
