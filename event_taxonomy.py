from __future__ import annotations

TIER_A_EVENTS = frozenset({
    "THRONE_EXPOSED",
    "SECOND_T4_TOWER_FALL",
    "OBJECTIVE_CONVERSION_T4",
    "T3_PLUS_T4_CHAIN",
    "OBJECTIVE_CONVERSION_T3",
})

TIER_B_EVENTS = frozenset({
    "ULTRA_LATE_WIPE",
    "LATE_GAME_WIPE",
    "STOMP_THROW",
    "MAJOR_COMEBACK",
    "FIRST_T4_TOWER_FALL",
    "ALL_T3_TOWERS_DOWN",
    "MULTI_STRUCTURE_COLLAPSE",
    "MULTIPLE_T3_TOWERS_DOWN",
})

TIER_C_EVENTS = frozenset({
    "KILL_CONFIRMED_LEAD_SWING",
    "EXTREME_LEAD_SWING_30S",
    "LEAD_SWING_60S",
    "LEAD_SWING_30S",
    "KILL_BURST_30S",
    "T2_TOWER_FALL",
    "MULTIPLE_T2_TOWERS_DOWN",
    "ALL_T2_TOWERS_DOWN",
    "COMEBACK",
})


def event_tier(event_type: str | None) -> str:
    if event_type in TIER_A_EVENTS:
        return "A"
    if event_type in TIER_B_EVENTS:
        return "B"
    if event_type in TIER_C_EVENTS:
        return "C"
    return "unknown"


def event_is_primary(event_type: str | None) -> bool:
    return event_tier(event_type) in {"A", "B"}
