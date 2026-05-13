from __future__ import annotations

TIER_A_EVENTS = frozenset({
    "THRONE_EXPOSED",
    "SECOND_T4_TOWER_FALL",
    "OBJECTIVE_CONVERSION_T4",
    "T3_PLUS_T4_CHAIN",
    "OBJECTIVE_CONVERSION_T3",
})

TIER_B_EVENTS = frozenset({
    "LATE_MAJOR_COMEBACK_REPRICE",
    "CHAINED_LATE_FIGHT_RECOVERY",
    "LATE_ECONOMIC_CRASH",
    "ULTRA_LATE_WIPE_CONFIRMED",
    "STOMP_THROW_WITH_OBJECTIVE_RISK",
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

RESEARCH_EVENTS = frozenset({
    "OBJECTIVE_CONVERSION_T2",
    "FIGHT_TO_GOLD_CONFIRM_30S",
    "LOW_PRICE_UNDERDOG_COUNTERPUNCH",
    "LATE_CHEAP_LEAD_SWING_REPRICE",
    "KILL_CONFIRMED_LEAD_SWING_LATE_CHEAP",
    "CORE_NETWORTH_CRASH",
    "CORE_GAP_FLIP",
    "SUPPORT_KILL_FILTER",
    "AEGIS_PUSH_WINDOW",
    "ROSHAN_SWING",
})

BLOCKING_EVENTS = frozenset({
    "PRICED_OUT_HIGH_GROUND_STOMP",
    "WIDE_SPREAD_COMEBACK_ALERT",
    "STALE_BOOK_STRONG_EVENT",
    "STALE_SOURCE_EVENT",
    "CHASING_TERMINAL_PRICE",
    "MAPPING_UNCERTAIN_EVENT",
    "DUPLICATE_MATCH_MAPPING_EVENT",
})

FIRST_LIVE_ALLOWLIST = frozenset({
    "THRONE_EXPOSED",
    "SECOND_T4_TOWER_FALL",
    "OBJECTIVE_CONVERSION_T4",
    "T3_PLUS_T4_CHAIN",
})

EVENT_FAMILY: dict[str, str] = {
    "THRONE_EXPOSED": "terminal_base",
    "SECOND_T4_TOWER_FALL": "terminal_base",
    "OBJECTIVE_CONVERSION_T4": "fight_objective_conversion",
    "T3_PLUS_T4_CHAIN": "fight_objective_conversion",
    "OBJECTIVE_CONVERSION_T3": "fight_objective_conversion",
    "OBJECTIVE_CONVERSION_T2": "research",
    "LATE_MAJOR_COMEBACK_REPRICE": "late_reversal",
    "CHAINED_LATE_FIGHT_RECOVERY": "late_reversal",
    "LATE_ECONOMIC_CRASH": "late_reversal",
    "ULTRA_LATE_WIPE_CONFIRMED": "late_reversal",
    "STOMP_THROW_WITH_OBJECTIVE_RISK": "late_reversal",
    "ALL_T3_TOWERS_DOWN": "base_pressure",
    "FIRST_T4_TOWER_FALL": "base_pressure",
    "MULTI_STRUCTURE_COLLAPSE": "base_pressure",
    "MULTIPLE_T3_TOWERS_DOWN": "base_pressure",
    "KILL_CONFIRMED_LEAD_SWING": "fight_economy_confirmation",
    "EXTREME_LEAD_SWING_30S": "fight_economy_confirmation",
    "LEAD_SWING_60S": "fight_economy_confirmation",
    "LEAD_SWING_30S": "fight_economy_confirmation",
    "KILL_BURST_30S": "fight_economy_confirmation",
    "COMEBACK": "fight_economy_confirmation",
    "T2_TOWER_FALL": "map_control_context",
    "MULTIPLE_T2_TOWERS_DOWN": "map_control_context",
    "ALL_T2_TOWERS_DOWN": "map_control_context",
}


def event_tier(event_type: str | None) -> str:
    if event_type in TIER_A_EVENTS:
        return "A"
    if event_type in TIER_B_EVENTS:
        return "B"
    if event_type in TIER_C_EVENTS:
        return "C"
    if event_type in RESEARCH_EVENTS:
        return "research"
    if event_type in BLOCKING_EVENTS:
        return "block"
    return "unknown"


def event_is_primary(event_type: str | None) -> bool:
    return event_tier(event_type) in {"A", "B"}


def event_family(event_type: str | None) -> str:
    if event_type in BLOCKING_EVENTS:
        return "blocking"
    if event_type in RESEARCH_EVENTS:
        return "research"
    return EVENT_FAMILY.get(str(event_type or ""), "unknown")


def first_live_allowed(event_type: str | None) -> bool:
    return event_type in FIRST_LIVE_ALLOWLIST
