from __future__ import annotations

EVENT_SCHEMA_VERSION = "cadence_v1"

TIER_A_EVENTS = frozenset({
    "OBJECTIVE_CONVERSION_T4",
    "THRONE_EXPOSED",
    "POLL_ULTRA_LATE_FIGHT_FLIP",
})

TIER_B_EVENTS = frozenset({
    "OBJECTIVE_CONVERSION_T3",
    "BASE_PRESSURE_T4",
    "BASE_PRESSURE_T3_COLLAPSE",
    "POLL_STOMP_THROW_CONFIRMED",
    "POLL_LATE_FIGHT_FLIP",
    "POLL_LEAD_FLIP_WITH_KILLS",
    "POLL_MAJOR_COMEBACK_RECOVERY",
    "POLL_KILL_BURST_CONFIRMED",
    "POLL_FIGHT_SWING",
    "POLL_COMEBACK_RECOVERY",
})

TIER_C_EVENTS = frozenset()

RESEARCH_EVENTS = frozenset({
    "OBJECTIVE_CONVERSION_T2",
    "BLOODY_EVEN_FIGHT",
    "ECON_ONLY_MOVE",
    "STRUCTURE_CONTEXT",
    "LOW_PRICE_UNDERDOG_COUNTERPUNCH",
    "LATE_CHEAP_LEAD_SWING_REPRICE",
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

RETIRED_FIXED_WINDOW_EVENTS = frozenset({
    "LEAD_SWING_30S",
    "LEAD_SWING_60S",
    "EXTREME_LEAD_SWING_30S",
    "TEAMFIGHT_SWING_30S",
    "KILL_BURST_30S",
    "KILL_CONFIRMED_LEAD_SWING",
    "FIGHT_TO_GOLD_CONFIRM_30S",
    "LATE_GAME_WIPE",
    "ULTRA_LATE_WIPE",
    "STOMP_THROW",
    "COMEBACK",
    "COMEBACK_RECOVERY_60S",
    "MAJOR_COMEBACK",
    "MAJOR_COMEBACK_RECOVERY_60S",
    "LATE_MAJOR_COMEBACK_REPRICE",
    "CHAINED_LATE_FIGHT_RECOVERY",
    "LATE_ECONOMIC_CRASH",
    "ULTRA_LATE_WIPE_CONFIRMED",
    "STOMP_THROW_WITH_OBJECTIVE_RISK",
    "T2_TOWER_FALL",
    "T3_TOWER_FALL",
    "MULTIPLE_T2_TOWERS_DOWN",
    "ALL_T2_TOWERS_DOWN",
    "MULTIPLE_T3_TOWERS_DOWN",
    "ALL_T3_TOWERS_DOWN",
    "FIRST_T4_TOWER_FALL",
    "SECOND_T4_TOWER_FALL",
    "T3_PLUS_T4_CHAIN",
    "MULTI_STRUCTURE_COLLAPSE",
    "BLOODY_EVEN_FIGHT_30S",
})

FIRST_LIVE_ALLOWLIST = frozenset({
    "THRONE_EXPOSED",
    "OBJECTIVE_CONVERSION_T4",
    "POLL_ULTRA_LATE_FIGHT_FLIP",
})

EVENT_FAMILY: dict[str, str] = {
    "OBJECTIVE_CONVERSION_T4": "fight_objective_conversion",
    "OBJECTIVE_CONVERSION_T3": "fight_objective_conversion",
    "OBJECTIVE_CONVERSION_T2": "research",
    "THRONE_EXPOSED": "terminal_base",
    "BASE_PRESSURE_T4": "terminal_base",
    "BASE_PRESSURE_T3_COLLAPSE": "base_pressure",
    "POLL_ULTRA_LATE_FIGHT_FLIP": "late_reversal",
    "POLL_STOMP_THROW_CONFIRMED": "late_reversal",
    "POLL_LATE_FIGHT_FLIP": "late_reversal",
    "POLL_LEAD_FLIP_WITH_KILLS": "late_reversal",
    "POLL_MAJOR_COMEBACK_RECOVERY": "late_reversal",
    "POLL_COMEBACK_RECOVERY": "late_reversal",
    "POLL_KILL_BURST_CONFIRMED": "fight_economy_confirmation",
    "POLL_FIGHT_SWING": "fight_economy_confirmation",
    "BLOODY_EVEN_FIGHT": "teamfight_context",
    "ECON_ONLY_MOVE": "research",
    "STRUCTURE_CONTEXT": "research",
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
    if event_type in RETIRED_FIXED_WINDOW_EVENTS:
        return "retired"
    return "unknown"


def event_is_primary(event_type: str | None) -> bool:
    return event_tier(event_type) in {"A", "B"}


def event_family(event_type: str | None) -> str:
    if event_type in BLOCKING_EVENTS:
        return "blocking"
    if event_type in RESEARCH_EVENTS:
        return "research"
    if event_type in RETIRED_FIXED_WINDOW_EVENTS:
        return "retired"
    return EVENT_FAMILY.get(str(event_type or ""), "unknown")


def first_live_allowed(event_type: str | None) -> bool:
    return event_type in FIRST_LIVE_ALLOWLIST
