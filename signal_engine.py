from __future__ import annotations

import time
from math import exp, log
from collections import deque
from dataclasses import dataclass
from typing import Any, Iterable

from team_utils import norm_team
from event_taxonomy import TIER_A_EVENTS, TIER_B_EVENTS, event_family, event_is_primary, event_tier

from config import (
    MAX_STEAM_AGE_MS, MAX_SOURCE_UPDATE_AGE_SEC, REQUIRE_TOP_LIVE_FOR_SIGNALS,
    MAX_BOOK_AGE_MS, MAX_SPREAD, MIN_LAG, MIN_EXECUTABLE_EDGE,
    PRICE_LOOKBACK_SEC, DEFAULT_MAX_FILL_PRICE,
    MIN_ASK_SIZE_USD, PAPER_TRADE_SIZE_USD, PAPER_SLIPPAGE_CENTS,
    EVENT_LEAD_SWING_30S, EVENT_LEAD_SWING_60S,
)


@dataclass(frozen=True)
class EventSpec:
    base: float
    cap: float
    half_life_sec: float
    kind: str


# Final fast-API event model. Ancient/game-over is intentionally NOT here:
# game_over/Ancient state changes are terminal handlers, not probability signals.
ACTIVE_EVENTS: dict[str, EventSpec] = {
    # Composite events are emitted when a structure falls in the same short
    # window as a same-direction kill/networth swing. They score above raw
    # structure events because the objective was converted from pressure, not
    # merely taken by split-push noise.
    "OBJECTIVE_CONVERSION_T4":   EventSpec(0.36, 0.65, 2.0,  "primary"),
    "OBJECTIVE_CONVERSION_T3":   EventSpec(0.18, 0.38, 5.0,  "primary"),
    "OBJECTIVE_CONVERSION_T2":   EventSpec(0.075,0.16, 8.0,  "primary"),

    "THRONE_EXPOSED":            EventSpec(0.35, 0.70, 1.5,  "primary"),
    "SECOND_T4_TOWER_FALL":      EventSpec(0.32, 0.65, 2.0,  "primary"),
    "FIRST_T4_TOWER_FALL":       EventSpec(0.22, 0.48, 3.0,  "primary"),
    "T3_PLUS_T4_CHAIN":          EventSpec(0.28, 0.55, 3.0,  "primary"),
    "MULTI_STRUCTURE_COLLAPSE":  EventSpec(0.22, 0.45, 4.0,  "primary"),
    "ULTRA_LATE_WIPE":           EventSpec(0.22, 0.48, 4.0,  "primary"),
    "ULTRA_LATE_WIPE_CONFIRMED": EventSpec(0.24, 0.52, 4.0,  "primary"),
    "LATE_GAME_WIPE":            EventSpec(0.15, 0.36, 5.0,  "primary"),
    "STOMP_THROW":               EventSpec(0.14, 0.36, 10.0, "primary"),
    "STOMP_THROW_WITH_OBJECTIVE_RISK": EventSpec(0.18, 0.40, 8.0, "primary"),
    "LATE_MAJOR_COMEBACK_REPRICE": EventSpec(0.16, 0.36, 8.0, "primary"),
    "CHAINED_LATE_FIGHT_RECOVERY": EventSpec(0.14, 0.32, 8.0, "primary"),
    "LATE_ECONOMIC_CRASH":       EventSpec(0.13, 0.30, 8.0,  "primary"),
    "ALL_T3_TOWERS_DOWN":        EventSpec(0.18, 0.40, 5.0,  "primary"),
    "MULTIPLE_T3_TOWERS_DOWN":   EventSpec(0.15, 0.30, 7.0,  "primary"),
    "T3_TOWER_FALL":             EventSpec(0.09, 0.22, 7.0,  "primary"),
    "MAJOR_COMEBACK":            EventSpec(0.15, 0.32, 18.0, "primary"),
    "EXTREME_LEAD_SWING_30S":   EventSpec(0.12, 0.28, 10.0, "primary"),
    "KILL_CONFIRMED_LEAD_SWING": EventSpec(0.09, 0.22, 12.0, "primary"),
    "LEAD_SWING_60S":            EventSpec(0.05, 0.12, 15.0, "primary"),
    "LEAD_SWING_30S":            EventSpec(0.06, 0.15, 15.0, "primary"),
    "KILL_BURST_30S":            EventSpec(0.025, 0.12, 6.0,  "primary"),

    # Confirmation / lower-confidence events. main.py uses cluster scoring and
    "T2_TOWER_FALL":             EventSpec(0.035, 0.08, 10.0, "confirmation"),
    "COMEBACK":                  EventSpec(0.040, 0.12, 12.0, "confirmation"),
    "FIGHT_TO_GOLD_CONFIRM_30S": EventSpec(0.050, 0.12, 6.0, "confirmation"),
    # Map-control context only: useful as support for sizing/edge, not a standalone live trigger.
    "MULTIPLE_T2_TOWERS_DOWN":   EventSpec(0.035, 0.08, 12.0, "confirmation"),
    "ALL_T2_TOWERS_DOWN":        EventSpec(0.050, 0.12, 15.0, "confirmation"),
}

PRIMARY_TRADE_EVENTS = set(TIER_A_EVENTS | TIER_B_EVENTS)

SUPPRESSIONS: dict[str, set[str]] = {
    "OBJECTIVE_CONVERSION_T4": {"FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL"},
    "OBJECTIVE_CONVERSION_T3": {"T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN"},
    "OBJECTIVE_CONVERSION_T2": {"T2_TOWER_FALL", "MULTIPLE_T2_TOWERS_DOWN", "ALL_T2_TOWERS_DOWN"},
    "ULTRA_LATE_WIPE": {"LATE_GAME_WIPE", "KILL_BURST_30S"},
    "THRONE_EXPOSED": {"SECOND_T4_TOWER_FALL", "FIRST_T4_TOWER_FALL"},
    "SECOND_T4_TOWER_FALL": {"FIRST_T4_TOWER_FALL"},
    "T3_PLUS_T4_CHAIN": {"FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL", "THRONE_EXPOSED",
                          "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN", "ALL_T3_TOWERS_DOWN"},
    "ALL_T3_TOWERS_DOWN": {"MULTIPLE_T3_TOWERS_DOWN", "T3_TOWER_FALL"},
    "MULTI_STRUCTURE_COLLAPSE": {"T2_TOWER_FALL", "T3_TOWER_FALL", "FIRST_T4_TOWER_FALL"},
    "MAJOR_COMEBACK": {"COMEBACK"},
    "LATE_MAJOR_COMEBACK_REPRICE": {"MAJOR_COMEBACK", "COMEBACK", "LEAD_SWING_60S"},
    "CHAINED_LATE_FIGHT_RECOVERY": {"KILL_CONFIRMED_LEAD_SWING", "LEAD_SWING_60S", "KILL_BURST_30S"},
    "LATE_ECONOMIC_CRASH": {"LEAD_SWING_60S", "LEAD_SWING_30S", "EXTREME_LEAD_SWING_30S"},
    "ULTRA_LATE_WIPE_CONFIRMED": {"ULTRA_LATE_WIPE", "LATE_GAME_WIPE", "KILL_BURST_30S"},
    "STOMP_THROW_WITH_OBJECTIVE_RISK": {"STOMP_THROW", "KILL_BURST_30S"},
    "FIGHT_TO_GOLD_CONFIRM_30S": {"KILL_CONFIRMED_LEAD_SWING", "KILL_BURST_30S"},
    "MULTIPLE_T3_TOWERS_DOWN": {"T3_TOWER_FALL"},
    "ALL_T2_TOWERS_DOWN": {"MULTIPLE_T2_TOWERS_DOWN", "T2_TOWER_FALL"},
    "MULTIPLE_T2_TOWERS_DOWN": {"T2_TOWER_FALL"},
}

# Event-specific safety rails. Real acceptance still comes from fair_price - ask;
# these caps only prevent chasing obviously expensive entries.
_EVENT_MAX_FILL: dict[str, float] = {
    "OBJECTIVE_CONVERSION_T4": 0.96,
    "OBJECTIVE_CONVERSION_T3": 0.88,
    "OBJECTIVE_CONVERSION_T2": 0.78,
    "THRONE_EXPOSED": 0.97,
    "SECOND_T4_TOWER_FALL": 0.96,
    "FIRST_T4_TOWER_FALL": 0.93,
    "T3_PLUS_T4_CHAIN": 0.93,
    "MULTI_STRUCTURE_COLLAPSE": 0.90,
    "ULTRA_LATE_WIPE": 0.92,
    "ULTRA_LATE_WIPE_CONFIRMED": 0.88,
    "LATE_GAME_WIPE": 0.88,
    "STOMP_THROW": 0.88,
    "STOMP_THROW_WITH_OBJECTIVE_RISK": 0.80,
    "LATE_MAJOR_COMEBACK_REPRICE": 0.70,
    "CHAINED_LATE_FIGHT_RECOVERY": 0.75,
    "LATE_ECONOMIC_CRASH": 0.75,
    "ALL_T3_TOWERS_DOWN": 0.87,
    "MULTIPLE_T3_TOWERS_DOWN": 0.85,
    "MAJOR_COMEBACK": 0.85,
    "FIGHT_TO_GOLD_CONFIRM_30S": 0.80,
    "T3_TOWER_FALL": 0.82,
}

MIN_FILL_PRICE = 0.15
MIN_GAME_TIME_SEC = 5 * 60
MAX_SIZE_MULTIPLIER = 3.0
_HISTORY_MAXLEN = 300

_LEAD_SWING_TYPES = frozenset({"LEAD_SWING_30S", "LEAD_SWING_60S", "EXTREME_LEAD_SWING_30S"})
_LEAD_SWING_THRESHOLDS = {"LEAD_SWING_30S": EVENT_LEAD_SWING_30S, "LEAD_SWING_60S": EVENT_LEAD_SWING_60S, "EXTREME_LEAD_SWING_30S": EVENT_LEAD_SWING_30S * 3}
# Keep lead swings high-severity-only when evaluated as standalone signals.
# Standalone lead swings still need high severity in live mode, but we allow
# medium in paper/shadow mode for analysis.
_HIGH_SEVERITY_ONLY = frozenset({"LEAD_SWING_30S", "LEAD_SWING_60S", "EXTREME_LEAD_SWING_30S"})
_KILL_CONFIRMED_NW_THRESHOLD = 2500
_KILL_BURST_MIN = 3


def age_ms(ns: int | None) -> int:
    if not ns:
        return 10 ** 9
    return int((time.time_ns() - ns) / 1_000_000)


def time_multiplier(game_time_sec: int | None) -> float:
    if game_time_sec is None:
        return 1.0
    minute = game_time_sec / 60.0
    if minute < 20:
        return 0.65
    if minute < 35:
        return 1.00
    if minute < 45:
        return 1.25
    if minute < 55:
        return 1.45
    return 1.70


def freshness_multiplier(age_sec: float, half_life_sec: float) -> float:
    if half_life_sec <= 0:
        return 1.0
    return 0.5 ** (max(age_sec, 0.0) / half_life_sec)


def _clip_probability(p: float) -> float:
    return min(max(float(p), 0.001), 0.999)


def _logit(p: float) -> float:
    p = _clip_probability(p)
    return log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + exp(-x))


def apply_probability_move(anchor_price: float, impact_cents: float) -> float:
    """Apply a probability shock in logit space.

    The event table is calibrated in approximate probability/cents around 50%.
    Applying it in logit space avoids impossible prices near 0/1 and produces
    more realistic edge checks than anchor + impact.
    """
    return _clip_probability(_sigmoid(_logit(anchor_price) + impact_cents * 4.0))


def _dynamic_lead_swing_threshold(event_type: str, game_time_sec: int | None) -> int:
    minute = (game_time_sec or 0) / 60.0
    if event_type == "LEAD_SWING_30S":
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


def _event_attr(event: Any, key: str, default: Any = None) -> Any:
    if isinstance(event, dict):
        return event.get(key, default)
    return getattr(event, key, default)


def _event_quality_score(events: Iterable[Any]) -> float:
    scores: list[float] = []
    for event in events:
        explicit = _event_attr(event, "event_quality")
        if explicit is not None:
            try:
                scores.append(float(explicit))
                continue
            except (TypeError, ValueError):
                pass
        base = float(_event_attr(event, "base_pressure_score", 0.0) or 0.0)
        conversion = float(_event_attr(event, "conversion_score", 0.0) or 0.0)
        fight = float(_event_attr(event, "fight_pressure_score", 0.0) or 0.0)
        economy = float(_event_attr(event, "economic_pressure_score", 0.0) or 0.0)
        scores.append((0.35 * base) + (0.25 * conversion) + (0.20 * fight) + (0.20 * economy))
    return round(max(scores, default=0.0), 4)


def _execution_quality_scores(book_age: int, spread: float | None, ask: float, ask_size: Any) -> dict[str, float]:
    book_freshness_score = max(0.0, min(1.0, 1.0 - (book_age / max(MAX_BOOK_AGE_MS, 1))))
    spread_score = 0.6 if spread is None else max(0.0, min(1.0, 1.0 - (spread / max(MAX_SPREAD, 0.0001))))
    notional = None
    try:
        if ask_size is not None:
            notional = ask * float(ask_size)
    except (TypeError, ValueError):
        notional = None
    size_score = 0.6 if notional is None else max(0.0, min(1.0, notional / max(MIN_ASK_SIZE_USD, 0.01)))
    price_not_chased_score = 0.0 if ask >= 0.97 else (0.25 if ask >= 0.95 else (0.65 if ask >= 0.90 else 1.0))
    execution_quality = book_freshness_score * spread_score * size_score * price_not_chased_score
    price_quality = spread_score * price_not_chased_score
    return {
        "book_freshness_score": round(book_freshness_score, 4),
        "spread_score": round(spread_score, 4),
        "size_score": round(size_score, 4),
        "price_not_chased_score": round(price_not_chased_score, 4),
        "price_quality_score": round(price_quality, 4),
        "execution_quality_score": round(execution_quality, 4),
    }


def apply_suppressions(events: Iterable[Any]) -> list[Any]:
    out = list(events)
    event_types = {_event_attr(e, "event_type") for e in out}
    suppressed: set[str] = set()
    for winner, losers in SUPPRESSIONS.items():
        if winner in event_types:
            suppressed.update(losers)
    return [e for e in out if _event_attr(e, "event_type") not in suppressed]


def _event_team_lead(event_direction: str, game: dict) -> int | None:
    lead = game.get("radiant_lead")
    try:
        lead = int(lead)
    except (TypeError, ValueError):
        return None
    if event_direction == "radiant":
        return lead
    if event_direction == "dire":
        return -lead
    return None


def _side_bits_for_enemy(event_direction: str, game: dict) -> int | None:
    """Return currently alive enemy structures for the event-favored side.

    GetTopLiveGame building_state is not the standard 11-bit tower_state mask.
    Do not derive structure context from it until its layout is decoded. The
    tower_state fallback only contains Radiant alive buildings, so it can only
    describe Dire-favoring attacks against Radiant.
    """
    bs = game.get("building_state")
    if bs is not None:
        try:
            bs = int(bs)
        except (TypeError, ValueError):
            bs = None
    if bs is not None:
        return None

    ts = game.get("tower_state")
    if ts is not None and event_direction == "dire":
        try:
            return int(ts) & 0x7FF
        except (TypeError, ValueError):
            return None
    return None


def _structure_context(event_direction: str, game: dict) -> dict[str, int] | None:
    bits = _side_bits_for_enemy(event_direction, game)
    if bits is None:
        return None
    t2_mask = (1 << 1) | (1 << 4) | (1 << 7)
    t3_mask = (1 << 2) | (1 << 5) | (1 << 8)
    t4_mask = (1 << 9) | (1 << 10)
    t2_alive = (bits & t2_mask).bit_count()
    t3_alive = (bits & t3_mask).bit_count()
    t4_alive = (bits & t4_mask).bit_count()
    return {
        "enemy_t2_alive": t2_alive,
        "enemy_t2_dead": 3 - t2_alive,
        "enemy_t3_alive": t3_alive,
        "enemy_t3_dead": 3 - t3_alive,
        "enemy_t4_alive": t4_alive,
        "enemy_t4_dead": 2 - t4_alive,
    }


class EventSignalEngine:
    """Event-driven latency-arb signal engine.

    The live path should call evaluate_cluster(): one primary event plus any
    same-direction confirmations becomes a single capped probability shock.
    evaluate() is kept as a backwards-compatible one-event wrapper.
    """

    def __init__(self):
        self._price_history: dict[str, deque] = {}
        self._pregame_price: dict[str, float] = {}
        self._last_signal_ms: dict[tuple[str, str, str], int] = {}

    def record_price(self, token_id: str, mid: float, game_time_sec: int | None = None):
        hist = self._price_history.setdefault(token_id, deque(maxlen=_HISTORY_MAXLEN))
        hist.append((int(time.time() * 1000), mid))

        if game_time_sec is None or game_time_sec <= 0:
            self._pregame_price[token_id] = mid
        elif token_id not in self._pregame_price:
            self._pregame_price[token_id] = mid

    def _price_n_seconds_ago(self, token_id: str, n_sec: float) -> float | None:
        hist = self._price_history.get(token_id)
        if not hist:
            return None
        cutoff_ms = int(time.time() * 1000) - int(n_sec * 1000)
        for wall_ms, price in reversed(hist):
            if wall_ms <= cutoff_ms:
                return price
        return None

    def _current_price(self, token_id: str) -> float | None:
        hist = self._price_history.get(token_id)
        return hist[-1][1] if hist else None

    def evaluate(
        self,
        event_type: str,
        event_direction: str,
        event_delta: float | None,
        game: dict,
        mapping: dict,
        yes_book: dict | None,
        no_book: dict | None,
        severity: str = "",
    ) -> dict:
        event = {
            "event_type": event_type,
            "direction": event_direction,
            "delta": event_delta,
            "severity": severity,
            "game_time_sec": game.get("game_time_sec"),
        }
        return self.evaluate_cluster(
            [event], game, mapping, yes_book, no_book,
            require_primary=False,
        )

    def evaluate_cluster(
        self,
        events: Iterable[Any],
        game: dict,
        mapping: dict,
        yes_book: dict | None,
        no_book: dict | None,
        require_primary: bool = True,
    ) -> dict:
        events = [e for e in apply_suppressions(events) if _event_attr(e, "event_type") in ACTIVE_EVENTS]
        if not events:
            return {"decision": "skip", "reason": "event_type_inactive"}

        # Choose the strongest active event's direction and discard contrary events.
        events.sort(key=lambda e: ACTIVE_EVENTS[_event_attr(e, "event_type")].base, reverse=True)
        event_direction = _event_attr(events[0], "direction") or ""
        events = [e for e in events if (_event_attr(e, "direction") or "") == event_direction]
        if not events or not event_direction:
            return {"decision": "skip", "reason": "event_direction_unknown"}

        if require_primary and not any(_event_attr(e, "event_type") in PRIMARY_TRADE_EVENTS for e in events):
            primary_event_type = _event_attr(events[0], "event_type")
            return {
                "decision": "skip",
                "reason": "no_primary_event",
                "event_type": primary_event_type,
                "event_tier": event_tier(primary_event_type),
                "event_is_primary": event_is_primary(primary_event_type),
            }

        # Standalone lead swings still need high severity.
        if not require_primary:
            for e in events:
                et = _event_attr(e, "event_type")
                if et in _HIGH_SEVERITY_ONLY and _event_attr(e, "severity", "") != "high":
                    return {"decision": "skip", "reason": "severity_too_low"}

        if mapping.get("market_type") != "MAP_WINNER":
            return {"decision": "skip", "reason": "unsupported_market_type"}

        game_time = game.get("game_time_sec")
        if game_time is not None and game_time < MIN_GAME_TIME_SEC:
            return {"decision": "skip", "reason": "game_too_early", "game_time_sec": game_time}

        steam_age = age_ms(game.get("received_at_ns"))
        if steam_age > MAX_STEAM_AGE_MS:
            return {"decision": "skip", "reason": "steam_stale", "steam_age_ms": steam_age}

        data_source = game.get("data_source")
        if REQUIRE_TOP_LIVE_FOR_SIGNALS and data_source != "top_live":
            return {
                "decision": "skip", "reason": "non_top_live_source",
                "data_source": data_source, "steam_age_ms": steam_age,
            }

        source_update_age_sec = game.get("source_update_age_sec")
        if source_update_age_sec is not None:
            try:
                source_update_age_sec = float(source_update_age_sec)
            except (TypeError, ValueError):
                source_update_age_sec = None
        if source_update_age_sec is not None and source_update_age_sec > MAX_SOURCE_UPDATE_AGE_SEC:
            return {
                "decision": "skip", "reason": "source_update_stale",
                "source_update_age_sec": round(source_update_age_sec, 3),
                "steam_age_ms": steam_age,
            }

        stream_delay_s = game.get("stream_delay_s")
        if stream_delay_s is not None:
            try:
                stream_delay_s = float(stream_delay_s)
            except (TypeError, ValueError):
                stream_delay_s = None
        # stream_delay_s is Valve's spectator/broadcast delay metadata, not proof
        # that GetTopLiveGame itself is stale. Keep it for logs/research only;
        # freshness guards must use received_at_ns, plausible last_update_time,
        # book age, and market repricing.

        yes_team = norm_team(mapping.get("yes_team"))
        radiant_team = norm_team(game.get("radiant_team"))
        dire_team = norm_team(game.get("dire_team"))

        # Primary side detection: string match against radiant/dire names
        if yes_team and radiant_team and yes_team == radiant_team:
            event_favors_yes = (event_direction == "radiant")
        elif yes_team and dire_team and yes_team == dire_team:
            event_favors_yes = (event_direction == "dire")
        else:
            # Fallback: use the robust direction saved during sync_markets
            side_map = mapping.get("steam_side_mapping")  # "normal" or "reversed"
            if side_map == "normal":
                event_favors_yes = (event_direction == "radiant")
            elif side_map == "reversed":
                event_favors_yes = (event_direction == "dire")
            else:
                return {"decision": "skip", "reason": "team_side_unknown"}

        if event_favors_yes:
            token_book = yes_book
            token_id = mapping.get("yes_token_id", "")
        else:
            token_book = no_book
            token_id = mapping.get("no_token_id", "")

        if not token_book or token_book.get("best_ask") is None:
            return {"decision": "skip", "reason": "missing_book"}

        book_age = age_ms(token_book.get("received_at_ns"))
        if book_age > MAX_BOOK_AGE_MS:
            return {"decision": "skip", "reason": "book_stale", "book_age_ms": book_age}

        ask = float(token_book["best_ask"])
        bid = token_book.get("best_bid")
        mid = (ask + float(bid)) / 2.0 if bid is not None else ask
        spread = (ask - float(bid)) if bid is not None else None
        ask_size = token_book.get("ask_size")
        primary_event_type = _event_attr(events[0], "event_type")
        event_quality = _event_quality_score(events)
        execution_scores = _execution_quality_scores(book_age, spread, ask, ask_size)

        if ask < MIN_FILL_PRICE:
            return {"decision": "skip", "reason": "fill_price_too_low", "ask": ask, "mid": mid}

        if ask >= 0.97 and primary_event_type != "THRONE_EXPOSED":
            return {
                "decision": "skip", "reason": "chasing_terminal_price",
                "ask": ask, "mid": mid, "event_type": primary_event_type,
                "event_tier": event_tier(primary_event_type), "event_family": event_family(primary_event_type),
                "event_quality": event_quality, **execution_scores,
            }
        if (
            ask >= 0.95
            and primary_event_type in {
                "OBJECTIVE_CONVERSION_T3", "OBJECTIVE_CONVERSION_T4", "MULTIPLE_T3_TOWERS_DOWN",
                "ALL_T3_TOWERS_DOWN", "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL",
                "T3_PLUS_T4_CHAIN", "MULTI_STRUCTURE_COLLAPSE", "T3_TOWER_FALL",
            }
        ):
            return {
                "decision": "skip", "reason": "priced_out_high_ground_stomp",
                "ask": ask, "mid": mid, "event_type": primary_event_type,
                "event_tier": event_tier(primary_event_type), "event_family": event_family(primary_event_type),
                "event_quality": event_quality, **execution_scores,
            }

        max_fill = _EVENT_MAX_FILL.get(primary_event_type, DEFAULT_MAX_FILL_PRICE)
        if ask > max_fill:
            return {
                "decision": "skip", "reason": "fill_price_too_high", "ask": ask, "mid": mid,
                "event_type": primary_event_type, "event_tier": event_tier(primary_event_type),
                "event_family": event_family(primary_event_type), "event_quality": event_quality,
                **execution_scores,
            }

        if primary_event_type in {"COMEBACK", "MAJOR_COMEBACK", "LATE_MAJOR_COMEBACK_REPRICE"} and spread is not None and spread > 0.08:
            return {
                "decision": "skip", "reason": "wide_spread_comeback_alert",
                "spread": spread, "ask": ask, "mid": mid, "event_type": primary_event_type,
                "event_tier": event_tier(primary_event_type), "event_family": event_family(primary_event_type),
                "event_quality": event_quality, **execution_scores,
            }

        if spread is not None and spread > MAX_SPREAD:
            return {
                "decision": "skip", "reason": "spread_too_wide", "spread": spread,
                "event_type": primary_event_type, "event_tier": event_tier(primary_event_type),
                "event_family": event_family(primary_event_type), "event_quality": event_quality,
                **execution_scores,
            }

        if ask_size is not None and ask * float(ask_size) < MIN_ASK_SIZE_USD:
            return {
                "decision": "skip", "reason": "insufficient_ask_size",
                "event_type": primary_event_type, "event_tier": event_tier(primary_event_type),
                "event_family": event_family(primary_event_type), "event_quality": event_quality,
                **execution_scores,
            }

        match_id = str(game.get("match_id") or game.get("lobby_id") or "")
        cooldown_key = (match_id, event_direction, primary_event_type)
        now_ms = int(time.time() * 1000)
        cooldown_ms = max(60_000, int(PRICE_LOOKBACK_SEC * 1000))
        if now_ms - self._last_signal_ms.get(cooldown_key, 0) < cooldown_ms:
            return {"decision": "skip", "reason": "cooldown"}

        current_price = self._current_price(token_id)
        if current_price is None:
            return {"decision": "skip", "reason": "no_price_history"}

        # 1. Repricing check: if the market already moved significantly in the
        # last 30s (Steam's typical delay window), the edge is likely gone.
        anchor_30s = self._price_n_seconds_ago(token_id, 30)
        if anchor_30s is not None:
            recent_repriced_move = current_price - anchor_30s
            # If the market moved > 75% of MIN_LAG in the event's direction, skip.
            if recent_repriced_move > (MIN_LAG * 0.75):
                return {
                    "decision": "skip", "reason": "already_repriced",
                    "move_30s": round(recent_repriced_move, 4),
                    "current_price": round(current_price, 4),
                    "anchor_30s": round(anchor_30s, 4),
                }

        anchor_price = self._price_n_seconds_ago(token_id, PRICE_LOOKBACK_SEC)
        if anchor_price is None:
            anchor_price = self._pregame_price.get(token_id)
            if anchor_price is None:
                return {"decision": "skip", "reason": "insufficient_price_history"}

        adjusted_values = [self._adjusted_event_value(e, game) for e in events]
        expected_move = self._combine_event_impacts(adjusted_values)
        expected_move = min(expected_move, self._state_cap(events, game))
        expected_move *= self._context_multiplier(events, game)

        market_move = current_price - anchor_price
        fair_price = apply_probability_move(anchor_price, expected_move)
        executable_price = min(ask + PAPER_SLIPPAGE_CENTS, 0.99)
        remaining_move = fair_price - current_price
        executable_edge = fair_price - executable_price
        lag = remaining_move

        required_edge = self._required_edge(events, game, ask, spread)
        if executable_edge < required_edge:
            return {
                "decision": "skip", "reason": "edge_too_small",
                "lag": round(lag, 4), "expected_move": round(expected_move, 4),
                "fair_price": round(fair_price, 4),
                "executable_price": round(executable_price, 4),
                "executable_edge": round(executable_edge, 4),
                "required_edge": round(required_edge, 4),
                "remaining_move": round(remaining_move, 4),
                "market_move_recent": round(market_move, 4),
                "net_edge": round(executable_edge, 4),
            }

        if remaining_move < MIN_LAG:
            return {
                "decision": "skip", "reason": "lag_too_small",
                "lag": round(lag, 4), "expected_move": round(expected_move, 4),
                "fair_price": round(fair_price, 4),
                "executable_price": round(executable_price, 4),
                "executable_edge": round(executable_edge, 4),
                "required_lag": round(MIN_LAG, 4),
                "remaining_move": round(remaining_move, 4),
                "market_move_recent": round(market_move, 4),
                "net_edge": round(executable_edge, 4),
            }

        recent_price = self._price_n_seconds_ago(token_id, 3)
        if recent_price is not None:
            recent_move = current_price - recent_price
            if recent_move < -MIN_LAG:
                return {
                    "decision": "skip", "reason": "adverse_market_move",
                    "lag": round(lag, 4), "expected_move": round(expected_move, 4),
                    "market_move_3s": round(recent_move, 4),
                }

        pregame = self._pregame_price.get(token_id)
        pregame_move = (current_price - pregame) if pregame is not None else 0.0

        size_multiplier = min(max(executable_edge, 0.0) / 0.05, MAX_SIZE_MULTIPLIER)
        if pregame is not None and pregame_move > 0.20:
            size_multiplier *= 0.5

        r_score = int(game.get("radiant_score") or 0)
        d_score = int(game.get("dire_score") or 0)
        event_kill_lead = (r_score - d_score) if event_direction == "radiant" else (d_score - r_score)
        if event_kill_lead >= 8:
            size_multiplier = min(size_multiplier * 1.25, MAX_SIZE_MULTIPLIER)
        elif event_kill_lead >= 4:
            size_multiplier = min(size_multiplier * 1.10, MAX_SIZE_MULTIPLIER)
        target_size_usd = PAPER_TRADE_SIZE_USD * size_multiplier

        cluster_event_types = [_event_attr(e, "event_type") for e in events]
        severities = [_event_attr(e, "severity", "") for e in events]
        trade_score = event_quality * execution_scores["execution_quality_score"]

        return {
            "_cooldown_key": cooldown_key,
            "_cooldown_ms": now_ms,
            "decision": "paper_buy_yes",
            "reason": "event_cluster_lag_signal" if len(events) > 1 else "event_lag_signal",
            "event_type": primary_event_type,
            "event_tier": event_tier(primary_event_type),
            "event_is_primary": event_is_primary(primary_event_type),
            "event_family": event_family(primary_event_type),
            "event_quality": event_quality,
            **execution_scores,
            "trade_score": round(trade_score, 4),
            "cluster_event_types": "+".join(cluster_event_types),
            "event_direction": event_direction,
            "token_id": token_id,
            "side": "YES" if event_favors_yes else "NO",
            "lag": round(lag, 4),
            "expected_move": round(expected_move, 4),
            "fair_price": round(fair_price, 4),
            "executable_price": round(executable_price, 4),
            "executable_edge": round(executable_edge, 4),
            "required_edge": round(required_edge, 4),
            "remaining_move": round(remaining_move, 4),
            "market_move_recent": round(market_move, 4),
            "price_lookback_sec": PRICE_LOOKBACK_SEC,
            "pregame_move": round(pregame_move, 4) if pregame is not None else None,
            "anchor_price": round(anchor_price, 4),
            "current_price": round(current_price, 4),
            "ask": ask,
            "max_fill_price": round(max_fill, 4),
            "bid": float(bid) if bid is not None else None,
            "spread": round(spread, 4) if spread is not None else None,
            "ask_size": ask_size,
            "target_size_usd": round(target_size_usd, 2),
            "size_multiplier": round(size_multiplier, 2),
            "phase_mult": time_multiplier(game_time),
            "event_kill_lead": event_kill_lead,
            "severity": "+".join([s for s in severities if s]),
            "game_time_sec": game_time,
            "steam_age_ms": steam_age,
            "source_update_age_sec": round(source_update_age_sec, 3) if source_update_age_sec is not None else None,
            "stream_delay_s": round(stream_delay_s, 3) if stream_delay_s is not None else None,
            "data_source": data_source,
            "book_age_ms": book_age,
            "book_age_at_signal_ms": book_age,
        }

    def _adjusted_event_value(self, event: Any, game: dict) -> float:
        event_type = _event_attr(event, "event_type")
        spec = ACTIVE_EVENTS[event_type]
        game_time = game.get("game_time_sec")
        value = spec.base * time_multiplier(game_time)

        event_game_time = _event_attr(event, "game_time_sec", game_time)
        age_sec = 0.0
        if game_time is not None and event_game_time is not None:
            age_sec = max(0.0, float(game_time) - float(event_game_time))
        value *= freshness_multiplier(age_sec, spec.half_life_sec)

        delta = _event_attr(event, "delta")
        if delta is not None and isinstance(delta, (int, float)):
            abs_delta = abs(float(delta))
            if event_type in _LEAD_SWING_TYPES:
                threshold = _event_attr(event, "threshold") or _dynamic_lead_swing_threshold(event_type, game_time)
                value *= min(abs_delta / float(threshold), 3.0)
            elif event_type == "COMEBACK":
                value *= min(abs_delta / 3000, 2.0)
            elif event_type == "MAJOR_COMEBACK":
                value *= min(abs_delta / 8000, 2.0)
            elif event_type in {"LATE_MAJOR_COMEBACK_REPRICE", "LATE_ECONOMIC_CRASH"}:
                value *= min(abs_delta / 10_000, 2.0)
            elif event_type in {"CHAINED_LATE_FIGHT_RECOVERY", "STOMP_THROW_WITH_OBJECTIVE_RISK"}:
                value *= min(abs_delta / 5_000, 2.0)
            elif event_type in {"KILL_CONFIRMED_LEAD_SWING", "FIGHT_TO_GOLD_CONFIRM_30S"}:
                value *= min(abs_delta / _KILL_CONFIRMED_NW_THRESHOLD, 2.0)
            elif event_type in {"KILL_BURST_30S", "ULTRA_LATE_WIPE_CONFIRMED"}:
                value *= min(abs_delta / _KILL_BURST_MIN, 2.0)
            elif event_type in {
                "T2_TOWER_FALL", "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN",
                "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL",
                "OBJECTIVE_CONVERSION_T2", "OBJECTIVE_CONVERSION_T3", "OBJECTIVE_CONVERSION_T4",
                "MULTIPLE_T2_TOWERS_DOWN", "ALL_T2_TOWERS_DOWN",
                "THRONE_EXPOSED", "ALL_T3_TOWERS_DOWN",
                "T3_PLUS_T4_CHAIN", "MULTI_STRUCTURE_COLLAPSE",
            }:
                if abs_delta > 1:
                    value *= min(abs_delta, 2.0)

        return min(value, spec.cap)

    @staticmethod
    def _combine_event_impacts(values: list[float]) -> float:
        if not values:
            return 0.0
        values = sorted(values, reverse=True)
        return values[0] + 0.25 * sum(values[1:])

    @staticmethod
    def _state_cap(events: Iterable[Any], game: dict) -> float:
        event_types = {_event_attr(e, "event_type") for e in events}
        if "THRONE_EXPOSED" in event_types:
            return 0.70
        if "OBJECTIVE_CONVERSION_T4" in event_types or "SECOND_T4_TOWER_FALL" in event_types:
            return 0.65
        if "T3_PLUS_T4_CHAIN" in event_types:
            return 0.55
        if "FIRST_T4_TOWER_FALL" in event_types:
            return 0.50
        if "MULTI_STRUCTURE_COLLAPSE" in event_types:
            return 0.45
        if "ULTRA_LATE_WIPE_CONFIRMED" in event_types:
            return 0.52
        if "ULTRA_LATE_WIPE" in event_types:
            return 0.48
        if "STOMP_THROW_WITH_OBJECTIVE_RISK" in event_types:
            return 0.40
        if "LATE_MAJOR_COMEBACK_REPRICE" in event_types:
            return 0.36
        if "CHAINED_LATE_FIGHT_RECOVERY" in event_types:
            return 0.34
        if "LATE_ECONOMIC_CRASH" in event_types:
            return 0.32
        if "OBJECTIVE_CONVERSION_T3" in event_types:
            return 0.42
        if "ALL_T3_TOWERS_DOWN" in event_types:
            return 0.40
        if "MULTIPLE_T3_TOWERS_DOWN" in event_types:
            return 0.36
        if "T3_TOWER_FALL" in event_types:
            return 0.30
        if "OBJECTIVE_CONVERSION_T2" in event_types:
            return 0.20
        if "ALL_T2_TOWERS_DOWN" in event_types:
            return 0.14
        if "MULTIPLE_T2_TOWERS_DOWN" in event_types:
            return 0.12
        game_time = game.get("game_time_sec")
        if game_time is not None and game_time >= 2400:
            return 0.22
        return 0.12

    @staticmethod
    def _context_multiplier(events: Iterable[Any], game: dict) -> float:
        """Risk-adjust the probability shock using only same-feed context.

        Tower-only moves can be split-push noise; objective-conversion events are
        stronger because the tower fall was accompanied by same-direction kills or
        net worth. Wipes/comebacks are already strong and should not be damped.
        """
        event_list = list(events)
        event_types = {_event_attr(e, "event_type") for e in event_list}
        event_direction = _event_attr(event_list[0], "direction", "") if event_list else ""
        ctx = _structure_context(event_direction, game) if event_direction else None
        event_team_lead = _event_team_lead(event_direction, game) if event_direction else None

        mult = 1.0
        if any(e.startswith("OBJECTIVE_CONVERSION_") for e in event_types):
            mult *= 1.08
            if ctx and (ctx.get("enemy_t3_dead", 0) >= 2 or ctx.get("enemy_t4_dead", 0) >= 1):
                mult *= 1.04
            if event_team_lead is not None and event_team_lead >= 8000:
                mult *= 1.03
            return min(mult, 1.18)

        raw_structure = event_types & {
            "T2_TOWER_FALL", "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN",
            "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL",
            "MULTIPLE_T2_TOWERS_DOWN", "ALL_T2_TOWERS_DOWN",
            "THRONE_EXPOSED", "ALL_T3_TOWERS_DOWN",
            "T3_PLUS_T4_CHAIN", "MULTI_STRUCTURE_COLLAPSE",
        }
        support = event_types & {
            "COMEBACK", "MAJOR_COMEBACK", "LEAD_SWING_60S", "LEAD_SWING_30S",
            "EXTREME_LEAD_SWING_30S", "KILL_CONFIRMED_LEAD_SWING",
            "KILL_BURST_30S", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE", "STOMP_THROW",
            "LATE_MAJOR_COMEBACK_REPRICE", "CHAINED_LATE_FIGHT_RECOVERY",
            "LATE_ECONOMIC_CRASH", "ULTRA_LATE_WIPE_CONFIRMED",
            "STOMP_THROW_WITH_OBJECTIVE_RISK", "FIGHT_TO_GOLD_CONFIRM_30S",
        }
        if raw_structure and not support:
            mult *= 0.82
        if "ALL_T2_TOWERS_DOWN" in event_types:
            mult *= 1.03
        if event_team_lead is not None and event_team_lead < -4000 and not (event_types & {"COMEBACK", "MAJOR_COMEBACK", "STOMP_THROW", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE", "LATE_MAJOR_COMEBACK_REPRICE", "STOMP_THROW_WITH_OBJECTIVE_RISK"}):
            mult *= 0.92
        return min(max(mult, 0.75), 1.15)

    @staticmethod
    def _required_edge(events: Iterable[Any], game: dict, ask: float, spread: float | None) -> float:
        """Dynamic edge buffer for live-fill noise and model uncertainty."""
        required = MIN_EXECUTABLE_EDGE
        event_list = list(events)
        event_types = {_event_attr(e, "event_type") for e in event_list}
        event_direction = _event_attr(event_list[0], "direction", "") if event_list else ""
        event_team_lead = _event_team_lead(event_direction, game) if event_direction else None

        if any(e.startswith("OBJECTIVE_CONVERSION_") for e in event_types):
            required += 0.005
        if event_types & {"LATE_MAJOR_COMEBACK_REPRICE", "CHAINED_LATE_FIGHT_RECOVERY", "LATE_ECONOMIC_CRASH", "STOMP_THROW_WITH_OBJECTIVE_RISK"}:
            required += 0.02
        if event_types == {"OBJECTIVE_CONVERSION_T2"} or event_types == {"T2_TOWER_FALL"}:
            required += 0.015
        if spread is not None and spread > MAX_SPREAD * 0.5:
            required += 0.005
        if ask >= 0.75:
            required += 0.005
        if "ALL_T2_TOWERS_DOWN" in event_types:
            required -= 0.002
        if event_team_lead is not None:
            if event_team_lead < -4000 and not (event_types & {"COMEBACK", "MAJOR_COMEBACK", "STOMP_THROW", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE", "LATE_MAJOR_COMEBACK_REPRICE", "STOMP_THROW_WITH_OBJECTIVE_RISK"}):
                required += 0.010
            elif event_team_lead >= 10000 and any(e.startswith("OBJECTIVE_CONVERSION_") or e.endswith("T4_TOWER_FALL") for e in event_types):
                required -= 0.003
        required = max(MIN_EXECUTABLE_EDGE, required)

        game_time = game.get("game_time_sec")
        if game_time is not None and game_time >= 45 * 60 and not (event_types & {"ULTRA_LATE_WIPE", "ULTRA_LATE_WIPE_CONFIRMED", "SECOND_T4_TOWER_FALL", "OBJECTIVE_CONVERSION_T4"}):
            required += 0.005
        return required

    def commit_signal(self, signal: dict) -> None:
        key = signal.get("_cooldown_key")
        ms = signal.get("_cooldown_ms")
        if key and ms:
            self._last_signal_ms[key] = ms
