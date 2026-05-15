from __future__ import annotations

import time
from dataclasses import dataclass

from config import (
    EXIT_TAKE_PROFIT,
    EXIT_STOP_LOSS_ABS,
    EXIT_STOP_LOSS_REL,
    EXIT_LATENCY_EDGE_SEC,
    EXIT_HORIZON_SEC,
    EXIT_HORIZON_BY_EVENT,
    MAX_HOLD_HOURS,
)


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str = ""
    reference_bid: float | None = None
    price_floor: float | None = None


def decide_live_exit(
    *,
    position,
    book: dict | None,
    game_over_match_ids: set[str],
    adverse_token_ids: set[str] | None = None,
    now_ns: int | None = None,
) -> ExitDecision:
    now_ns = now_ns or time.time_ns()
    adverse_token_ids = adverse_token_ids or set()

    if position.token_id in adverse_token_ids:
        return ExitDecision(True, "adverse_event")

    raw_bid = (book or {}).get("best_bid")
    bid = float(raw_bid) if raw_bid is not None else None

    age_sec = (now_ns - position.entry_time_ns) / 1e9

    if position.match_id in game_over_match_ids:
        return ExitDecision(True, "game_over", bid)

    if bid is None:
        if age_sec >= MAX_HOLD_HOURS * 3600:
            return ExitDecision(True, "max_hold_timeout", None)
        return ExitDecision(False)

    model_target = position.fair_price if position.fair_price > position.entry_price else None
    if model_target is None and position.expected_move > 0:
        model_target = position.entry_price + position.expected_move

    take_profit_price = min(model_target or EXIT_TAKE_PROFIT, EXIT_TAKE_PROFIT)

    stop_offset = (
        min(EXIT_STOP_LOSS_REL, position.expected_move)
        if position.expected_move > 0
        else EXIT_STOP_LOSS_REL
    )
    stop_price = max(EXIT_STOP_LOSS_ABS, position.entry_price - stop_offset)

    event_horizon = EXIT_HORIZON_BY_EVENT.get(position.event_type, EXIT_HORIZON_SEC)

    if bid >= take_profit_price:
        return ExitDecision(True, "take_profit", bid)
    if position.fair_price > 0 and bid >= position.fair_price:
        return ExitDecision(True, "model_value_exit", bid)
    if bid <= stop_price:
        return ExitDecision(True, "stop_loss", bid)
    if EXIT_LATENCY_EDGE_SEC > 0 and age_sec >= EXIT_LATENCY_EDGE_SEC:
        return ExitDecision(True, "latency_edge_timeout", bid)
    if event_horizon > 0 and age_sec >= event_horizon:
        return ExitDecision(True, "horizon", bid)
    if age_sec >= MAX_HOLD_HOURS * 3600:
        return ExitDecision(True, "max_hold_timeout", bid)

    return ExitDecision(False)
