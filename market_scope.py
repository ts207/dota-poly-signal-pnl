from __future__ import annotations

from typing import Any


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_game3_match_proxy(mapping: dict) -> bool:
    """True when MATCH_WINNER is mathematically equivalent to current map winner.

    BO3 Game 3 at 1-1:
      match winner probability == current map winner probability.
    """
    if str(mapping.get("market_type") or "").upper() != "MATCH_WINNER":
        return False

    series_type = _to_int(mapping.get("series_type"))
    game_number = (
        _to_int(mapping.get("current_game_number"))
        or _to_int(mapping.get("game_number"))
        or _to_int(mapping.get("game_number_in_series"))
    )
    score_yes = _to_int(mapping.get("series_score_yes"))
    score_no = _to_int(mapping.get("series_score_no"))

    return (
        series_type == 1
        and game_number == 3
        and score_yes == 1
        and score_no == 1
    )


def is_active_strategy_mapping(
    mapping: dict,
    *,
    enable_match_winner_game3_proxy: bool,
    enable_match_winner_research: bool = False,
) -> bool:
    market_type = str(mapping.get("market_type") or "").upper()

    if market_type == "MAP_WINNER":
        return True

    if market_type == "MATCH_WINNER":
        if enable_match_winner_research:
            return True
        return enable_match_winner_game3_proxy and is_game3_match_proxy(mapping)

    return False


def market_scope_metadata(mapping: dict) -> dict:
    if is_game3_match_proxy(mapping):
        return {
            "proxy_market_type": "MATCH_WINNER_AS_MAP3",
            "is_game3_match_proxy": True,
        }
    return {
        "proxy_market_type": "",
        "is_game3_match_proxy": False,
    }
