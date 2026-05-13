from __future__ import annotations

from typing import Any

from .schemas import FEATURE_SCHEMA_VERSION, phase_for_duration

DEFAULT_FEATURE_COLUMNS = [
    "game_time_sec",
    "radiant_score",
    "dire_score",
    "score_diff",
    "radiant_tower_state",
    "dire_tower_state",
    "radiant_barracks_state",
    "dire_barracks_state",
    "radiant_net_worth",
    "dire_net_worth",
    "net_worth_diff",
    "top1_net_worth_diff",
    "top2_net_worth_diff",
    "top3_net_worth_diff",
    "level_diff",
    "gpm_diff",
    "xpm_diff",
    "gold_diff",
    "radiant_dead_count",
    "dire_dead_count",
    "radiant_core_dead_count",
    "dire_core_dead_count",
    "max_respawn_timer",
    "radiant_has_aegis",
    "dire_has_aegis",
]


def row_to_features(row: dict[str, Any], feature_columns: list[str] | None = None) -> list[float]:
    columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    return [_to_float(row.get(column)) for column in columns]


def build_feature_row(row: dict[str, Any]) -> dict[str, Any]:
    out = {column: _to_float(row.get(column)) for column in DEFAULT_FEATURE_COLUMNS}
    out["match_id"] = str(row.get("match_id") or "")
    out["model_phase"] = phase_for_duration(row.get("game_time_sec"))
    out["feature_schema_version"] = FEATURE_SCHEMA_VERSION
    return out


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

