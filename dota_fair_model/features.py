from __future__ import annotations

from typing import Any

from .schemas import FEATURE_SCHEMA_VERSION, phase_for_duration

BASE_FEATURE_COLUMNS = [
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

MISSINGNESS_SOURCE_COLUMNS = [
    "game_time_sec",
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
    "max_respawn_timer",
    "radiant_has_aegis",
    "dire_has_aegis",
]

MISSINGNESS_COLUMNS = [f"{column}_missing" for column in MISSINGNESS_SOURCE_COLUMNS]
DEFAULT_FEATURE_COLUMNS = BASE_FEATURE_COLUMNS + MISSINGNESS_COLUMNS


def row_to_features(row: dict[str, Any], feature_columns: list[str] | None = None) -> list[float]:
    columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    feature_row = build_feature_row(row)
    return [_to_float(feature_row.get(column)) for column in columns]


def build_feature_row(row: dict[str, Any]) -> dict[str, Any]:
    derived = dict(row)
    _derive_diff(derived, "score_diff", "radiant_score", "dire_score")
    _derive_diff(derived, "net_worth_diff", "radiant_net_worth", "dire_net_worth")
    _derive_top_n_diff(derived, "top1_net_worth_diff", 1)
    _derive_top_n_diff(derived, "top2_net_worth_diff", 2)
    _derive_top_n_diff(derived, "top3_net_worth_diff", 3)
    _derive_diff(derived, "level_diff", "radiant_level", "dire_level")
    _derive_diff(derived, "gpm_diff", "radiant_gpm", "dire_gpm")
    _derive_diff(derived, "xpm_diff", "radiant_xpm", "dire_xpm")
    _derive_diff(derived, "gold_diff", "radiant_gold", "dire_gold")

    out = {column: _to_float(derived.get(column)) for column in BASE_FEATURE_COLUMNS}
    for column in MISSINGNESS_SOURCE_COLUMNS:
        out[f"{column}_missing"] = 1.0 if _is_missing(derived.get(column)) else 0.0
    out["match_id"] = str(row.get("match_id") or "")
    out["model_phase"] = phase_for_duration(row.get("game_time_sec"))
    out["feature_schema_version"] = FEATURE_SCHEMA_VERSION
    return out


def _derive_diff(row: dict[str, Any], out_key: str, left_key: str, right_key: str) -> None:
    if not _is_missing(row.get(out_key)):
        return
    left = _to_optional_float(row.get(left_key))
    right = _to_optional_float(row.get(right_key))
    if left is not None and right is not None:
        row[out_key] = left - right


def _derive_top_n_diff(row: dict[str, Any], out_key: str, n: int) -> None:
    if not _is_missing(row.get(out_key)):
        return
    radiant = _top_values_from_row(row, "radiant", n)
    dire = _top_values_from_row(row, "dire", n)
    if len(radiant) >= n and len(dire) >= n:
        row[out_key] = sum(radiant[:n]) - sum(dire[:n])


def _top_values_from_row(row: dict[str, Any], side: str, n: int) -> list[float]:
    values = []
    for idx in range(1, 6):
        value = _to_optional_float(row.get(f"{side}_p{idx}_net_worth"))
        if value is not None:
            values.append(value)
    values.sort(reverse=True)
    return values[:n]


def _is_missing(value: Any) -> bool:
    return value in (None, "")


def _to_optional_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float:
    parsed = _to_optional_float(value)
    if parsed is None:
        return 0.0
    return parsed
