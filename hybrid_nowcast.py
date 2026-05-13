from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Iterable

from liveleague_features import classify_liveleague_lag


@dataclass(frozen=True)
class HybridNowcast:
    slow_model_fair: float | None
    fast_event_adjustment: float
    hybrid_fair: float | None
    hybrid_confidence: float
    uncertainty_penalty: float
    liveleague_usage: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clip_probability(value: float) -> float:
    return min(max(float(value), 0.001), 0.999)


def _event_attr(event: Any, key: str, default: Any = None) -> Any:
    if isinstance(event, dict):
        return event.get(key, default)
    return getattr(event, key, default)


def compute_hybrid_nowcast(
    *,
    latest_liveleague_features: dict | None,
    latest_toplive_snapshot: dict | None,
    toplive_event_cluster: Iterable[Any] | None,
    source_delay_metrics: dict | None,
    slow_model_fair: float | None = None,
    event_only_fair: float | None = None,
) -> HybridNowcast:
    """Shadow-only fair-value combiner.

    This intentionally returns diagnostics only. It does not place trades or
    override the existing event engine until latency/markout evidence justifies it.
    """
    source_delay_metrics = source_delay_metrics or {}
    lag = source_delay_metrics.get("game_time_lag_sec")
    if lag is None and latest_liveleague_features and latest_toplive_snapshot:
        llg_gt = latest_liveleague_features.get("game_time_sec")
        top_gt = latest_toplive_snapshot.get("game_time_sec")
        if llg_gt is not None and top_gt is not None:
            lag = top_gt - llg_gt

    usage = classify_liveleague_lag(lag)
    base = slow_model_fair if slow_model_fair is not None else event_only_fair
    if base is None:
        return HybridNowcast(
            slow_model_fair=slow_model_fair,
            fast_event_adjustment=0.0,
            hybrid_fair=None,
            hybrid_confidence=0.0,
            uncertainty_penalty=_uncertainty_penalty(lag),
            liveleague_usage=usage,
        )

    events = list(toplive_event_cluster or [])
    fast_adj = _fast_event_adjustment(events)
    structure_adj = _structure_adjustment(events)
    fight_adj = _fight_adjustment(events)
    economy_adj = _economy_adjustment(events)
    aegis_adj = _aegis_adjustment(latest_liveleague_features, events)
    penalty = _uncertainty_penalty(lag)
    confidence = _confidence(usage, events)

    fair = _clip_probability(base + fast_adj + structure_adj + fight_adj + economy_adj + aegis_adj - penalty)
    return HybridNowcast(
        slow_model_fair=slow_model_fair,
        fast_event_adjustment=round(fast_adj + structure_adj + fight_adj + economy_adj + aegis_adj, 4),
        hybrid_fair=round(fair, 4),
        hybrid_confidence=round(confidence, 4),
        uncertainty_penalty=round(penalty, 4),
        liveleague_usage=usage,
    )


def _fast_event_adjustment(events: list[Any]) -> float:
    confidence_values = []
    for event in events:
        conf = _event_attr(event, "event_confidence")
        try:
            confidence_values.append(float(conf))
        except (TypeError, ValueError):
            pass
    if not confidence_values:
        return 0.0
    return min(sum(confidence_values) * 0.01, 0.04)


def _structure_adjustment(events: list[Any]) -> float:
    event_types = {_event_attr(event, "event_type", "") for event in events}
    if "THRONE_EXPOSED" in event_types:
        return 0.12
    if "SECOND_T4_TOWER_FALL" in event_types or "OBJECTIVE_CONVERSION_T4" in event_types:
        return 0.08
    if "FIRST_T4_TOWER_FALL" in event_types or "T3_PLUS_T4_CHAIN" in event_types:
        return 0.05
    if "OBJECTIVE_CONVERSION_T3" in event_types or "ALL_T3_TOWERS_DOWN" in event_types:
        return 0.035
    return 0.0


def _fight_adjustment(events: list[Any]) -> float:
    values = []
    for event in events:
        score = _event_attr(event, "fight_pressure_score")
        try:
            values.append(float(score))
        except (TypeError, ValueError):
            pass
    return min(max(values or [0.0]) * 0.04, 0.04)


def _economy_adjustment(events: list[Any]) -> float:
    values = []
    for event in events:
        score = _event_attr(event, "economic_pressure_score")
        try:
            values.append(float(score))
        except (TypeError, ValueError):
            pass
    return min(max(values or [0.0]) * 0.04, 0.04)


def _aegis_adjustment(features: dict | None, events: list[Any]) -> float:
    if not features or not events:
        return 0.0
    direction = _event_attr(events[0], "direction")
    if features.get("aegis_team") == direction:
        return 0.015
    return 0.0


def _uncertainty_penalty(lag: Any) -> float:
    try:
        lag = float(lag)
    except (TypeError, ValueError):
        return 0.05
    if lag <= 10:
        return 0.0
    if lag <= 60:
        return min((lag - 10) / 50 * 0.04, 0.04)
    return min(0.04 + (lag - 60) / 120 * 0.06, 0.10)


def _confidence(usage: str, events: list[Any]) -> float:
    base = {"direct": 0.8, "prior": 0.55, "background": 0.35, "unknown": 0.25}.get(usage, 0.25)
    if any(str(_event_attr(e, "event_type", "")).startswith("OBJECTIVE_CONVERSION_") for e in events):
        base += 0.1
    if any(_event_attr(e, "event_type") in {"THRONE_EXPOSED", "SECOND_T4_TOWER_FALL"} for e in events):
        base += 0.1
    return min(base, 1.0)
