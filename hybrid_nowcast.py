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
    game_time_sec: int | None = None,
    event_direction: str | None = None,
) -> HybridNowcast:
    """Fair-value combiner using RealtimeStats as the slow anchor.

    The slow_model_fair is derived from 120s delayed GetRealtimeStats features.
    We apply fast event adjustments from GetTopLiveGame (0s delayed) as residuals.
    Additionally, we calculate 'lead drift' — the change in Radiant Lead between
    the 120s old base and the current 0s snapshot — and apply it as a smooth 
    probability adjustment.
    """
    source_delay_metrics = source_delay_metrics or {}
    lag = source_delay_metrics.get("game_time_lag_sec")
    if lag is None and latest_liveleague_features and latest_toplive_snapshot:
        llg_gt = latest_liveleague_features.get("game_time_sec")
        top_gt = latest_toplive_snapshot.get("game_time_sec")
        if llg_gt is not None and top_gt is not None:
            lag = top_gt - llg_gt

    usage = classify_liveleague_lag(lag)
    has_ml = slow_model_fair is not None
    base = slow_model_fair if has_ml else event_only_fair

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
    
    # Advanced Nowcasting Layer: Non-Linear Lead Drift with Phase Elasticity
    drift_adj = 0.0
    if latest_toplive_snapshot:
        top_lead = latest_toplive_snapshot.get("radiant_lead")
        rt_lead = latest_toplive_snapshot.get("realtime_lead_nw")
        if top_lead is not None and rt_lead is not None:
            radiant_drift = float(top_lead - rt_lead)
            direction = event_direction or (_event_attr(events[0], "direction", None) if events else None)
            drift = -radiant_drift if direction == "dire" else radiant_drift
            
            # 1. Gold elasticity: probability points per 1k gold.
            # Baseline is 1% per 1k and decays as the game gets later.
            minute = (game_time_sec or 1800) / 60.0
            elasticity_per_1k = 0.01 * (0.5 ** (minute / 45.0))
            
            # 2. Diminishing Returns (Square-root scaling for extreme swings)
            # Small drifts (<3k) are linear; larger drifts use root-scaling.
            if abs(drift) < 3000:
                raw_drift_move = (drift / 1000.0) * elasticity_per_1k
            else:
                # First 3k is linear; extra drift has diminishing returns.
                sign = 1.0 if drift > 0 else -1.0
                extra_1k_equiv = ((abs(drift) - 3000) ** 0.5) / (1000 ** 0.5)
                raw_drift_move = sign * (3.0 + extra_1k_equiv) * elasticity_per_1k
            
            # 3. Buyback Damping (Inferred)
            # If the drift is massive but the drift-team has multiple deaths in RealtimeStats,
            # they likely bought back (spent gold to stop the bleeding).
            dead_r = latest_liveleague_features.get("radiant_dead_count", 0) if latest_liveleague_features else 0
            dead_d = latest_liveleague_features.get("dire_dead_count", 0) if latest_liveleague_features else 0
            if (drift > 2000 and dead_r >= 2) or (drift < -2000 and dead_d >= 2):
                raw_drift_move *= 0.65  # Dampen by 35% to account for buyback expenditure
                
            drift_adj = min(max(raw_drift_move, -0.15), 0.15)

    penalty = _uncertainty_penalty(lag)
    confidence = _confidence(usage, events)

    raw_event_total = fast_adj + structure_adj + fight_adj + economy_adj + aegis_adj + drift_adj

    if has_ml:
        ml_dampened_total = _ml_residual_adjustment(
            base=base,
            raw_adj=raw_event_total,
            events=events,
            game_time_sec=game_time_sec,
        )
        fair = _clip_probability(base + ml_dampened_total - penalty)
    else:
        fair = _clip_probability(base + raw_event_total - penalty)

    return HybridNowcast(
        slow_model_fair=slow_model_fair,
        fast_event_adjustment=round(raw_event_total if not has_ml else ml_dampened_total, 4),
        hybrid_fair=round(fair, 4),
        hybrid_confidence=round(confidence, 4),
        uncertainty_penalty=round(penalty, 4),
        liveleague_usage=usage,
    )


def _ml_residual_adjustment(
    base: float,
    raw_adj: float,
    events: list[Any],
    game_time_sec: int | None
) -> float:
    """Dampens event shocks in extreme probability regions and scales by phase."""
    # Probability space damping: 4*p*(1-p) makes adjustments smaller near 0/1
    # where the model is already very certain.
    dampening = 4.0 * base * (1.0 - base)
    
    # Phase scaling: Ultra-late game events have higher impact variance
    phase_scale = 1.0
    if game_time_sec is not None:
        if game_time_sec > 3000:   # 50min+ (Ultra Late)
            phase_scale = 1.5
        elif game_time_sec > 1800: # 30min+ (Late)
            phase_scale = 1.2
            
    return raw_adj * dampening * phase_scale


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
