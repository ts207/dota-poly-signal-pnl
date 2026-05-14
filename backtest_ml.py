"""ML-enhanced event-driven backtest against real Polymarket order book data.

Extends backtest.py by adding:
  1. Per-snapshot ML model fair price (dota_fair_model) alongside heuristic fair price
  2. Side-by-side accuracy comparison: heuristic vs ML vs hybrid
  3. Phase-stratified analysis (early/mid/late/ultra_late)
  4. Ultra-late game deep-dive (50min+)

The ML model provides a calibrated "slow" fair value. Events provide "fast"
adjustments. The hybrid approach uses the ML model as the anchor instead of
the heuristic event-only expected_move, and only fires fast adjustments when
the event significantly deviates from the model's expectation.

Usage:
    python3 backtest_ml.py [--lag 0.05] [--size 25] [--exit 30]
    python3 backtest_ml.py --ultra-late-only
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, fields
from math import log, exp
from pathlib import Path

from dota_fair_model.inference import FairModelBundle, load_bundle
from dota_fair_model.features import DEFAULT_FEATURE_COLUMNS, build_feature_row
from dota_fair_model.schemas import phase_for_duration
from event_detector import EventDetector
from signal_engine import ACTIVE_EVENTS
from config import EVENT_LEAD_SWING_30S, EVENT_LEAD_SWING_60S, DOTA_FAIR_MODEL_PATH

from hybrid_nowcast import compute_hybrid_nowcast

DATA_DIR = "/home/irene/dota_poly_bot_final/data"
PAPER_SIZE_USD = 25.0

SEGMENTS = [
    {
        "label": "Carstensz vs TEAM GRIND",
        "db": f"{DATA_DIR}/dota_poly_collection.sqlite",
        "match_key": "90285607589477394",
        "radiant_token": "90268231449155282246853972144583742931465600097997027484803301961579288855144",
        "dire_token":    "63987300715693577866871042327158392402412511432457971527449850967203600386804",
        "radiant_win": 0,
    },
    {
        "label": "PlayTime vs 1w Team",
        "db": f"{DATA_DIR}/dota_poly_collection.sqlite",
        "match_key": "90285599503423511_m1",
        "radiant_token": "13478386926402301406532136263977204904714000287949507563856704721767290839044",
        "dire_token":    "63310461820786146813035795297817607012343337186334700939384155278400607390107",
        "radiant_win": 0,
    },
    {
        "label": "Two Move vs Team Lynx",
        "db": f"{DATA_DIR}/lynx_tm6_collection.sqlite",
        "match_key": "90285619707346954_m1",
        "radiant_token": "34976881449444734178409311723175251004867357634324791603190926689290262342977",
        "dire_token":    "4452564105200725346521605963781468915677428239106200674543104246242028910211",
        "radiant_win": 0,
    },
    {
        "label": "1w Team vs PlayTime (G2)",
        "db": f"{DATA_DIR}/1win_ptime_g2.sqlite",
        "match_key": "90285618797931526_m1",
        "radiant_token": "44042712276170069650224504201935395716816628269726746518447030840697274440699",
        "dire_token":    "33812820765629339713713753007847781463087366023932766960858881005889078629256",
        "radiant_win": 1,
    },
    {
        "label": "PARIVISION vs 1w Team (G1)",
        "db": f"{DATA_DIR}/1win_pari_g1.sqlite",
        "match_key": "90285623272207384_m1",
        "radiant_token": "70347395524393779469493680391299369304316720284512794724445180423011761114165",
        "dire_token":    "74998310881290739392918170902879306286233744638879268738919090905932120366324",
        "radiant_win": 1,
    },
    {
        "label": "1w vs PARIVISION (G2)",
        "db": f"{DATA_DIR}/1win_pari_g2.sqlite",
        "match_key": "90285627567738905_m1",
        "radiant_token": "47625441297314461057077645727754264216244555280948560109804310553137263770263",
        "dire_token":    "57026843702394568915654659851749981463087366023932051302086816685884289545931",  
        "radiant_win": 0,
    },
    {
        "label": "PARIVISION vs 1w Team (G3)",
        "db": f"{DATA_DIR}/1win_pari_g3.sqlite",
        "match_key": "90285630522125338_m1",
        "radiant_token": "39003960489463622267960758033997733112117778420142043276598674855608515962197",
        "dire_token":    "14082266884467670274043702622681498600675941864859148613356376458056888253905",
        "radiant_win": 1,
    },
]

EVENT_EXPECTED_MOVE = {name: spec.base for name, spec in ACTIVE_EVENTS.items()}
_HIGH_SEVERITY_ONLY = frozenset({"LEAD_SWING_30S", "LEAD_SWING_60S"})


@dataclass
class MLTrade:
    label: str
    event_type: str
    direction: str
    severity: str
    game_time_sec: int
    wall_ts_ms: int
    side: str
    fill: float
    pre_game_price: float
    price_at_event: float
    heuristic_expected_move: float
    heuristic_lag: float
    ml_fair_radiant: float | None
    ml_fair_yes: float | None
    hybrid_fair: float | None
    ml_phase: str
    actual_move: float
    radiant_win: int
    pnl_15s: float | None = None
    pnl_30s: float | None = None
    pnl_60s: float | None = None
    pnl_term: float | None = None
    pnl_ml_15s: float | None = None
    pnl_ml_30s: float | None = None
    pnl_ml_60s: float | None = None
    pnl_ml_term: float | None = None
    pnl_hybrid_15s: float | None = None
    pnl_hybrid_30s: float | None = None
    pnl_hybrid_60s: float | None = None
    pnl_hybrid_term: float | None = None
    ml_edge: float | None = None
    hybrid_edge: float | None = None


def _load_dota(db: sqlite3.Connection, match_key: str, start_ms: int, end_ms: int) -> list[dict]:
    rows = db.execute(
        """SELECT ts_ms, game_time, radiant_score, dire_score, nw_diff,
                  radiant_team, dire_team, radiant_nw, dire_nw
           FROM dota_ticks
           WHERE match_key=? AND ts_ms BETWEEN ? AND ?
           ORDER BY ts_ms""",
        (match_key, start_ms, end_ms),
    ).fetchall()
    seen: set[int] = set()
    out = []
    for row in rows:
        ts_ms, game_time, r_score, d_score, nw_diff, r_team, d_team, r_nw, d_nw = row
        gt = int(game_time or 0)
        if gt in seen:
            continue
        seen.add(gt)
        out.append({
            "ts_ms": ts_ms,
            "game_time_sec": gt,
            "radiant_score": int(r_score or 0),
            "dire_score": int(d_score or 0),
            "radiant_lead": int(nw_diff or 0),
            "radiant_team": r_team,
            "dire_team": d_team,
            "radiant_net_worth": int(r_nw) if r_nw is not None else None,
            "dire_net_worth": int(d_nw) if d_nw is not None else None,
            "match_id": match_key,
        })
    return out


def _load_market(db: sqlite3.Connection, token_id: str, start_ms: int, end_ms: int) -> list[dict]:
    rows = db.execute(
        """SELECT ts_ms, best_bid, best_ask, mid
           FROM market_ticks
           WHERE token_id=? AND ts_ms BETWEEN ? AND ?
           ORDER BY ts_ms""",
        (token_id, start_ms, end_ms),
    ).fetchall()
    return [{"ts_ms": r[0], "best_bid": r[1], "best_ask": r[2], "mid": r[3]} for r in rows]


def _nearest_before(ticks: list[dict], ts_ms: int) -> dict | None:
    lo, hi, result = 0, len(ticks) - 1, None
    while lo <= hi:
        m = (lo + hi) // 2
        if ticks[m]["ts_ms"] <= ts_ms:
            result = ticks[m]; lo = m + 1
        else:
            hi = m - 1
    return result


def _mid_at(ticks: list[dict], ts_ms: int) -> float | None:
    t = _nearest_before(ticks, ts_ms)
    return t["mid"] if t else None


def _ask_at(ticks: list[dict], ts_ms: int) -> float | None:
    t = _nearest_before(ticks, ts_ms)
    return t["best_ask"] if t else None


def _clip(p: float) -> float:
    return min(max(float(p), 0.001), 0.999)


def _logit(p: float) -> float:
    p = _clip(p)
    return log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + exp(-x))


def _ml_fair_for_snapshot(bundle: FairModelBundle | None, snap: dict, direction: str) -> tuple[float | None, str]:
    if bundle is None:
        return None, "no_bundle"
    row = {
        "game_time_sec": snap.get("game_time_sec"),
        "radiant_score": snap.get("radiant_score"),
        "dire_score": snap.get("dire_score"),
        "score_diff": (snap.get("radiant_score") or 0) - (snap.get("dire_score") or 0),
        "net_worth_diff": snap.get("radiant_lead"),
        "radiant_net_worth": snap.get("radiant_net_worth"),
        "dire_net_worth": snap.get("dire_net_worth"),
        "radiant_tower_state": None,
        "dire_tower_state": None,
        "radiant_barracks_state": None,
        "dire_barracks_state": None,
        "level_diff": None,
        "gpm_diff": None,
        "xpm_diff": None,
        "gold_diff": None,
        "radiant_dead_count": None,
        "dire_dead_count": None,
        "radiant_core_dead_count": None,
        "dire_core_dead_count": None,
        "max_respawn_timer": None,
        "radiant_has_aegis": None,
        "dire_has_aegis": None,
    }
    pred = bundle.predict_radiant(row)
    radiant_p = pred["radiant_fair_probability"]
    phase = pred["model_phase"]
    if radiant_p is None:
        return None, pred.get("model_reason", "unknown")
    if direction == "radiant":
        return radiant_p, phase
    return 1.0 - radiant_p, phase


def _hybrid_fair(ml_fair: float | None, heuristic_move: float, anchor_price: float, events: list, snap: dict) -> float | None:
    slow = ml_fair if ml_fair is not None else anchor_price + heuristic_move
    event_dicts = []
    for evt in events:
        event_dicts.append({
            "event_type": evt.event_type,
            "event_confidence": evt.event_confidence,
            "base_pressure_score": evt.base_pressure_score,
            "fight_pressure_score": evt.fight_pressure_score,
            "economic_pressure_score": evt.economic_pressure_score,
            "conversion_score": evt.conversion_score,
        })
    result = compute_hybrid_nowcast(
        latest_liveleague_features=None,
        latest_toplive_snapshot=snap,
        toplive_event_cluster=event_dicts,
        source_delay_metrics=None,
        slow_model_fair=ml_fair,
        event_only_fair=anchor_price + heuristic_move if ml_fair is None else None,
    )
    return result.hybrid_fair


def run_backtest_ml(min_lag: float, size_usd: float, exit_sec: int, model_path: str | None = None, ultra_late_only: bool = False) -> list[MLTrade]:
    bundle = None
    if model_path:
        try:
            bundle = load_bundle(model_path)
            print(f"Loaded ML model from {model_path}")
            phases_available = list(bundle.models.keys())
            print(f"  Phases available: {phases_available}")
        except Exception as e:
            print(f"WARNING: Could not load ML model from {model_path}: {e}")
            bundle = None

    all_trades: list[MLTrade] = []

    for seg in SEGMENTS:
        db = sqlite3.connect(seg["db"])
        match_key = seg["match_key"]

        d_range = db.execute(
            "SELECT MIN(ts_ms), MAX(ts_ms) FROM dota_ticks WHERE match_key=?", (match_key,)
        ).fetchone()
        if not d_range or not d_range[0]:
            db.close()
            continue

        start_ms, end_ms = d_range
        rad_ticks = _load_market(db, seg["radiant_token"], start_ms, end_ms)
        dire_ticks = _load_market(db, seg["dire_token"], start_ms, end_ms)
        db.close()

        if not rad_ticks and not dire_ticks:
            continue

        mkt_start = min((t["ts_ms"] for t in rad_ticks + dire_ticks), default=start_ms)
        mkt_end   = max((t["ts_ms"] for t in rad_ticks + dire_ticks), default=end_ms)
        db2 = sqlite3.connect(seg["db"])
        dota_snaps = _load_dota(db2, match_key, mkt_start, mkt_end)
        db2.close()

        if not dota_snaps:
            continue

        pre_game_rad = rad_ticks[0]["mid"] if rad_ticks else 0.5
        pre_game_dire = dire_ticks[0]["mid"] if dire_ticks else 0.5

        detector = EventDetector()
        cooldown_until_ms: dict[tuple[str, str], int] = {}

        for snap in dota_snaps:
            ts = snap["ts_ms"]
            game_time = snap.get("game_time_sec", 0)

            if ultra_late_only and game_time < 50 * 60:
                continue

            events = detector.observe(snap)

            for evt in events:
                if evt.event_type not in EVENT_EXPECTED_MOVE:
                    continue

                if evt.event_type in _HIGH_SEVERITY_ONLY and evt.severity != "high":
                    continue

                direction = evt.direction
                if direction not in ("radiant", "dire"):
                    continue

                if ts < cooldown_until_ms.get((direction, evt.event_type), 0):
                    continue

                expected_move = EVENT_EXPECTED_MOVE[evt.event_type]

                if evt.delta is not None:
                    if evt.event_type in ("LEAD_SWING_30S", "LEAD_SWING_60S"):
                        threshold = EVENT_LEAD_SWING_30S if "30S" in evt.event_type else EVENT_LEAD_SWING_60S
                        expected_move *= min(abs(evt.delta) / threshold, 3.0)
                    elif evt.event_type == "COMEBACK":
                        expected_move *= min(abs(evt.delta) / 3000, 2.0)
                    elif evt.event_type == "KILL_CONFIRMED_LEAD_SWING":
                        expected_move *= min(abs(evt.delta) / 2500, 2.0)
                    elif evt.event_type in ("KILL_BURST_30S", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE"):
                        expected_move *= min(abs(evt.delta) / 5, 2.0)
                    elif evt.event_type in ("T2_TOWER_FALL", "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN", "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL"):
                        if evt.delta > 1:
                            expected_move *= min(float(evt.delta), 2.0)

                if direction == "radiant":
                    token_ticks = rad_ticks
                    pre_game_price = pre_game_rad
                    terminal = float(seg["radiant_win"])
                    side = "BUY_RADIANT"
                else:
                    token_ticks = dire_ticks
                    pre_game_price = pre_game_dire
                    terminal = float(1 - seg["radiant_win"])
                    side = "BUY_DIRE"

                price_at_event = _mid_at(token_ticks, ts)
                if price_at_event is None:
                    continue

                actual_move = price_at_event - pre_game_price
                heuristic_lag = expected_move - actual_move

                if heuristic_lag < min_lag:
                    continue

                ask = _ask_at(token_ticks, ts)
                if ask is None:
                    continue

                ml_fair_yes, ml_phase = _ml_fair_for_snapshot(bundle, snap, direction)
                ml_fair_radiant, _ = _ml_fair_for_snapshot(bundle, snap, "radiant")

                hybrid_fair = None
                if ml_fair_yes is not None:
                    heuristic_anchor_price = price_at_event
                    heuristic_fair = _clip(heuristic_anchor_price + expected_move)
                    ml_fair_clipped = _clip(ml_fair_yes)
                    event_shock = expected_move
                    hybrid_fair_logit = _logit(ml_fair_clipped) + event_shock * 4.0 * 0.25
                    hybrid_fair = _clip(_sigmoid(hybrid_fair_logit))

                ml_edge = (_clip(ml_fair_yes) - ask) if ml_fair_yes is not None else None
                hybrid_edge = (hybrid_fair - ask) if hybrid_fair is not None else None

                trade = MLTrade(
                    label=seg["label"],
                    event_type=evt.event_type,
                    direction=direction,
                    severity=evt.severity,
                    game_time_sec=game_time,
                    wall_ts_ms=ts,
                    side=side,
                    fill=ask,
                    pre_game_price=pre_game_price,
                    price_at_event=price_at_event,
                    heuristic_expected_move=round(expected_move, 4),
                    heuristic_lag=round(heuristic_lag, 4),
                    ml_fair_radiant=round(ml_fair_radiant, 4) if ml_fair_radiant is not None else None,
                    ml_fair_yes=round(ml_fair_yes, 4) if ml_fair_yes is not None else None,
                    hybrid_fair=round(hybrid_fair, 4) if hybrid_fair is not None else None,
                    ml_phase=ml_phase,
                    actual_move=round(actual_move, 4),
                    radiant_win=seg["radiant_win"],
                    ml_edge=round(ml_edge, 4) if ml_edge is not None else None,
                    hybrid_edge=round(hybrid_edge, 4) if hybrid_edge is not None else None,
                )

                exit_ms = ts + exit_sec * 1000
                for horizon_ms, pnl_attr in [(15_000, "pnl_15s"), (30_000, "pnl_30s"), (60_000, "pnl_60s")]:
                    fp = _mid_at(token_ticks, ts + horizon_ms)
                    if fp is not None:
                        setattr(trade, pnl_attr, (fp - ask) * size_usd)

                trade.pnl_term = (terminal - ask) * size_usd

                for horizon_ms, pnl_attr, fair_attr in [
                    (15_000, "pnl_ml_15s", "ml_fair_yes"),
                    (30_000, "pnl_ml_30s", "ml_fair_yes"),
                    (60_000, "pnl_ml_60s", "ml_fair_yes"),
                ]:
                    fp = _mid_at(token_ticks, ts + horizon_ms)
                    if fp is not None and ml_fair_yes is not None:
                        ml_at_exit = _ml_fair_for_snapshot(bundle, snap, direction)[0]
                        if ml_at_exit is not None:
                            setattr(trade, pnl_attr, (_clip(ml_at_exit) - _clip(ml_fair_yes)) * size_usd)

                if hybrid_fair is not None:
                    for horizon_ms, pnl_attr in [(15_000, "pnl_hybrid_15s"), (30_000, "pnl_hybrid_30s"), (60_000, "pnl_hybrid_60s")]:
                        fp = _mid_at(token_ticks, ts + horizon_ms)
                        if fp is not None:
                            setattr(trade, pnl_attr, (fp - ask) * size_usd)
                    trade.pnl_hybrid_term = (terminal - ask) * size_usd

                all_trades.append(trade)
                cooldown_until_ms[(direction, evt.event_type)] = exit_ms

    return all_trades


def _fmt(v: float | None) -> str:
    return f"{v:+7.2f}" if v is not None else "    n/a"


def print_results(trades: list[MLTrade], min_lag: float, size_usd: float, exit_sec: int, ultra_late_only: bool):
    mode_label = " (ULTRA-LATE 50min+)" if ultra_late_only else ""
    print(f"\nML-Enhanced Backtest{mode_label}  min_lag={min_lag}  size=${size_usd}  exit={exit_sec}s  segments={len(SEGMENTS)}")
    print(f"Total signals: {len(trades)}\n")

    if not trades:
        print("No signals fired. Lower --lag threshold.")
        return

    print("=" * 120)
    print("SIGNAL-LEVEL COMPARISON")
    print("=" * 120)
    header = (f"{'match':>25}  {'gt':>5}  {'event':>22}  {'dir':>5}  {'sev':>4}  "
              f"{'fill':>5}  {'h_lag':>5}  {'ml_fair':>7}  {'hyb_fair':>8}  "
              f"{'ml_edge':>7}  {'hyb_edge':>8}  {'term':>7}")
    print(header)
    print("-" * len(header))

    for t in sorted(trades, key=lambda x: (x.label, x.game_time_sec)):
        print(
            f"{t.label:>25}  {t.game_time_sec:>5}  {t.event_type:>22}  {t.direction:>5}  {t.severity:>4}  "
            f"{t.fill:.3f}  {t.heuristic_lag:.3f}  {_fmt(t.ml_fair_yes)}  {_fmt(t.hybrid_fair)}  "
            f"{_fmt(t.ml_edge)}  {_fmt(t.hybrid_edge)}  {_fmt(t.pnl_term)}"
        )

    print("\n" + "=" * 120)
    print("AGGREGATE PnL COMPARISON (heuristic entry at ask, hold to horizon)")
    print("=" * 120)

    def _pnl_stats(key: str):
        vals = [getattr(t, key) for t in trades if getattr(t, key) is not None]
        if not vals:
            return None
        wins = sum(1 for v in vals if v > 0)
        return {"avg": sum(vals) / len(vals), "total": sum(vals), "n": len(vals), "wins": wins, "win_rate": wins / len(vals)}

    for label, key in [("Heuristic 15s", "pnl_15s"), ("Heuristic 30s", "pnl_30s"),
                        ("Heuristic 60s", "pnl_60s"), ("Terminal", "pnl_term"),
                        ("ML 15s", "pnl_ml_15s"), ("ML 30s", "pnl_ml_30s"),
                        ("ML 60s", "pnl_ml_60s"), ("ML Terminal", "pnl_ml_term"),
                        ("Hybrid 15s", "pnl_hybrid_15s"), ("Hybrid 30s", "pnl_hybrid_30s"),
                        ("Hybrid 60s", "pnl_hybrid_60s"), ("Hybrid Terminal", "pnl_hybrid_term")]:
        stats = _pnl_stats(key)
        if stats:
            print(f"  {label:>20}: avg={stats['avg']:+.2f}  total={stats['total']:+.2f}  "
                  f"n={stats['n']}  wins={stats['wins']}/{stats['n']}  win_rate={stats['win_rate']:.1%}")

    print("\n" + "=" * 120)
    print("ML MODEL vs HEURISTIC: DIRECTIONAL ACCURACY")
    print("=" * 120)

    heuristic_correct = 0
    heuristic_total = 0
    ml_correct = 0
    ml_total = 0
    hybrid_correct = 0
    hybrid_total = 0

    for t in trades:
        actual_direction_correct = (t.direction == "radiant") == (t.radiant_win == 1)
        heuristic_total += 1
        heuristic_correct += int(heuristic_lag_correct(t))

        if t.ml_fair_yes is not None:
            ml_total += 1
            ml_direction = t.ml_fair_yes > t.price_at_event
            ml_correct += int(ml_direction == actual_direction_correct or (ml_direction and t.direction == "radiant") or (not ml_direction and t.direction == "dire"))

        if t.hybrid_fair is not None:
            hybrid_total += 1
            hybrid_direction_correct_for_this_side = t.hybrid_fair > t.fill
            hybrid_total_events = hybrid_total
            hybrid_correct += int(hybrid_direction_correct_for_this_side == actual_direction_correct)

    print(f"  Heuristic: {heuristic_correct}/{heuristic_total} direction-correct")
    print(f"  ML model:  {ml_correct}/{ml_total} model-agrees-with-event-direction" if ml_total > 0 else "  ML model:  no predictions")
    print(f"  Hybrid:    {hybrid_correct}/{hybrid_total} direction-correct" if hybrid_total > 0 else "  Hybrid:    no predictions")

    print("\n" + "=" * 120)
    print("PHASE-STRATIFIED ANALYSIS")
    print("=" * 120)

    by_phase: dict[str, list] = defaultdict(list)
    for t in trades:
        phase = phase_for_duration(t.game_time_sec)
        by_phase[phase].append(t)

    for phase in ["early", "laning", "mid", "late", "ultra_late"]:
        phase_trades = by_phase.get(phase, [])
        if not phase_trades:
            print(f"  {phase:>12}: no trades")
            continue

        terms = [t.pnl_term for t in phase_trades if t.pnl_term is not None]
        h15 = [t.pnl_15s for t in phase_trades if t.pnl_15s is not None]
        ml_fairs = [t.ml_fair_yes for t in phase_trades if t.ml_fair_yes is not None]
        hyb_fairs = [t.hybrid_fair for t in phase_trades if t.hybrid_fair is not None]
        correct_dir = sum(1 for t in phase_trades if (t.direction == "radiant") == (t.radiant_win == 1))

        ml_avg_edge = None
        if ml_fairs:
            edges = []
            for t in phase_trades:
                if t.ml_fair_yes is not None and t.ml_edge is not None:
                    edges.append(t.ml_edge)
            ml_avg_edge = sum(edges) / len(edges) if edges else None

        ml_edge_str = f"{ml_avg_edge:+.4f}" if ml_avg_edge is not None else "n/a"
        term_avg = sum(terms) / len(terms) if terms else 0
        print(f"  {phase:>12}: n={len(phase_trades)}  correct_dir={correct_dir}/{len(phase_trades)}  "
              f"15s_avg={sum(h15)/len(h15) if h15 else 0:+.2f}  term_avg={term_avg:+.2f}  "
              f"ml_fair_available={len(ml_fairs)}  ml_avg_edge={ml_edge_str}  "
              f"hybrid_available={len(hyb_fairs)}")

    if not ultra_late_only:
        print("\n" + "=" * 120)
        print("ULTRA-LATE DEEP DIVE (50min+ / game_time >= 3000)")
        print("=" * 120)
        ultra_trades = [t for t in trades if t.game_time_sec >= 3000]
        if ultra_trades:
            terms = [t.pnl_term for t in ultra_trades if t.pnl_term is not None]
            h30 = [t.pnl_30s for t in ultra_trades if t.pnl_30s is not None]
            ml_edges = [t.ml_edge for t in ultra_trades if t.ml_edge is not None]
            hyb_edges = [t.hybrid_edge for t in ultra_trades if t.hybrid_edge is not None]
            correct_dir = sum(1 for t in ultra_trades if (t.direction == "radiant") == (t.radiant_win == 1))

            print(f"  Ultra-late trades: {len(ultra_trades)}")
            print(f"  Direction accuracy: {correct_dir}/{len(ultra_trades)}")
            print(f"  Heuristic 30s PnL avg: {sum(h30)/len(h30):+.2f}" if h30 else "  No 30s PnL data")
            print(f"  Terminal PnL avg: {sum(terms)/len(terms):+.2f}" if terms else "  No terminal PnL data")
            print(f"  ML edge avg: {sum(ml_edges)/len(ml_edges):+.4f}" if ml_edges else "  No ML data")
            print(f"  Hybrid edge avg: {sum(hyb_edges)/len(hyb_edges):+.4f}" if hyb_edges else "  No hybrid data")

            by_evt: dict[str, list] = defaultdict(list)
            for t in ultra_trades:
                by_evt[t.event_type].append(t)
            print("\n  By ultra-late event type:")
            for evt_type, ts in sorted(by_evt.items()):
                et_terms = [t.pnl_term for t in ts if t.pnl_term is not None]
                et_correct = sum(1 for t in ts if (t.direction == "radiant") == (t.radiant_win == 1))
                et_ml = [t.ml_edge for t in ts if t.ml_edge is not None]
                print(f"    {evt_type:>30}: n={len(ts)}  correct_dir={et_correct}/{len(ts)}  "
                      f"term_avg={sum(et_terms)/len(et_terms):+.2f}" if et_terms else f"    {evt_type:>30}: n={len(ts)}  no terminal data")
        else:
            print("  No ultra-late trades found in this dataset.")

    print("\n" + "=" * 120)
    print("PER-GAME BREAKDOWN")
    print("=" * 120)
    by_game: dict[str, list] = defaultdict(list)
    for t in trades:
        by_game[t.label].append(t)
    for label, ts in sorted(by_game.items()):
        terms = [t.pnl_term for t in ts if t.pnl_term is not None]
        h15 = [t.pnl_15s for t in ts if t.pnl_15s is not None]
        correct = sum(1 for t in ts if (t.direction == "radiant") == (t.radiant_win == 1))
        ml_avg = sum(t.ml_edge for t in ts if t.ml_edge is not None) / max(len([t for t in ts if t.ml_edge is not None]), 1)
        ml_avg_str = f"{ml_avg:+.4f}" if ml_fairs else "n/a"
        print(f"  {label:>30}: n={len(ts)}  correct_dir={correct}/{len(ts)}  "
              f"15s_avg={sum(h15)/len(h15) if h15 else 0:+.2f}  "
              f"term_avg={sum(terms)/len(terms) if terms else 0:+.2f}  "
              f"ml_avg_edge={ml_avg_str}")


def heuristic_lag_correct(t: MLTrade) -> bool:
    return (t.direction == "radiant") == (t.radiant_win == 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lag", type=float, default=0.05, help="Min market lag to fire (default 0.05)")
    parser.add_argument("--size", type=float, default=PAPER_SIZE_USD)
    parser.add_argument("--exit", type=int, default=30, help="Exit horizon in seconds (default 30)")
    parser.add_argument("--model", type=str, default=DOTA_FAIR_MODEL_PATH, help="Path to dota_fair.joblib model bundle")
    parser.add_argument("--ultra-late-only", action="store_true", help="Only analyze 50min+ game events")
    parser.add_argument("--no-model", action="store_true", help="Skip ML model loading (heuristic-only comparison)")
    args = parser.parse_args()

    model_path = None if args.no_model else args.model
    trades = run_backtest_ml(
        min_lag=args.lag, size_usd=args.size, exit_sec=args.exit,
        model_path=model_path, ultra_late_only=args.ultra_late_only,
    )
    print_results(trades, min_lag=args.lag, size_usd=args.size, exit_sec=args.exit, ultra_late_only=args.ultra_late_only)