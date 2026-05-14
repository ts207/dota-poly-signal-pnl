"""Pure ML-only strategy backtest.
Trades whenever the ML fair price deviates from the market price by a threshold,
regardless of whether any specific event was detected.
"""
from __future__ import annotations
import argparse
import sqlite3
import os
import sys
from dataclasses import dataclass
from collections import defaultdict
from dota_fair_model.inference import FairModelBundle, load_bundle
from dota_fair_model.schemas import phase_for_duration
from config import DOTA_FAIR_MODEL_PATH

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
]

@dataclass
class MLOnlyTrade:
    match_label: str
    game_time_sec: int
    wall_ts_ms: int
    side: str
    fill: float
    ml_fair: float
    edge: float
    terminal_pnl: float
    horizon_pnl: float | None = None

def _load_dota(db: sqlite3.Connection, match_key: str) -> list[dict]:
    rows = db.execute(
        "SELECT ts_ms, game_time, radiant_score, dire_score, nw_diff, radiant_nw, dire_nw "
        "FROM dota_ticks WHERE match_key=? ORDER BY ts_ms", (match_key,)
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "ts_ms": r[0], "game_time_sec": r[1], "radiant_score": r[2], "dire_score": r[3],
            "radiant_lead": r[4], "radiant_net_worth": r[5], "dire_net_worth": r[6]
        })
    return out

def _load_market(db: sqlite3.Connection, token_id: str) -> list[dict]:
    rows = db.execute(
        "SELECT ts_ms, best_bid, best_ask, mid FROM market_ticks WHERE token_id=? ORDER BY ts_ms", (token_id,)
    ).fetchall()
    return [{"ts_ms": r[0], "best_bid": r[1], "best_ask": r[2], "mid": r[3]} for r in rows]

def _nearest_before(ticks: list[dict], ts_ms: int) -> dict | None:
    lo, hi, res = 0, len(ticks)-1, None
    while lo <= hi:
        m = (lo + hi) // 2
        if ticks[m]["ts_ms"] <= ts_ms:
            res = ticks[m]; lo = m + 1
        else:
            hi = m - 1
    return res

def run_ml_only_backtest(min_edge: float, exit_sec: int, model_path: str):
    bundle = load_bundle(model_path)
    all_trades = []

    for seg in SEGMENTS:
        db = sqlite3.connect(seg["db"])
        dota_ticks = _load_dota(db, seg["match_key"])
        rad_ticks = _load_market(db, seg["radiant_token"])
        dire_ticks = _load_market(db, seg["dire_token"])
        db.close()

        cooldown_until_ms = 0
        
        for snap in dota_ticks:
            ts = snap["ts_ms"]
            game_time = snap["game_time_sec"] or 0
            
            # FIX: Skip pre-game and early-early game (unreliable features)
            if game_time < 300: 
                continue
                
            if ts < cooldown_until_ms:
                continue

            # Check Radiant side
            mkt_rad = _nearest_before(rad_ticks, ts)
            if not mkt_rad or not mkt_rad["best_ask"]: continue
            
            row = {
                "game_time_sec": snap["game_time_sec"],
                "radiant_score": snap["radiant_score"],
                "dire_score": snap["dire_score"],
                "score_diff": (snap["radiant_score"] or 0) - (snap["dire_score"] or 0),
                "net_worth_diff": snap["radiant_lead"],
                "radiant_net_worth": snap["radiant_net_worth"],
                "dire_net_worth": snap["dire_net_worth"],
            }
            pred = bundle.predict_radiant(row)
            ml_rad = pred["radiant_fair_probability"]
            if ml_rad is None: continue

            # Buy Radiant if edge > threshold
            edge_rad = ml_rad - mkt_rad["best_ask"]
            if edge_rad > min_edge:
                terminal = float(seg["radiant_win"])
                exit_mkt = _nearest_before(rad_ticks, ts + exit_sec * 1000)
                h_pnl = (exit_mkt["mid"] - mkt_rad["best_ask"]) * PAPER_SIZE_USD if exit_mkt else None
                all_trades.append(MLOnlyTrade(
                    seg["label"], snap["game_time_sec"], ts, "BUY_RADIANT", 
                    mkt_rad["best_ask"], ml_rad, edge_rad, (terminal - mkt_rad["best_ask"]) * PAPER_SIZE_USD, h_pnl
                ))
                cooldown_until_ms = ts + 60 * 1000 # 1 min cooldown
                continue

            # Check Dire side
            mkt_dire = _nearest_before(dire_ticks, ts)
            if not mkt_dire or not mkt_dire["best_ask"]: continue
            
            ml_dire = 1.0 - ml_rad
            edge_dire = ml_dire - mkt_dire["best_ask"]
            if edge_dire > min_edge:
                terminal = float(1 - seg["radiant_win"])
                exit_mkt = _nearest_before(dire_ticks, ts + exit_sec * 1000)
                h_pnl = (exit_mkt["mid"] - mkt_dire["best_ask"]) * PAPER_SIZE_USD if exit_mkt else None
                all_trades.append(MLOnlyTrade(
                    seg["label"], snap["game_time_sec"], ts, "BUY_DIRE", 
                    mkt_dire["best_ask"], ml_dire, edge_dire, (terminal - mkt_dire["best_ask"]) * PAPER_SIZE_USD, h_pnl
                ))
                cooldown_until_ms = ts + 60 * 1000
                
    return all_trades

def print_summary(trades: list[MLOnlyTrade]):
    print(f"\nML-ONLY STRATEGY BACKTEST (n={len(trades)})")
    print("=" * 80)
    if not trades:
        print("No trades found.")
        return
        
    total_term = sum(t.terminal_pnl for t in trades)
    wins = sum(1 for t in trades if t.terminal_pnl > 0)
    print(f"Total Terminal PnL: ${total_term:+.2f}")
    print(f"Win Rate: {wins/len(trades):.1%} ({wins}/{len(trades)})")
    print(f"Average Edge: {sum(t.edge for t in trades)/len(trades):.1%}")
    
    print("\nSample Trades:")
    for t in trades[:10]:
        print(f"  {t.match_label:>25} | {t.game_time_sec:>4}s | {t.side:>12} | fill={t.fill:.3f} | ml={t.ml_fair:.3f} | edge={t.edge:+.3f} | pnl={t.terminal_pnl:+.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--edge", type=float, default=0.05)
    parser.add_argument("--exit", type=int, default=120)
    args = parser.parse_args()
    
    trades = run_ml_only_backtest(args.edge, args.exit, DOTA_FAIR_MODEL_PATH)
    print_summary(trades)
