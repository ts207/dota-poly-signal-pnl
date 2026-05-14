import argparse
import sqlite3
import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dota_fair_model.inference import FairModelBundle, load_bundle
from event_detector import EventDetector
from signal_engine import ACTIVE_EVENTS
from hybrid_nowcast import compute_hybrid_nowcast
from config import DOTA_FAIR_MODEL_PATH

DATA_DIR = "/home/irene/dota_poly_bot_final/data"
SEGMENTS = [
    {"label": "Carstensz vs Grind", "db": f"{DATA_DIR}/dota_poly_collection.sqlite", "match_key": "90285607589477394", "token": "90268231449155282246853972144583742931465600097997027484803301961579288855144", "win": 0},
    {"label": "PlayTime vs 1w", "db": f"{DATA_DIR}/dota_poly_collection.sqlite", "match_key": "90285599503423511_m1", "token": "13478386926402301406532136263977204904714000287949507563856704721767290839044", "win": 0},
]

def load_data(db_path, match_key, token_id):
    db = sqlite3.connect(db_path)
    dota = db.execute("SELECT ts_ms, game_time, radiant_score, dire_score, nw_diff, radiant_nw, dire_nw FROM dota_ticks WHERE match_key=? ORDER BY ts_ms", (match_key,)).fetchall()
    market = db.execute("SELECT ts_ms, best_ask, mid FROM market_ticks WHERE token_id=? ORDER BY ts_ms", (token_id,)).fetchall()
    db.close()
    return dota, market

def get_nearest(ticks, ts_ms):
    lo, hi, res = 0, len(ticks)-1, None
    while lo <= hi:
        m = (lo + hi) // 2
        if ticks[m][0] <= ts_ms: res = ticks[m]; lo = m + 1
        else: hi = m - 1
    return res

async def run_hybrid_backtest(min_edge=0.02):
    bundle = load_bundle(DOTA_FAIR_MODEL_PATH)
    detector = EventDetector()
    results = []

    for seg in SEGMENTS:
        dota_rows, market_ticks = load_data(seg["db"], seg["match_key"], seg["token"])
        if not dota_rows or not market_ticks: continue
        
        for i, row in enumerate(dota_rows):
            ts, gt, r_score, d_score, lead, r_nw, d_nw = row
            snap = {"game_time_sec": gt, "radiant_score": r_score, "dire_score": d_score, "radiant_lead": lead, "radiant_net_worth": r_nw, "dire_net_worth": d_nw}
            
            # Simulate 120s delayed stats base
            delay_ticks = 120 # Assuming 1 tick/sec approx
            base_idx = max(0, i - delay_ticks)
            base_row = dota_rows[base_idx]
            base_snap = {
                "game_time_sec": base_row[1], 
                "radiant_score": base_row[2], 
                "dire_score": base_row[3], 
                "radiant_lead": base_row[4],
                "radiant_net_worth": base_row[5], 
                "dire_net_worth": base_row[6],
                "realtime_lead_nw": base_row[4]
            }

            events = detector.observe(snap)
            if not events: continue
            
            # Hybrid Calculation
            ml_fair = bundle.predict_radiant(base_snap)["radiant_fair_probability"]
            mkt = get_nearest(market_ticks, ts)
            if not mkt: continue
            ask = mkt[1]
            
            event_dicts = [{"event_type": e.event_type} for e in events]
            nowcast = compute_hybrid_nowcast(
                latest_liveleague_features=None,
                latest_toplive_snapshot=base_snap, # Context from base
                toplive_event_cluster=event_dicts,
                source_delay_metrics={"game_time_lag_sec": 120},
                slow_model_fair=ml_fair,
                event_only_fair=mkt[2],
                game_time_sec=gt
            )
            
            # Override TopLive snapshot fields with the FRESH lead for drift calculation
            # In live, compute_hybrid_nowcast takes latest_toplive_snapshot for the 0s lead
            nowcast = compute_hybrid_nowcast(
                latest_liveleague_features=None,
                latest_toplive_snapshot=snap, # Fresh lead
                toplive_event_cluster=event_dicts,
                source_delay_metrics={"game_time_lag_sec": 120},
                slow_model_fair=ml_fair,
                event_only_fair=mkt[2],
                game_time_sec=gt
            )
            
            edge = (nowcast.hybrid_fair - ask) if nowcast.hybrid_fair else 0
            
            # Debug some samples
            if i % 500 == 0:
                print(f"DEBUG: GT={gt} | ML={ml_fair:.3f} | Hyb={nowcast.hybrid_fair:.3f} | Ask={ask:.3f} | Edge={edge:.3f} | Drift={snap['radiant_lead'] - base_snap['radiant_lead']}")

            if edge > min_edge:
                # Trade fired!
                results.append({"match": seg["label"], "gt": gt, "edge": edge, "fill": ask, "win": seg["win"]})
                
    print(f"\nHybrid Edge Backtest (min_edge={min_edge})")
    print(f"{'Match':<20} | {'GT':<6} | {'Edge':<6} | {'Fill':<6}")
    for r in results:
        print(f"{r['match']:<20} | {r['gt']:<6} | {r['edge']:<6.3f} | {r['fill']:<6.3f}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_hybrid_backtest())
