import sqlite3
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dota_fair_model.inference import load_bundle
from config import DOTA_FAIR_MODEL_PATH

DATA_DIR = "/home/irene/dota_poly_bot_final/data"
DB_PATH = f"{DATA_DIR}/1win_pari_g1.sqlite"
MATCH_KEY = "90285623272207384_m1"
TOKEN_ID = "70347395524393779469493680391299369304316720284512794724445180423011761114165"

def analyze():
    db = sqlite3.connect(DB_PATH)
    bundle = load_bundle(DOTA_FAIR_MODEL_PATH)
    
    dota = db.execute("SELECT ts_ms, game_time, radiant_score, dire_score, nw_diff, radiant_nw, dire_nw FROM dota_ticks WHERE match_key=? ORDER BY ts_ms", (MATCH_KEY,)).fetchall()
    market = db.execute("SELECT ts_ms, mid FROM market_ticks WHERE token_id=? ORDER BY ts_ms", (TOKEN_ID,)).fetchall()
    
    print(f"Analyzing DreamLeague Efficiency: {MATCH_KEY}")
    print(f"{'GT':<6} | {'Lead':<7} | {'ML Fair':<7} | {'Market Mid':<10} | {'Edge':<6}")
    print("-" * 50)
    
    m_ptr = 0
    for row in dota[::50]: # Sample every 50 ticks for brevity
        ts, gt, rs, ds, lead, rnw, dnw = row
        snap = {"game_time_sec": gt, "radiant_score": rs, "dire_score": ds, "radiant_lead": lead, "radiant_net_worth": rnw, "dire_net_worth": dnw}
        
        pred = bundle.predict_radiant(snap)
        fair = pred["radiant_fair_probability"]
        
        # Find market price at this time
        while m_ptr < len(market) and market[m_ptr][0] < ts:
            m_ptr += 1
        
        if m_ptr < len(market):
            mid = market[m_ptr][1]
            edge = (fair - mid) if fair is not None else 0
            print(f"{gt:<6} | {lead:<7} | {fair:<7.3f} | {mid:<10.3f} | {edge:<6.3f}")

if __name__ == "__main__":
    analyze()
