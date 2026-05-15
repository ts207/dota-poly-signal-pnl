import csv
import json
from datetime import datetime, timedelta
from dota_fair_model.inference import load_bundle
from dota_fair_model.features import row_to_features
from signal_engine import apply_probability_move

def load_team_stats():
    with open('dota_fair_model/models/team_stats.json') as f:
        return json.load(f)

def get_markout(books, token_id, signal_ts, seconds):
    target_ts = signal_ts + timedelta(seconds=seconds)
    best_bid = None
    for b in books:
        if b['asset_id'] != token_id: continue
        ts = datetime.fromisoformat(b['timestamp_utc'].replace('Z', '+00:00'))
        if ts >= target_ts:
            return float(b['best_bid']) if b['best_bid'] else None
    return None

def main():
    bundle = load_bundle('dota_fair_model/models/dota_fair.joblib')
    team_stats = load_team_stats()
    
    with open('logs/signals.csv', 'r') as f:
        signals = list(csv.DictReader(f))
        
    with open('logs/book_events.csv', 'r') as f:
        books = list(csv.DictReader(f))
        
    print("--- Simulating Missed Trades (edge_too_small) ---")
    for r in signals:
        if r.get('skip_reason') not in ['edge_too_small']:
            continue
            
        signal_ts = datetime.fromisoformat(r['timestamp_utc'].replace('Z', '+00:00'))
        match_id = r.get('match_id')
        event = r.get('event_type')
        token = r.get('token_id')
        side = r.get('side')
        
        # Build features correctly
        row = dict(r)
        
        # Fix missing features
        try:
            row['radiant_score'] = float(r.get('radiant_score') or 0)
            row['dire_score'] = float(r.get('dire_score') or 0)
            row['score_diff'] = row['radiant_score'] - row['dire_score']
            row['game_time_sec'] = float(r.get('game_time_sec') or 0)
            row['net_worth_diff'] = float(r.get('networth_delta') or 0)
            row['kill_diff_delta'] = float(r.get('kill_diff_delta') or 0)
            row['radiant_team_win_ratio'] = team_stats.get(str(r.get('radiant_team_id') or ""), 0.5)
            row['dire_team_win_ratio'] = team_stats.get(str(r.get('dire_team_id') or ""), 0.5)
        except Exception as e:
            print(f"Error preparing row: {e}")
            continue
            
        pred = bundle.predict_radiant(row)
        p_rad = pred.get("radiant_fair_probability")
        
        if not p_rad: continue
            
        event_direction = r.get('event_direction')
        slow_model_fair = p_rad if event_direction == "radiant" else (1.0 - p_rad)
        
        # In hybrid nowcast, slow_model_fair + fast_event_adjustment = hybrid_fair
        fast_adj = float(r.get('fast_event_adjustment') or 0)
        uncertainty = float(r.get('uncertainty_penalty') or 0.05)
        
        true_hybrid_fair = min(max(slow_model_fair + fast_adj - uncertainty, 0.001), 0.999)
        
        old_fair = float(r.get('fair_price') or 0)
        ask = float(r.get('ask') or r.get('executable_price') or 0)
        
        if ask == 0:
            print(f"\n{signal_ts.strftime('%H:%M:%S')} | {event} | Ask was missing")
            continue
            
        new_edge = true_hybrid_fair - ask
        
        print(f"\n{signal_ts.strftime('%H:%M:%S')} | Match {match_id} | {event} ({side})")
        print(f"  Old Broken Fair: {old_fair:.3f} | True Fixed Fair: {true_hybrid_fair:.3f}")
        print(f"  Ask Price:       {ask:.3f}")
        print(f"  Old Edge:        {old_fair - ask:.3f} | New Edge:        {new_edge:.3f}")
        
        if new_edge > 0.01:
            print("  >>> WOULD HAVE TRADED! <<<")
            bid_30s = get_markout(books, token, signal_ts, 30)
            bid_60s = get_markout(books, token, signal_ts, 60)
            bid_120s = get_markout(books, token, signal_ts, 120)
            
            pnl_30s = (bid_30s - ask) if bid_30s else 0
            pnl_60s = (bid_60s - ask) if bid_60s else 0
            pnl_120s = (bid_120s - ask) if bid_120s else 0
            
            print(f"  Markouts (Bid): 30s: {bid_30s} | 60s: {bid_60s} | 120s: {bid_120s}")
            print(f"  Simulated PnL:  30s: {pnl_30s:+.3f} | 60s: {pnl_60s:+.3f} | 120s: {pnl_120s:+.3f}")
        else:
            print("  >>> STILL WOULD HAVE SKIPPED <<<")

if __name__ == "__main__":
    main()
