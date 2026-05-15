import csv
from datetime import datetime

with open('dota-poly-signal-pnl/logs/latency.csv') as f:
    latency = list(csv.DictReader(f))

with open('dota-poly-signal-pnl/logs/book_refresh_rescue.csv') as f:
    rescue = list(csv.DictReader(f))

# Join by timestamp_utc
latency_by_ts = {r['timestamp_utc']: r for r in latency}

for r in rescue:
    ts = r['timestamp_utc']
    lat = latency_by_ts.get(ts)
    if not lat: continue
    
    print(f"\n--- Signal at {ts} ---")
    print(f"Event: {r['event_type']} (Match: {r['match_id']})")
    print(f"Rescue Decision: {r['fresh_decision']} (Reason: {r['fresh_skip_reason']})")
    
    steam_delay = float(lat['steam_source_update_age_sec']) if lat['steam_source_update_age_sec'] else 0
    detect_lat = float(lat['event_detection_latency_ms']) if lat['event_detection_latency_ms'] else 0
    
    print(f"Steam API internal delay: {steam_delay:.1f} sec")
    print(f"Our processing delay: {detect_lat:.1f} ms")
    print(f"Local Ask at signal: {r['local_ask']} (book was {int(r['local_book_age_ms'])/1000:.1f}s old)")
    print(f"Fresh Ask fetched:   {r['fresh_ask']}")
    if r['local_to_fresh_ask_change']:
        change = float(r['local_to_fresh_ask_change'])
        print(f"Market changed by:   {change:+.2f}")
    
    print(f"Fair Price evaluated: {lat['fair_price']}")
    print(f"Hybrid Fair Price: {lat['hybrid_fair']}")
