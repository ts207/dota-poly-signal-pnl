import csv
from collections import defaultdict

with open('logs/signals.csv', 'r') as f:
    signals = list(csv.DictReader(f))

# Look at the last 100 signals
recent = signals[-100:]

rejections = defaultdict(list)
for r in recent:
    reason = r.get('skip_reason', '')
    if reason and reason not in ('', 'book_stale', 'missing_book'):
        rejections[reason].append(r)

for reason, items in rejections.items():
    print(f"\n=== Rejection Reason: {reason} (Count: {len(items)}) ===")
    for r in items[-3:]: # Show up to 3 examples
        print(f"Time: {r['timestamp_utc'][:19]} | Match: {r['match_id']} | Event: {r['event_type']}")
        
        # Print relevant fields based on reason
        if reason == 'edge_too_small':
            print(f"  Fair Price: {r.get('fair_price', '')[:5]} | Executable Price: {r.get('executable_price', '')[:5]} | Edge: {r.get('executable_edge', '')[:6]}")
            print(f"  Ask: {r.get('ask', '')[:5]} | Hybrid Fair: {r.get('hybrid_fair', '')[:5]}")
        elif reason == 'spread_too_wide':
            print(f"  Spread: {r.get('spread', '')} | Bid: {r.get('bid', '')} | Ask: {r.get('ask', '')}")
        elif reason == 'already_repriced':
            print(f"  Current Price: {r.get('current_price', '')} | Anchor Price: {r.get('anchor_price', '')} | Market Move: {r.get('market_move_recent', '')}")
        elif reason == 'chasing_terminal_price':
            print(f"  Ask: {r.get('ask', '')}")
        elif reason == 'insufficient_ask_size':
            ask = float(r.get('ask') or 0)
            size = float(r.get('ask_size') or 0)
            print(f"  Ask: {ask} | Ask Size (Shares): {size} | Notional USD: ${ask*size:.2f}")
        else:
            print(f"  Ask: {r.get('ask', '')} | Spread: {r.get('spread', '')} | Edge: {r.get('executable_edge', '')}")
