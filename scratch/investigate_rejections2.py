import csv

with open('logs/signals.csv', 'r') as f:
    signals = list(csv.DictReader(f))

# Helper to find values safely
def get(row, keys):
    for k in keys:
        if row.get(k): return row[k]
    return "N/A"

for r in signals[-20:]:
    reason = r.get('skip_reason', '')
    if reason and reason not in ('', 'book_stale', 'missing_book'):
        print(f"\n--- {r['timestamp_utc'][:19]} | Match: {r['match_id']} | Event: {r['event_type']} | Side: {r['side']} ---")
        print(f"Reason: {reason}")
        
        if reason == 'edge_too_small':
            print(f"The model calculated Fair Price = {r.get('fair_price')}")
            print(f"But the Executable Price (Ask + slippage) = {r.get('executable_price')}")
            print(f"Edge = Fair ({r.get('fair_price')}) - Executable ({r.get('executable_price')}) = {r.get('executable_edge')}")
            print("Explanation: The model thinks the true probability is much lower than the current asking price, so buying would be a losing bet.")
            
        elif reason == 'spread_too_wide':
            spread = r.get('spread') or get(r, ['markout_3s']) # just trying to find it
            print(f"Spread was recorded as: {r.get('spread')}")
            print("Explanation: The gap between the best Bid and best Ask is too large (wider than the 6 cent MAX_SPREAD). Crossing the spread here guarantees immediate loss of value.")
            
        elif reason == 'already_repriced':
            print(f"Current Price: {r.get('current_price')} | Move: {r.get('move_30s', 'N/A')}")
            print("Explanation: The market has already moved significantly in the direction of the event in the last 30 seconds. The market makers beat the bot to the punch.")
            
        elif reason == 'chasing_terminal_price':
            print("Explanation: The Ask price is >= 97 cents. The game is basically decided and risking $0.97 to make $0.03 is poor risk management.")
