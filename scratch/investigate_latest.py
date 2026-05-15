import csv
from datetime import datetime

with open('logs/signals.csv', 'r') as f:
    signals = list(csv.DictReader(f))

# Filter for the most recent ones (last 20)
print(f"Total signals in log: {len(signals)}")
recent = signals[-20:]

print(f"{'Time':<25} | {'Decision':<15} | {'Skip Reason':<25} | {'Event':<20} | {'Edge':<8} | {'Lag':<8}")
print("-" * 110)
for r in recent:
    edge = r.get('executable_edge', '')
    lag = r.get('lag', '')
    if edge: edge = f"{float(edge):.3f}"
    if lag: lag = f"{float(lag):.3f}"
    
    print(f"{r['timestamp_utc'][:23]:<25} | {r.get('decision', ''):<15} | {r.get('skip_reason', ''):<25} | {r.get('event_type', '')[:20]:<20} | {edge:<8} | {lag:<8}")
