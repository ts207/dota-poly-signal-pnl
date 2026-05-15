import csv
from collections import defaultdict
from datetime import datetime
import statistics

with open('logs/book_events.csv') as f:
    books = list(csv.DictReader(f))

updates_by_token = defaultdict(list)
for b in books:
    token = b['asset_id']
    ts = datetime.fromisoformat(b['timestamp_utc'].replace('Z', '+00:00'))
    updates_by_token[token].append(ts)

print(f"Total book updates received: {len(books)}")
print("\nUpdate frequency by token:")
for token, times in updates_by_token.items():
    times.sort()
    gaps = [(times[i] - times[i-1]).total_seconds() for i in range(1, len(times))]
    if gaps:
        print(f"Token {token[-6:]}: {len(times)} updates, Median gap: {statistics.median(gaps):.2f}s, Max gap: {max(gaps):.2f}s")
    else:
        print(f"Token {token[-6:]}: {len(times)} updates (no gaps to measure)")

