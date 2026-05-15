
import csv
from pathlib import Path
from collections import defaultdict
from statistics import mean, median

def fnum(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None

def analyze():
    path = Path("logs/signal_markouts.csv")
    if not path.exists():
        print("logs/signal_markouts.csv not found")
        return

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Total signals in markouts log: {len(rows)}")

    stats_by_event = defaultdict(lambda: {"markout_3s": [], "markout_10s": [], "markout_30s": [], "count": 0})
    stats_by_skip = defaultdict(lambda: {"markout_3s": [], "markout_10s": [], "markout_30s": [], "count": 0})

    for row in rows:
        etype = row.get("event_type")
        skip = row.get("skip_reason")
        
        m3 = fnum(row.get("markout_3s"))
        m10 = fnum(row.get("markout_10s"))
        m30 = fnum(row.get("markout_30s"))

        if m3 is not None:
            stats_by_event[etype]["markout_3s"].append(m3)
            stats_by_skip[skip]["markout_3s"].append(m3)
        if m10 is not None:
            stats_by_event[etype]["markout_10s"].append(m10)
            stats_by_skip[skip]["markout_10s"].append(m10)
        if m30 is not None:
            stats_by_event[etype]["markout_30s"].append(m30)
            stats_by_skip[skip]["markout_30s"].append(m30)
        
        stats_by_event[etype]["count"] += 1
        stats_by_skip[skip]["count"] += 1

    print("\n=== Markout Analysis by Event Type ===")
    print(f"{'Event Type':<30} | {'Count':<5} | {'M3 Avg':<8} | {'M10 Avg':<8} | {'M30 Avg':<8} | {'Win% 30s':<8}")
    print("-" * 85)
    for etype, s in sorted(stats_by_event.items(), key=lambda x: x[1]["count"], reverse=True):
        m3_avg = mean(s["markout_3s"]) if s["markout_3s"] else 0
        m10_avg = mean(s["markout_10s"]) if s["markout_10s"] else 0
        m30_avg = mean(s["markout_30s"]) if s["markout_30s"] else 0
        win_rate_30s = len([x for x in s["markout_30s"] if x > 0]) / len(s["markout_30s"]) if s["markout_30s"] else 0
        print(f"{etype:<30} | {s['count']:<5} | {m3_avg:+.4f} | {m10_avg:+.4f} | {m30_avg:+.4f} | {win_rate_30s:.1%}")

    print("\n=== Markout Analysis by Skip Reason ===")
    print(f"{'Skip Reason':<30} | {'Count':<5} | {'M3 Avg':<8} | {'M10 Avg':<8} | {'M30 Avg':<8} | {'Win% 30s':<8}")
    print("-" * 85)
    for skip, s in sorted(stats_by_skip.items(), key=lambda x: x[1]["count"], reverse=True):
        m3_avg = mean(s["markout_3s"]) if s["markout_3s"] else 0
        m10_avg = mean(s["markout_10s"]) if s["markout_10s"] else 0
        m30_avg = mean(s["markout_30s"]) if s["markout_30s"] else 0
        win_rate_30s = len([x for x in s["markout_30s"] if x > 0]) / len(s["markout_30s"]) if s["markout_30s"] else 0
        print(f"{str(skip):<30} | {s['count']:<5} | {m3_avg:+.4f} | {m10_avg:+.4f} | {m30_avg:+.4f} | {win_rate_30s:.1%}")

if __name__ == "__main__":
    analyze()
