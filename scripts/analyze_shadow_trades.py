from __future__ import annotations

import csv
import os
from collections import defaultdict
from statistics import mean


INPUT = "logs/shadow_trades.csv"
OUT_DIR = "reports"
OUT_CSV = os.path.join(OUT_DIR, "shadow_summary.csv")
OUT_MD = os.path.join(OUT_DIR, "shadow_report.md")


def _float(v):
    if v in (None, ""):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _phase(game_time):
    gt = _float(game_time)
    if gt is None:
        return "unknown"
    minute = gt / 60
    if minute < 20:
        return "early"
    if minute < 35:
        return "mid"
    if minute < 50:
        return "late"
    return "ultra_late"


def _avg(rows, key):
    vals = [_float(r.get(key)) for r in rows]
    vals = [v for v in vals if v is not None]
    return round(mean(vals), 5) if vals else ""


def _win_rate(rows, key):
    vals = [_float(r.get(key)) for r in rows]
    vals = [v for v in vals if v is not None]
    if not vals:
        return ""
    return round(sum(1 for v in vals if v > 0) / len(vals), 4)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(INPUT):
        raise SystemExit(f"missing {INPUT}")

    with open(INPUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    groups = defaultdict(list)
    for row in rows:
        key = (
            row.get("event_type") or "",
            row.get("market_type") or "",
            row.get("proxy_market_type") or "",
            _phase(row.get("game_time_sec")),
            row.get("decision") or "",
            row.get("skip_reason") or "",
        )
        groups[key].append(row)

    headers = [
        "event_type", "market_type", "proxy_market_type", "phase",
        "decision", "skip_reason", "count",
        "avg_markout_3s", "avg_markout_10s", "avg_markout_30s", "avg_markout_60s",
        "win_rate_3s", "win_rate_10s", "win_rate_30s", "win_rate_60s",
        "avg_executable_edge", "avg_lag", "avg_spread",
    ]

    out = []
    for key, gr in sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True):
        event_type, market_type, proxy, phase, decision, skip_reason = key
        out.append({
            "event_type": event_type,
            "market_type": market_type,
            "proxy_market_type": proxy,
            "phase": phase,
            "decision": decision,
            "skip_reason": skip_reason,
            "count": len(gr),
            "avg_markout_3s": _avg(gr, "markout_3s"),
            "avg_markout_10s": _avg(gr, "markout_10s"),
            "avg_markout_30s": _avg(gr, "markout_30s"),
            "avg_markout_60s": _avg(gr, "markout_60s"),
            "win_rate_3s": _win_rate(gr, "markout_3s"),
            "win_rate_10s": _win_rate(gr, "markout_10s"),
            "win_rate_30s": _win_rate(gr, "markout_30s"),
            "win_rate_60s": _win_rate(gr, "markout_60s"),
            "avg_executable_edge": _avg(gr, "executable_edge"),
            "avg_lag": _avg(gr, "lag"),
            "avg_spread": _avg(gr, "spread_at_entry"),
        })

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(out)

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("# Shadow Trade Report\n\n")
        f.write(f"Rows: {len(rows)}\n\n")
        f.write("Top groups:\n\n")
        for row in out[:25]:
            f.write(
                f"- {row['event_type']} {row['market_type']} {row['proxy_market_type']} "
                f"{row['phase']} count={row['count']} "
                f"m10={row['avg_markout_10s']} wr10={row['win_rate_10s']} "
                f"m30={row['avg_markout_30s']} wr30={row['win_rate_30s']}\n"
            )

    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
