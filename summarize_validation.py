#!/usr/bin/env python3
"""Summarize event validation results across 0ms/500ms/1000ms delay runs.

For each run directory, reads signals.csv, latency.csv, paper_trades.csv,
and book_refresh_rescue.csv to produce the specific analysis requested:

- Tier A/B candidate count
- paper entry count
- skip reason distribution
- book_age_at_signal_ms
- stale_ask_survival_ms
- 3s / 10s / 30s markout
- bid-marked PnL
- paper PnL at 0ms vs 500ms vs 1000ms
"""
from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path
from statistics import median

TIER_A_EVENTS = {
    "THRONE_EXPOSED", "SECOND_T4_TOWER_FALL",
    "OBJECTIVE_CONVERSION_T4", "T3_PLUS_T4_CHAIN",
    "OBJECTIVE_CONVERSION_T3",
}
TIER_B_EVENTS = {
    "LATE_MAJOR_COMEBACK_REPRICE", "CHAINED_LATE_FIGHT_RECOVERY",
    "LATE_ECONOMIC_CRASH", "ULTRA_LATE_WIPE_CONFIRMED",
    "STOMP_THROW_WITH_OBJECTIVE_RISK", "ULTRA_LATE_WIPE",
    "LATE_GAME_WIPE", "STOMP_THROW", "MAJOR_COMEBACK",
    "FIRST_T4_TOWER_FALL", "ALL_T3_TOWERS_DOWN",
    "MULTI_STRUCTURE_COLLAPSE", "MULTIPLE_T3_TOWERS_DOWN",
}
INSPECT_EVENTS = TIER_A_EVENTS | TIER_B_EVENTS


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fnum(v):
    try:
        if v in (None, ""):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def summarize_run(run_dir: Path):
    label = run_dir.name
    signals = read_csv(run_dir / "logs" / "signals.csv")
    trades = read_csv(run_dir / "logs" / "paper_trades.csv")
    latency = read_csv(run_dir / "logs" / "latency.csv")
    rescue = read_csv(run_dir / "logs" / "book_refresh_rescue.csv")
    positions = read_csv(run_dir / "logs" / "positions.csv")

    # Extract delay from path
    delay = "?"
    if "0ms" in label:
        delay = "0ms"
    elif "500ms" in label:
        delay = "500ms"
    elif "1000ms" in label:
        delay = "1000ms"

    print(f"\n{'='*60}")
    print(f"  RUN: {label}  (delay={delay})")
    print(f"{'='*60}")

    # Tier A/B candidate count
    ab_signals = []
    for s in signals:
        et = s.get("event_type") or s.get("cluster_event_types") or ""
        parts = et.split("+")
        if any(p in INSPECT_EVENTS for p in parts):
            ab_signals.append(s)

    # Paper entry count
    entries = [t for t in trades if (t.get("action") or "").strip().lower() == "entry"]
    filled = [t for t in trades if t.get("paper_entry_result") == "filled"]

    # Skip reason distribution
    all_skips = Counter()
    ab_skips = Counter()
    for s in signals:
        d = s.get("decision", "")
        r = s.get("skip_reason", "")
        if d == "skip" and r:
            all_skips[r] += 1
    for s in ab_signals:
        d = s.get("decision", "")
        r = s.get("skip_reason", "")
        if d == "skip" and r:
            ab_skips[r] += 1

    # book_age_at_signal for A/B signals
    ab_book_ages = [fnum(s.get("book_age_ms") or s.get("book_age_at_signal_ms")) for s in ab_signals]
    ab_book_ages = [x for x in ab_book_ages if x is not None]

    # Stale ask survival from latency
    survival_rows = read_csv(run_dir / "logs" / "stale_ask_survival.csv")
    sa_ms = [fnum(r.get("stale_ask_survival_ms")) for r in survival_rows if r.get("stale_ask_survival_ms")]
    sa_ms = [x for x in sa_ms if x is not None]

    # Markouts from latency (paper_entry_result rows)
    m3 = [fnum(r.get("markout_3s")) for r in latency if r.get("decision") == "paper_entry_result"]
    m3 = [x for x in m3 if x is not None]
    m10 = [fnum(r.get("markout_10s")) for r in latency if r.get("decision") == "paper_entry_result"]
    m10 = [x for x in m10 if x is not None]
    m30 = [fnum(r.get("markout_30s")) for r in latency if r.get("decision") == "paper_entry_result"]
    m30 = [x for x in m30 if x is not None]

    # Bid-marked PnL from positions
    pnl_vals = [fnum(p.get("unrealized_pnl_usd")) for p in positions]
    pnl_vals = [x for x in pnl_vals if x is not None]

    # Rescue analysis
    print(f"\n  Total signals:          {len(signals)}")
    print(f"  Tier A/B candidates:   {len(ab_signals)}")
    print(f"  Paper entries:          {len(entries)}")
    print(f"  Paper fills:            {len(filled)}")
    print(f"\n  All skip reasons:")
    for reason, cnt in all_skips.most_common(10):
        print(f"    {reason}: {cnt}")
    print(f"\n  Tier A/B skip reasons:")
    for reason, cnt in ab_skips.most_common(10):
        print(f"    {reason}: {cnt}")

    if ab_book_ages:
        print(f"\n  A/B book_age_at_signal_ms: min={min(ab_book_ages):.0f} median={median(ab_book_ages):.0f} max={max(ab_book_ages):.0f}")
    else:
        print(f"\n  A/B book_age_at_signal_ms: (none)")

    if sa_ms:
        print(f"  stale_ask_survival_ms:   min={min(sa_ms):.0f} median={median(sa_ms):.0f} max={max(sa_ms):.0f}")
    else:
        print(f"  stale_ask_survival_ms:   (none)")

    if m3:
        print(f"  3s markout:  n={len(m3)} avg={sum(m3)/len(m3):.4f} median={median(m3):.4f}")
    else:
        print(f"  3s markout:  (none)")
    if m10:
        print(f"  10s markout: n={len(m10)} avg={sum(m10)/len(m10):.4f} median={median(m10):.4f}")
    else:
        print(f"  10s markout: (none)")
    if m30:
        print(f"  30s markout: n={len(m30)} avg={sum(m30)/len(m30):.4f} median={median(m30):.4f}")
    else:
        print(f"  30s markout: (none)")

    if pnl_vals:
        print(f"  bid-mark PnL: total=${sum(pnl_vals):.2f} avg=${sum(pnl_vals)/len(pnl_vals):.2f} n={len(pnl_vals)}")
    else:
        print(f"  bid-mark PnL: (none)")

    # Inspect specific events
    print(f"\n  --- Inspecting specific Tier A/B events ---")
    for et_name in sorted(INSPECT_EVENTS):
        matching = [s for s in ab_signals if et_name in (s.get("event_type") or s.get("cluster_event_types") or "")]
        if not matching:
            continue
        for s in matching:
            dec = s.get("decision", "")
            skip = s.get("skip_reason", "")
            ba = s.get("book_age_ms") or s.get("book_age_at_signal_ms") or ""
            edge = s.get("executable_edge", "")
            fair = s.get("fair_price", "")
            ask = s.get("ask", "")
            print(f"    {et_name}: dec={dec} skip={skip} book_age={ba}ms edge={edge} fair={fair} ask={ask}")

    # Rescue-specific analysis
    if rescue:
        print(f"\n  --- Book Refresh Rescue Analysis ---")
        print(f"  Total rescue attempts: {len(rescue)}")
        for r in rescue:
            et = r.get("event_type", "")
            tier = r.get("event_tier", "")
            local_age = r.get("local_book_age_ms", "")
            fresh_dec = r.get("fresh_decision", "")
            fresh_skip = r.get("fresh_skip_reason", "")
            fresh_lat = r.get("refresh_latency_ms", "")
            local_ask = r.get("local_ask", "")
            fresh_ask = r.get("fresh_ask", "")
            ask_change = r.get("local_to_fresh_ask_change", "")
            fresh_edge = r.get("fresh_executable_edge", "")
            print(f"    {et} tier={tier}: local_age={local_age}ms lat={fresh_lat}ms local_ask={local_ask} fresh_ask={fresh_ask} ask_change={ask_change} fresh_dec={fresh_dec} fresh_skip={fresh_skip} fresh_edge={fresh_edge}")
    else:
        print(f"\n  No book refresh rescue data.")


def main():
    base = Path(__file__).parent.parent / "runs"
    dirs = sorted(base.glob("final_event_validation_*"))
    if not dirs:
        print("No run directories found under runs/")
        sys.exit(1)

    for d in dirs:
        summarize_run(d)

    # Cross-run comparison
    print(f"\n{'='*60}")
    print(f"  CROSS-RUN COMPARISON")
    print(f"{'='*60}")
    print(f"  {'Delay':<10} {'A/B Signals':<15} {'Paper Entries':<15} {'Rescue Attempts':<18} {'Fresh Pass':<12}")
    for d in dirs:
        signals = read_csv(d / "logs" / "signals.csv")
        trades = read_csv(d / "logs" / "paper_trades.csv")
        rescue = read_csv(d / "logs" / "book_refresh_rescue.csv")
        ab_count = sum(1 for s in signals if any(p in INSPECT_EVENTS for p in (s.get("event_type") or s.get("cluster_event_types") or "").split("+")))
        entries = len([t for t in trades if (t.get("action") or "").strip().lower() == "entry"])
        filled = len([t for t in trades if t.get("paper_entry_result") == "filled"])
        rescue_count = len(rescue)
        fresh_pass = sum(1 for r in rescue if r.get("fresh_decision") == "paper_buy_yes")
        label = d.name.replace("final_event_validation_", "")
        print(f"  {label:<10} {ab_count:<15} {len(entries) if isinstance(entries, list) else entries:<15} {rescue_count:<18} {fresh_pass:<12}")


if __name__ == "__main__":
    main()