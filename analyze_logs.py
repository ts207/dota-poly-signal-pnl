from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from config import CSV_LOG_PATH, PAPER_TRADES_CSV_PATH, DOTA_EVENTS_CSV_PATH, BOOK_EVENTS_CSV_PATH, POSITIONS_CSV_PATH, PNL_SUMMARY_CSV_PATH

SIGNALS = Path(CSV_LOG_PATH)
TRADES = Path(PAPER_TRADES_CSV_PATH)
DOTA_EVENTS = Path(DOTA_EVENTS_CSV_PATH)
BOOK_EVENTS = Path(BOOK_EVENTS_CSV_PATH)
REACTION = Path("logs/reaction_lag.csv")
POSITIONS = Path(POSITIONS_CSV_PATH)
PNL_SUMMARY = Path(PNL_SUMMARY_CSV_PATH)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fnum(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except ValueError:
        return None


def print_counter(title: str, counter: Counter, limit: int = 12):
    print(f"\n{title}")
    if not counter:
        print("  none")
        return
    for key, val in counter.most_common(limit):
        print(f"  {key}: {val}")


def fmt_avg(values: list[float]) -> str:
    if not values:
        return "n/a"
    return f"avg={sum(values)/len(values):.4f}, median={median(values):.4f}, min={min(values):.4f}, max={max(values):.4f}"


def is_live_position_log(rows: list[dict]) -> bool:
    return any((row.get("action") or "").strip().lower() in {"entry", "exit"} for row in rows)


def main():
    signals = read_csv(SIGNALS)
    trades = read_csv(TRADES)
    dota_events = read_csv(DOTA_EVENTS)
    book_events = read_csv(BOOK_EVENTS)
    reaction = read_csv(REACTION)
    positions = read_csv(POSITIONS)
    pnl_summary = read_csv(PNL_SUMMARY)

    print(f"signals: {len(signals)}")
    print(f"paper trade rows: {len(trades)}")
    print(f"dota events: {len(dota_events)}")
    print(f"book events: {len(book_events)}")
    print(f"reaction rows: {len(reaction)}")
    print(f"positions: {len(positions)}")
    print(f"pnl summary rows: {len(pnl_summary)}")

    if signals:
        decisions = Counter(row.get("decision") for row in signals)
        skips = Counter(row.get("skip_reason") for row in signals if row.get("decision") == "skip")
        print_counter("decisions", decisions)
        print_counter("skip reasons", skips)

        edges = [fnum(row.get("edge")) for row in signals]
        edges = [x for x in edges if x is not None]
        if edges:
            print(f"\nlogged edge: {fmt_avg(edges)}")

        steam_ages = [fnum(row.get("steam_age_ms")) for row in signals]
        steam_ages = [x for x in steam_ages if x is not None]
        book_ages = [fnum(row.get("book_age_ms")) for row in signals]
        book_ages = [x for x in book_ages if x is not None]
        if steam_ages:
            print(f"steam age ms: {fmt_avg(steam_ages)}")
        if book_ages:
            print(f"book age ms: {fmt_avg(book_ages)}")

    if dota_events:
        print_counter("dota event types", Counter(row.get("event_type") for row in dota_events))
        print_counter("dota event severities", Counter(row.get("severity") for row in dota_events))

    if book_events:
        spreads = [fnum(row.get("spread")) for row in book_events]
        spreads = [x for x in spreads if x is not None]
        if spreads:
            print(f"\nbook spread: {fmt_avg(spreads)}")

    if trades and is_live_position_log(trades):
        entries = [r for r in trades if (r.get("action") or "").strip().lower() == "entry"]
        exits = [r for r in trades if (r.get("action") or "").strip().lower() == "exit"]
        open_count = max(len(entries) - len(exits), 0)
        realized = [fnum(r.get("pnl_usd")) for r in exits]
        realized = [x for x in realized if x is not None]
        costs = [fnum(r.get("cost_usd")) for r in entries]
        costs = [x for x in costs if x is not None]
        print("\nlive paper position log")
        print(f"  entries={len(entries)}, exits={len(exits)}, inferred_open={open_count}")
        if costs:
            print(f"  entry notional: ${sum(costs):.2f}")
        if realized:
            wins = len([x for x in realized if x > 0])
            print(f"  realized PnL: total=${sum(realized):.2f}, win_rate={wins/len(realized):.1%}, {fmt_avg(realized)}")

    elif trades:
        by_scenario = defaultdict(list)
        for row in trades:
            by_scenario[row.get("scenario_ms")].append(row)
        print("\nfill simulation by latency scenario")
        for scenario, rows in sorted(by_scenario.items(), key=lambda kv: int(kv[0] or 0)):
            filled = [r for r in rows if fnum(r.get("filled_usd")) and fnum(r.get("filled_usd")) > 0]
            total_filled = sum(fnum(r.get("filled_usd")) or 0 for r in filled)
            print(f"  {scenario} ms: {len(filled)}/{len(rows)} filled, ${total_filled:.2f}")


    if positions:
        print("\nposition marking")
        pnl_values = [fnum(row.get("unrealized_pnl_usd")) for row in positions]
        pnl_values = [x for x in pnl_values if x is not None]
        notionals = [fnum(row.get("notional_usd")) for row in positions]
        notionals = [x for x in notionals if x is not None]
        if pnl_values:
            print(f"  bid-mark PnL: total=${sum(pnl_values):.2f}, {fmt_avg(pnl_values)}")
        if notionals:
            print(f"  open notional: ${sum(notionals):.2f}")
        print_counter("positions by scenario", Counter(row.get("scenario_ms") for row in positions))

    if pnl_summary:
        print("\nPnL summary by scenario")
        for row in pnl_summary:
            label = row.get("scenario_ms")
            pnl = fnum(row.get("unrealized_pnl_usd"))
            notional = fnum(row.get("notional_usd"))
            fill_rate = fnum(row.get("fill_rate"))
            pct = fnum(row.get("unrealized_pnl_pct"))
            pnl_txt = "n/a" if pnl is None else f"${pnl:.2f}"
            notional_txt = "n/a" if notional is None else f"${notional:.2f}"
            fill_txt = "n/a" if fill_rate is None else f"{fill_rate:.1%}"
            pct_txt = "n/a" if pct is None else f"{pct:.1%}"
            print(f"  {label} ms: attempts={row.get('attempts')}, filled={row.get('filled')}, fill_rate={fill_txt}, notional={notional_txt}, PnL={pnl_txt} ({pct_txt})")

    if reaction:
        print("\nreaction-lag summary")
        expected = [fnum(r.get("time_to_expected_ask_move_s")) for r in reaction]
        expected = [x for x in expected if x is not None]
        any_move = [fnum(r.get("time_to_any_ask_move_s")) for r in reaction]
        any_move = [x for x in any_move if x is not None]
        spread = [fnum(r.get("time_to_spread_widen_s")) for r in reaction]
        spread = [x for x in spread if x is not None]
        liq = [fnum(r.get("time_to_ask_liquidity_drop_s")) for r in reaction]
        liq = [x for x in liq if x is not None]
        print(f"  any ask move: {len(any_move)}/{len(reaction)}" + (f", median={median(any_move):.3f}s" if any_move else ""))
        print(f"  expected ask move: {len(expected)}/{len(reaction)}" + (f", median={median(expected):.3f}s" if expected else ""))
        print(f"  spread widen: {len(spread)}/{len(reaction)}" + (f", median={median(spread):.3f}s" if spread else ""))
        print(f"  ask liquidity drop: {len(liq)}/{len(reaction)}" + (f", median={median(liq):.3f}s" if liq else ""))


if __name__ == "__main__":
    main()
