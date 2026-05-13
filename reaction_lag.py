from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median

from config import DOTA_EVENTS_CSV_PATH, BOOK_EVENTS_CSV_PATH, REACTION_WINDOW_SECONDS, BOOK_MOVE_MIN_CENTS
from storage import RAW_SNAPSHOTS_CSV_PATH
from team_utils import norm_team

OUTPUT = Path("logs/reaction_lag.csv")
RAW_LAG_OUTPUT = Path("logs/raw_lag.csv")


def read_csv(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_ts(value: str | None):
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def fnum(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except ValueError:
        return None


def event_favors_yes(event: dict) -> bool | None:
    direction = (event.get("direction") or "").strip().casefold()
    yes_team = norm_team(event.get("yes_team"))
    radiant_team = norm_team(event.get("radiant_team"))
    dire_team = norm_team(event.get("dire_team"))

    if direction not in {"radiant", "dire"}:
        return None
    if not yes_team:
        return None

    if direction == "radiant" and yes_team == radiant_team:
        return True
    if direction == "dire" and yes_team == dire_team:
        return True
    if direction == "radiant" and yes_team == dire_team:
        return False
    if direction == "dire" and yes_team == radiant_team:
        return False
    return None


def seconds_between(a, b) -> float | None:
    if a is None or b is None:
        return None
    return (b - a).total_seconds()


def first_after(rows: list[dict], event_ts, predicate, window_seconds: int):
    for row in rows:
        ts = row.get("_ts")
        if ts is None or event_ts is None:
            continue
        dt = (ts - event_ts).total_seconds()
        if dt < 0:
            continue
        if dt > window_seconds:
            break
        if predicate(row):
            return row, dt
    return None, None


def latest_before(rows: list[dict], event_ts):
    last = None
    for row in rows:
        ts = row.get("_ts")
        if ts is None or event_ts is None:
            continue
        if ts <= event_ts:
            last = row
        else:
            break
    return last


def analyze_reaction_lag(events: list[dict], books: list[dict]) -> list[dict]:
    by_asset: dict[str, list[dict]] = defaultdict(list)
    for row in books:
        row["_ts"] = parse_ts(row.get("timestamp_utc"))
        asset_id = str(row.get("asset_id") or "")
        if asset_id and row["_ts"]:
            by_asset[asset_id].append(row)

    for rows in by_asset.values():
        rows.sort(key=lambda r: r["_ts"])

    results: list[dict] = []
    move = BOOK_MOVE_MIN_CENTS

    for event in events:
        event_ts = parse_ts(event.get("timestamp_utc"))
        asset_id = str(event.get("yes_token_id") or "")
        rows = by_asset.get(asset_id, [])
        if not event_ts or not asset_id or not rows:
            continue

        before = latest_before(rows, event_ts)
        if not before:
            continue

        base_ask = fnum(before.get("best_ask"))
        base_bid = fnum(before.get("best_bid"))
        base_spread = fnum(before.get("spread"))
        base_size = fnum(before.get("ask_size"))
        favors_yes = event_favors_yes(event)

        any_ask_move_row, any_ask_move_s = first_after(
            rows,
            event_ts,
            lambda r: base_ask is not None and fnum(r.get("best_ask")) is not None and abs(fnum(r.get("best_ask")) - base_ask) >= move,
            REACTION_WINDOW_SECONDS,
        )

        if favors_yes is True:
            expected_predicate = lambda r: base_ask is not None and fnum(r.get("best_ask")) is not None and fnum(r.get("best_ask")) >= base_ask + move
        elif favors_yes is False:
            expected_predicate = lambda r: base_ask is not None and fnum(r.get("best_ask")) is not None and fnum(r.get("best_ask")) <= base_ask - move
        else:
            expected_predicate = lambda r: False

        expected_row, expected_s = first_after(rows, event_ts, expected_predicate, REACTION_WINDOW_SECONDS)

        spread_row, spread_s = first_after(
            rows,
            event_ts,
            lambda r: base_spread is not None and fnum(r.get("spread")) is not None and fnum(r.get("spread")) >= base_spread + move,
            REACTION_WINDOW_SECONDS,
        )

        size_row, size_s = first_after(
            rows,
            event_ts,
            lambda r: base_size is not None and base_size > 0 and fnum(r.get("ask_size")) is not None and fnum(r.get("ask_size")) <= base_size * 0.75,
            REACTION_WINDOW_SECONDS,
        )

        final_ask = fnum(expected_row.get("best_ask")) if expected_row else None
        final_bid = fnum(expected_row.get("best_bid")) if expected_row else None

        results.append({
            "event_timestamp_utc": event.get("timestamp_utc"),
            "asset_id": asset_id,
            "mapping_name": event.get("mapping_name"),
            "event_type": event.get("event_type"),
            "severity": event.get("severity"),
            "game_time_sec": event.get("game_time_sec"),
            "direction": event.get("direction"),
            "yes_team": event.get("yes_team"),
            "favors_yes": favors_yes,
            "base_bid": base_bid,
            "base_ask": base_ask,
            "base_spread": base_spread,
            "base_ask_size": base_size,
            "time_to_any_ask_move_s": any_ask_move_s,
            "time_to_expected_ask_move_s": expected_s,
            "time_to_spread_widen_s": spread_s,
            "time_to_ask_liquidity_drop_s": size_s,
            "expected_move_bid": final_bid,
            "expected_move_ask": final_ask,
        })

    return results


def write_reaction_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "event_timestamp_utc", "asset_id", "mapping_name", "event_type", "severity", "game_time_sec",
        "direction", "yes_team", "favors_yes", "base_bid", "base_ask", "base_spread", "base_ask_size",
        "time_to_any_ask_move_s", "time_to_expected_ask_move_s", "time_to_spread_widen_s",
        "time_to_ask_liquidity_drop_s", "expected_move_bid", "expected_move_ask",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h) for h in headers})


def print_summary(rows: list[dict]):
    print(f"reaction rows: {len(rows)}")
    if not rows:
        return

    expected = [fnum(r.get("time_to_expected_ask_move_s")) for r in rows]
    expected = [x for x in expected if x is not None]
    any_moves = [fnum(r.get("time_to_any_ask_move_s")) for r in rows]
    any_moves = [x for x in any_moves if x is not None]
    spread = [fnum(r.get("time_to_spread_widen_s")) for r in rows]
    spread = [x for x in spread if x is not None]
    liq = [fnum(r.get("time_to_ask_liquidity_drop_s")) for r in rows]
    liq = [x for x in liq if x is not None]

    print(f"any ask move within window: {len(any_moves)}/{len(rows)}")
    if any_moves:
        print(f"  median time to any ask move: {median(any_moves):.3f}s")
    print(f"expected-direction ask move within window: {len(expected)}/{len(rows)}")
    if expected:
        print(f"  median time to expected move: {median(expected):.3f}s")
    print(f"spread widened within window: {len(spread)}/{len(rows)}")
    if spread:
        print(f"  median time to spread widen: {median(spread):.3f}s")
    print(f"ask liquidity dropped within window: {len(liq)}/{len(rows)}")
    if liq:
        print(f"  median time to liquidity drop: {median(liq):.3f}s")


def analyze_raw_lag(snapshots: list[dict], books: list[dict], mappings_yaml: str = "markets.yaml") -> list[dict]:
    """Measure lag from exact Steam API snapshot timestamps to Polymarket book moves.

    For each snapshot where radiant_lead changes, find the first Polymarket book
    move on the corresponding token and record the exact elapsed seconds.
    This is the DLTV-style analysis: anchored on real API update times, not
    event-detection thresholds.
    """
    import yaml

    try:
        with open(mappings_yaml) as f:
            mdata = yaml.safe_load(f) or {}
        markets = mdata.get("markets", [])
    except FileNotFoundError:
        return []

    # Build match_id → token mapping
    match_to_tokens: dict[str, dict] = {}
    for m in markets:
        mid = str(m.get("dota_match_id") or "")
        if mid and mid not in ("", "STEAM_MATCH_OR_LOBBY_ID_HERE"):
            match_to_tokens[mid] = {
                "yes_token_id": str(m.get("yes_token_id", "")),
                "no_token_id":  str(m.get("no_token_id", "")),
                "yes_team":     m.get("yes_team", ""),
                "name":         m.get("name", ""),
            }

    # Book lookup by asset_id
    by_asset: dict[str, list[dict]] = defaultdict(list)
    for row in books:
        row["_ts"] = parse_ts(row.get("timestamp_utc"))
        aid = str(row.get("asset_id") or "")
        if aid and row["_ts"]:
            by_asset[aid].append(row)
    for rows in by_asset.values():
        rows.sort(key=lambda r: r["_ts"])

    # Parse snapshots, attach timestamp
    for s in snapshots:
        s["_ts"] = parse_ts(s.get("received_at_utc"))

    snaps_by_match: dict[str, list[dict]] = defaultdict(list)
    for s in snapshots:
        if s["_ts"]:
            snaps_by_match[str(s.get("match_id") or "")].append(s)
    for rows in snaps_by_match.values():
        rows.sort(key=lambda r: r["_ts"])

    results = []
    move = BOOK_MOVE_MIN_CENTS

    for match_id, snaps in snaps_by_match.items():
        tokens = match_to_tokens.get(match_id)
        if not tokens:
            continue

        yes_id  = tokens["yes_token_id"]
        yes_rows = by_asset.get(yes_id, [])
        if not yes_rows:
            continue

        prev_lead = None
        for snap in snaps:
            lead = fnum(snap.get("radiant_lead"))
            ts   = snap["_ts"]
            if lead is None or ts is None:
                continue

            nw_delta = (lead - prev_lead) if prev_lead is not None else None
            prev_lead = lead

            if nw_delta is None or abs(nw_delta) < 300:
                continue  # only log meaningful NW changes

            # What was the YES ask just before this snapshot?
            before = latest_before(yes_rows, ts)
            if not before:
                continue
            base_ask = fnum(before.get("best_ask"))

            # Find first ask move ≥ move threshold after snapshot timestamp
            any_row, any_s = first_after(yes_rows, ts,
                lambda r: base_ask is not None
                    and fnum(r.get("best_ask")) is not None
                    and abs(fnum(r.get("best_ask")) - base_ask) >= move,
                REACTION_WINDOW_SECONDS)

            results.append({
                "snapshot_utc":     snap.get("received_at_utc"),
                "match_id":         match_id,
                "market_name":      tokens["name"],
                "game_time_sec":    snap.get("game_time_sec"),
                "radiant_lead":     lead,
                "nw_delta":         round(nw_delta),
                "base_ask":         base_ask,
                "moved_ask":        fnum(any_row.get("best_ask")) if any_row else None,
                "ask_delta":        round(fnum(any_row.get("best_ask")) - base_ask, 4) if any_row and base_ask else None,
                "lag_s":            round(any_s, 3) if any_s is not None else None,
                "data_source":      snap.get("data_source"),
            })

    return results


def write_raw_lag_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "snapshot_utc", "match_id", "market_name", "game_time_sec",
        "radiant_lead", "nw_delta", "base_ask", "moved_ask", "ask_delta", "lag_s", "data_source",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({h: row.get(h) for h in headers})


def print_raw_lag_summary(rows: list[dict]):
    filled = [r for r in rows if r.get("lag_s") is not None]
    lags = [r["lag_s"] for r in filled]
    print(f"\nRaw snapshot lag ({len(filled)}/{len(rows)} NW deltas got a book move within window):")
    if lags:
        lags_s = sorted(lags)
        print(f"  min={lags_s[0]:.1f}s  p25={lags_s[len(lags_s)//4]:.1f}s  "
              f"median={lags_s[len(lags_s)//2]:.1f}s  p75={lags_s[3*len(lags_s)//4]:.1f}s  "
              f"max={lags_s[-1]:.1f}s")
    no_move = [r for r in rows if r.get("lag_s") is None]
    if no_move:
        print(f"  {len(no_move)} NW deltas: no book move within {REACTION_WINDOW_SECONDS}s window")


def write_dynamic_csv(path: Path, rows: list[dict]):
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


STALE_ASK_OUTPUT = Path("logs/stale_ask_survival.csv")
MARKOUTS_OUTPUT = Path("logs/markouts.csv")


def estimate_stale_ask_survival(signals_path: str | Path, books_path: str | Path, output_path: str | Path):
    signals = read_csv(signals_path)
    books = read_csv(books_path)
    
    # Pre-parse book timestamps
    for b in books:
        b["_ts"] = parse_ts(b.get("timestamp_utc"))
    
    # Sort books by timestamp for efficient searching
    books.sort(key=lambda x: x["_ts"])
    
    # Group books by asset_id
    books_by_asset = defaultdict(list)
    for b in books:
        asset_id = b.get("asset_id") or b.get("token_id")
        if asset_id:
            books_by_asset[asset_id].append(b)

    results = []
    if signals and books:
        for sig in signals:
            if sig.get("decision") not in {"live_attempt_result", "paper_entry_result"}:
                continue
            
            # Skip results rows to avoid double counting, but use them for paper_entry_result specific data
            if sig.get("decision") == "paper_entry_result" and sig.get("paper_entry_result") != "filled":
                continue

            ts = parse_ts(sig.get("timestamp_utc"))
            if not ts:
                continue
                
            token_id = sig.get("token_id")
            if not token_id:
                continue
                
            executable_price = fnum(sig.get("executable_price"))
            initial_ask = fnum(sig.get("ask")) or fnum(sig.get("best_ask"))
            initial_ask_size = fnum(sig.get("ask_size"))
            initial_spread = fnum(sig.get("spread"))
            
            if executable_price is None or initial_ask is None:
                continue
                
            asset_books = books_by_asset.get(token_id, [])
            
            time_until_ask_above_executable = None
            time_until_ask_size_75pct_drop = None
            time_until_spread_widens_1c = None
            
            for b in asset_books:
                bts = b["_ts"]
                if bts is None or bts <= ts:
                    continue
                dt = (bts - ts).total_seconds()
                if dt > REACTION_WINDOW_SECONDS:
                    break
                    
                current_ask = fnum(b.get("best_ask"))
                current_ask_size = fnum(b.get("ask_size"))
                current_spread = fnum(b.get("spread"))
                
                if time_until_ask_above_executable is None and current_ask is not None:
                    if current_ask > executable_price:
                        time_until_ask_above_executable = dt
                        
                if time_until_ask_size_75pct_drop is None and current_ask_size is not None and initial_ask_size:
                    if current_ask_size <= initial_ask_size * 0.25:
                        time_until_ask_size_75pct_drop = dt
                        
                if time_until_spread_widens_1c is None and current_spread is not None and initial_spread is not None:
                    if current_spread >= initial_spread + 0.01:
                        time_until_spread_widens_1c = dt
                        
            results.append({
                "timestamp_utc": sig.get("timestamp_utc"),
                "match_id": sig.get("match_id"),
                "token_id": token_id,
                "event_type": sig.get("event_type"),
                "executable_price": executable_price,
                "initial_ask": initial_ask,
                "initial_ask_size": initial_ask_size,
                "initial_spread": initial_spread,
                "stale_ask_survival_ms": round(time_until_ask_above_executable * 1000, 1) if time_until_ask_above_executable is not None else None,
                "time_until_ask_above_limit": time_until_ask_above_executable,
                "time_until_ask_size_drops": time_until_ask_size_75pct_drop,
                "time_until_spread_widens": time_until_spread_widens_1c,
                "time_until_ask_above_executable_price": time_until_ask_above_executable,
                "time_until_ask_size_75pct_drop": time_until_ask_size_75pct_drop,
                "time_until_spread_widens_1c": time_until_spread_widens_1c,
            })

    if results:
        write_dynamic_csv(Path(output_path), results)
        print(f"wrote survival: {output_path}")
    else:
        # Write empty file with header to satisfy tests/checkers
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "timestamp_utc", "match_id", "token_id", "event_type",
                "executable_price", "initial_ask", "initial_ask_size", "initial_spread",
                "stale_ask_survival_ms", "time_until_ask_above_limit",
                "time_until_ask_size_drops", "time_until_spread_widens",
                "time_until_ask_above_executable_price", "time_until_ask_size_75pct_drop",
                "time_until_spread_widens_1c"
            ])
            writer.writeheader()
        print(f"wrote empty survival: {output_path}")


def estimate_markouts(signals_path: str | Path, books_path: str | Path, output_path: str | Path):
    signals = read_csv(signals_path)
    books = read_csv(books_path)
    for b in books:
        b["_ts"] = parse_ts(b.get("timestamp_utc"))

    books_by_asset = defaultdict(list)
    for b in books:
        asset_id = b.get("asset_id") or b.get("token_id")
        if asset_id and b["_ts"]:
            books_by_asset[asset_id].append(b)
    for rows in books_by_asset.values():
        rows.sort(key=lambda r: r["_ts"])

    rows = []
    for sig in signals:
        if sig.get("decision") not in {"paper_entry_result", "live_attempt_result"}:
            continue
        if sig.get("paper_entry_result") and sig.get("paper_entry_result") != "filled":
            continue
        token_id = sig.get("token_id")
        ts = parse_ts(sig.get("timestamp_utc"))
        if not token_id or not ts:
            continue
        reference = fnum(sig.get("paper_fill_price")) or fnum(sig.get("live_avg_fill_price")) or fnum(sig.get("executable_price"))
        if reference is None:
            continue
        asset_books = books_by_asset.get(token_id, [])
        markouts = {}
        for delay in (3, 10, 30):
            row = _first_book_at_or_after(asset_books, ts, delay)
            mid = _book_mid(row) if row else None
            markouts[f"markout_{delay}s"] = round(mid - reference, 4) if mid is not None else None
        rows.append({
            "timestamp_utc": sig.get("timestamp_utc"),
            "match_id": sig.get("match_id"),
            "market_name": sig.get("market_name"),
            "token_id": token_id,
            "event_type": sig.get("event_type"),
            "reference_price": reference,
            "markout_3s": markouts.get("markout_3s"),
            "markout_10s": markouts.get("markout_10s"),
            "markout_30s": markouts.get("markout_30s"),
        })

    headers = [
        "timestamp_utc", "match_id", "market_name", "token_id", "event_type",
        "reference_price", "markout_3s", "markout_10s", "markout_30s",
    ]
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h) for h in headers})
    print(f"wrote markouts: {output_path}")


def _first_book_at_or_after(rows: list[dict], ts, delay_seconds: int) -> dict | None:
    target = ts.timestamp() + delay_seconds
    best = None
    for row in rows:
        rts = row.get("_ts")
        if not rts:
            continue
        if rts.timestamp() >= target:
            best = row
            break
    return best


def _book_mid(row: dict | None) -> float | None:
    if not row:
        return None
    bid = fnum(row.get("best_bid"))
    ask = fnum(row.get("best_ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return ask if ask is not None else bid


def main():
    from config import CSV_LOG_PATH
    events    = read_csv(DOTA_EVENTS_CSV_PATH)
    books     = read_csv(BOOK_EVENTS_CSV_PATH)
    snapshots = read_csv(RAW_SNAPSHOTS_CSV_PATH)

    print(f"dota events:    {len(events)}")
    print(f"book events:    {len(books)}")
    print(f"raw snapshots:  {len(snapshots)}")

    rows = analyze_reaction_lag(events, books)
    write_reaction_csv(OUTPUT, rows)
    print_summary(rows)
    print(f"wrote: {OUTPUT}")

    if snapshots:
        raw_rows = analyze_raw_lag(snapshots, books)
        write_raw_lag_csv(RAW_LAG_OUTPUT, raw_rows)
        print_raw_lag_summary(raw_rows)
        print(f"wrote: {RAW_LAG_OUTPUT}")

    from config import LATENCY_CSV_PATH, MARKOUTS_CSV_PATH
    estimate_stale_ask_survival(LATENCY_CSV_PATH, BOOK_EVENTS_CSV_PATH, STALE_ASK_OUTPUT)
    estimate_markouts(LATENCY_CSV_PATH, BOOK_EVENTS_CSV_PATH, MARKOUTS_CSV_PATH)


if __name__ == "__main__":
    main()
