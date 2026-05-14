from __future__ import annotations

import csv
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml


LOGS = Path("logs")
OUTPUT = LOGS / "stale_reject_execution_sim.csv"
STALE_REASONS = {"book_stale", "source_update_stale", "steam_stale"}
MARK_SECONDS = (3, 10, 30, 60, 120)


def read_csvs(paths: list[Path]) -> list[dict]:
    rows = []
    for path in paths:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row["_file"] = str(path)
                rows.append(row)
    return rows


def parse_ts(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def fnum(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def norm(value: str | None) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def latest_before(rows: list[dict], ts: datetime):
    last = None
    for row in rows:
        if row["_ts"] <= ts:
            last = row
        else:
            break
    return last


def first_at_or_after(rows: list[dict], ts: datetime):
    for row in rows:
        if row["_ts"] >= ts:
            return row
    return None


def book_bid(row: dict | None):
    return fnum(row.get("best_bid")) if row else None


def book_ask(row: dict | None):
    return fnum(row.get("best_ask")) if row else None


def book_spread(row: dict | None):
    return fnum(row.get("spread")) if row else None


def book_age_ms(row: dict | None, ts: datetime):
    if not row:
        return None
    return round((ts - row["_ts"]).total_seconds() * 1000, 1)


def load_markets() -> tuple[dict[str, dict], dict[str, list[dict]]]:
    with open("markets.yaml", encoding="utf-8") as f:
        markets = (yaml.safe_load(f) or {}).get("markets", [])
    by_yes = {str(m.get("yes_token_id") or ""): m for m in markets}
    by_match: dict[str, list[dict]] = defaultdict(list)
    for market in markets:
        match_id = str(market.get("dota_match_id") or "")
        if match_id and match_id != "STEAM_MATCH_OR_LOBBY_ID_HERE":
            by_match[match_id].append(market)
    return by_yes, by_match


def market_for_signal(signal: dict, by_yes: dict[str, dict], by_match: dict[str, list[dict]]):
    yes_token = str(signal.get("yes_token_id") or "")
    if yes_token in by_yes:
        return by_yes[yes_token]
    candidates = by_match.get(str(signal.get("match_id") or ""), [])
    market_name = signal.get("market_name")
    market_type = signal.get("market_type")
    for market in candidates:
        if market_type and market.get("market_type") != market_type:
            continue
        if market_name and market.get("name") == market_name:
            return market
    return candidates[0] if candidates else None


def event_favors_yes(signal: dict, market: dict | None) -> bool | None:
    direction = (signal.get("event_direction") or "").strip().lower()
    if direction not in {"radiant", "dire"}:
        return None
    yes_team = norm(signal.get("yes_team") or (market or {}).get("yes_team"))
    radiant = norm(signal.get("radiant_team") or (market or {}).get("steam_radiant_team"))
    dire = norm(signal.get("dire_team") or (market or {}).get("steam_dire_team"))
    if yes_team and radiant and yes_team == radiant:
        return direction == "radiant"
    if yes_team and dire and yes_team == dire:
        return direction == "dire"
    side_map = (market or {}).get("steam_side_mapping")
    if side_map == "normal":
        return direction == "radiant"
    if side_map == "reversed":
        return direction == "dire"
    return None


def dedupe_stale_signals(signals: list[dict]) -> list[dict]:
    rows = [
        s for s in signals
        if s.get("decision") == "skip" and s.get("skip_reason") in STALE_REASONS
    ]
    seen = set()
    out = []
    for row in sorted(rows, key=lambda r: (r.get("timestamp_utc") or "", r.get("_file") or "")):
        key = (
            row.get("timestamp_utc"), row.get("match_id"), row.get("event_type"),
            row.get("event_direction"), row.get("skip_reason"), row.get("market_name"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def stat(values: list[float]) -> str:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return "n/a"
    vals = sorted(vals)
    return (
        f"n={len(vals)} win={sum(v > 0 for v in vals)} loss={sum(v < 0 for v in vals)} "
        f"min={vals[0]:.4f} med={statistics.median(vals):.4f} "
        f"avg={sum(vals) / len(vals):.4f} max={vals[-1]:.4f}"
    )


def main() -> None:
    signal_paths = [LOGS / "signals.csv"] + sorted(LOGS.glob("signals.csv.*.bak")) + sorted(LOGS.glob("archive_20260513_*/signals.csv"))
    book_paths = [LOGS / "book_events.csv"] + sorted(LOGS.glob("archive_20260513_*/book_events.csv"))

    signals = read_csvs(signal_paths)
    stale = dedupe_stale_signals(signals)

    books = read_csvs(book_paths)
    for row in books:
        row["_ts"] = parse_ts(row.get("timestamp_utc"))
    books_by_asset: dict[str, list[dict]] = defaultdict(list)
    for row in books:
        if row.get("_ts") and row.get("asset_id"):
            books_by_asset[str(row["asset_id"])].append(row)
    for rows in books_by_asset.values():
        rows.sort(key=lambda r: r["_ts"])

    by_yes, by_match = load_markets()
    output_rows = []
    for signal in stale:
        ts = parse_ts(signal.get("timestamp_utc"))
        if ts is None:
            continue
        market = market_for_signal(signal, by_yes, by_match)
        favors_yes = event_favors_yes(signal, market)
        if not market or favors_yes is None:
            token_id = ""
            side = "unknown"
        elif favors_yes:
            token_id = str(market.get("yes_token_id") or signal.get("yes_token_id") or "")
            side = "YES"
        else:
            token_id = str(market.get("no_token_id") or "")
            side = "NO"

        asset_books = books_by_asset.get(token_id, [])
        entry_book = latest_before(asset_books, ts)
        entry_ask = book_ask(entry_book)
        row = {
            "timestamp_utc": signal.get("timestamp_utc"),
            "signal_file": signal.get("_file"),
            "skip_reason": signal.get("skip_reason"),
            "match_id": signal.get("match_id"),
            "market_name": signal.get("market_name"),
            "market_type": signal.get("market_type"),
            "event_type": signal.get("event_type"),
            "event_direction": signal.get("event_direction"),
            "severity": signal.get("severity"),
            "game_time_sec": signal.get("game_time_sec"),
            "side": side,
            "token_id": token_id,
            "logged_book_age_ms": signal.get("book_age_ms") or signal.get("book_age_at_signal_ms"),
            "steam_age_ms": signal.get("steam_age_ms"),
            "source_update_age_sec": signal.get("source_update_age_sec"),
            "entry_book_ts": entry_book.get("timestamp_utc") if entry_book else "",
            "entry_book_age_ms": book_age_ms(entry_book, ts),
            "entry_bid": book_bid(entry_book),
            "entry_ask": entry_ask,
            "entry_spread": book_spread(entry_book),
        }
        if entry_ask is not None:
            for seconds in MARK_SECONDS:
                mark = first_at_or_after(asset_books, ts + timedelta(seconds=seconds))
                mark_bid = book_bid(mark)
                row[f"bid_{seconds}s"] = mark_bid
                row[f"pnl_per_share_{seconds}s"] = round(mark_bid - entry_ask, 4) if mark_bid is not None else ""
                row[f"roi_{seconds}s"] = round((mark_bid - entry_ask) / entry_ask, 4) if mark_bid is not None and entry_ask else ""
            later = [book for book in asset_books if book["_ts"] >= ts]
            latest = later[-1] if later else None
            latest_bid = book_bid(latest)
            row["latest_book_ts"] = latest.get("timestamp_utc") if latest else ""
            row["latest_bid"] = latest_bid
            row["pnl_per_share_latest"] = round(latest_bid - entry_ask, 4) if latest_bid is not None else ""
            row["roi_latest"] = round((latest_bid - entry_ask) / entry_ask, 4) if latest_bid is not None and entry_ask else ""
        output_rows.append(row)

    if output_rows:
        with OUTPUT.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(output_rows[0].keys()))
            writer.writeheader()
            writer.writerows(output_rows)

    print(f"stale raw={len([s for s in signals if s.get('skip_reason') in STALE_REASONS])} dedup={len(stale)} wrote={OUTPUT}")
    print(f"by skip reason: {dict(Counter(r['skip_reason'] for r in output_rows))}")
    print(f"missing entry ask: {sum(fnum(r.get('entry_ask')) is None for r in output_rows)}/{len(output_rows)}")
    print(f"entry book age: {stat([fnum(r.get('entry_book_age_ms')) for r in output_rows])}")
    for seconds in MARK_SECONDS:
        print(f"pnl/share {seconds}s: {stat([fnum(r.get(f'pnl_per_share_{seconds}s')) for r in output_rows])}")
    print(f"pnl/share latest: {stat([fnum(r.get('pnl_per_share_latest')) for r in output_rows])}")


if __name__ == "__main__":
    main()
