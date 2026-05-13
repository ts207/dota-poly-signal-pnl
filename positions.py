from __future__ import annotations

import csv
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable


POSITION_HEADERS = [
    "position_id", "scenario_ms", "market_name", "token_id", "side", "entry_time_utc",
    "entry_price", "shares", "notional_usd",
    "latest_book_time_utc", "latest_bid", "latest_ask", "market_value_bid_usd",
    "unrealized_pnl_usd", "unrealized_pnl_pct", "max_drawdown_usd", "max_runup_usd",
    "holding_seconds", "mark_count", "status",
]

SUMMARY_HEADERS = [
    "scope", "scenario_ms", "attempts", "filled", "fill_rate", "notional_usd",
    "marked_positions", "market_value_bid_usd", "unrealized_pnl_usd", "unrealized_pnl_pct",
    "avg_entry_price", "worst_drawdown_usd", "best_runup_usd",
]


@dataclass
class PositionMark:
    position_id: str
    scenario_ms: int
    market_name: str | None
    token_id: str
    side: str
    entry_time_utc: str
    entry_price: float
    shares: float
    notional_usd: float
    latest_book_time_utc: str | None
    latest_bid: float | None
    latest_ask: float | None
    market_value_bid_usd: float | None
    unrealized_pnl_usd: float | None
    unrealized_pnl_pct: float | None
    max_drawdown_usd: float | None
    max_runup_usd: float | None
    holding_seconds: float | None
    mark_count: int
    status: str = "open"

    def to_dict(self) -> dict:
        return asdict(self)


def read_csv(path: str | Path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: str | Path, rows: Iterable[dict], headers: list[str]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h) for h in headers})


def fnum(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fint(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def seconds_between(a: datetime | None, b: datetime | None) -> float | None:
    if not a or not b:
        return None
    return (b - a).total_seconds()


def _book_index(book_events: list[dict]) -> dict[str, list[dict]]:
    by_token: dict[str, list[dict]] = {}
    for row in book_events:
        token_id = str(row.get("asset_id") or row.get("token_id") or "")
        ts = parse_ts(row.get("timestamp_utc"))
        if not token_id or ts is None:
            continue
        row = dict(row)
        row["_ts"] = ts
        row["_bid"] = fnum(row.get("best_bid"))
        row["_ask"] = fnum(row.get("best_ask"))
        by_token.setdefault(token_id, []).append(row)

    for rows in by_token.values():
        rows.sort(key=lambda r: r["_ts"])
    return by_token


def _latest_before_or_any(rows: list[dict], entry_ts: datetime) -> dict | None:
    latest = None
    for row in rows:
        if row["_ts"] <= entry_ts:
            latest = row
        else:
            break
    return latest or (rows[-1] if rows else None)


def _is_live_position_log(paper_trades: list[dict]) -> bool:
    return any((row.get("action") or "").strip().lower() in {"entry", "exit"} for row in paper_trades)


def _build_legacy_positions(paper_trades: list[dict], book_events: list[dict]) -> list[PositionMark]:
    """Reconstruct open paper positions and mark them at latest visible bid.

    This is intentionally conservative: a long YES position is valued at the
    bid you could theoretically sell into, not midpoint, ask, or model price.
    """
    books = _book_index(book_events)
    positions: list[PositionMark] = []

    for idx, trade in enumerate(paper_trades, start=1):
        filled = fnum(trade.get("filled_usd")) or 0.0
        price = fnum(trade.get("price")) or 0.0
        shares = fnum(trade.get("shares")) or 0.0
        if filled <= 0 or price <= 0 or shares <= 0:
            continue

        token_id = str(trade.get("token_id") or "")
        entry_ts = parse_ts(trade.get("timestamp_utc"))
        if not token_id or entry_ts is None:
            continue

        token_books = books.get(token_id, [])
        after_entry = [row for row in token_books if row["_ts"] >= entry_ts]
        relevant_marks = after_entry or ([_latest_before_or_any(token_books, entry_ts)] if token_books else [])
        relevant_marks = [row for row in relevant_marks if row]

        latest = relevant_marks[-1] if relevant_marks else None
        latest_bid = latest.get("_bid") if latest else None
        latest_ask = latest.get("_ask") if latest else None
        latest_ts = latest.get("_ts") if latest else None

        mark_values = []
        for row in relevant_marks:
            bid = row.get("_bid")
            if bid is not None:
                mark_values.append(shares * bid - filled)

        if latest_bid is not None:
            market_value = shares * latest_bid
            pnl = market_value - filled
            pnl_pct = pnl / filled if filled else None
        else:
            market_value = None
            pnl = None
            pnl_pct = None

        max_drawdown = min(mark_values) if mark_values else None
        max_runup = max(mark_values) if mark_values else None

        positions.append(PositionMark(
            position_id=f"paper-{idx}",
            scenario_ms=fint(trade.get("scenario_ms")) or 0,
            market_name=trade.get("market_name"),
            token_id=token_id,
            side=trade.get("side") or "BUY_YES",
            entry_time_utc=trade.get("timestamp_utc") or "",
            entry_price=price,
            shares=shares,
            notional_usd=filled,
            latest_book_time_utc=latest.get("timestamp_utc") if latest else None,
            latest_bid=latest_bid,
            latest_ask=latest_ask,
            market_value_bid_usd=market_value,
            unrealized_pnl_usd=pnl,
            unrealized_pnl_pct=pnl_pct,
            max_drawdown_usd=max_drawdown,
            max_runup_usd=max_runup,
            holding_seconds=seconds_between(entry_ts, latest_ts),
            mark_count=len([row for row in relevant_marks if row.get("_bid") is not None]),
        ))

    return positions


def _build_live_positions(paper_trades: list[dict], book_events: list[dict]) -> list[PositionMark]:
    """Reconstruct positions from the current PositionLogger entry/exit schema."""
    books = _book_index(book_events)
    open_by_token: dict[str, list[dict]] = {}
    paired: list[tuple[dict, dict | None]] = []

    for row in paper_trades:
        action = (row.get("action") or "").strip().lower()
        token_id = str(row.get("token_id") or "")
        if not token_id:
            continue

        if action == "entry":
            open_by_token.setdefault(token_id, []).append(row)
        elif action == "exit":
            entries = open_by_token.get(token_id) or []
            if entries:
                paired.append((entries.pop(0), row))

    for entries in open_by_token.values():
        for entry in entries:
            paired.append((entry, None))

    positions: list[PositionMark] = []
    paired.sort(key=lambda pair: parse_ts(pair[0].get("timestamp_utc")) or datetime.min.replace(tzinfo=timezone.utc))

    for idx, (entry, exit_row) in enumerate(paired, start=1):
        token_id = str(entry.get("token_id") or "")
        entry_ts = parse_ts(entry.get("timestamp_utc"))
        price = fnum(entry.get("entry_price")) or 0.0
        shares = fnum(entry.get("shares")) or 0.0
        cost = fnum(entry.get("cost_usd")) or 0.0
        if not token_id or entry_ts is None or price <= 0 or shares <= 0 or cost <= 0:
            continue

        status = "closed" if exit_row else "open"
        latest_ts = None
        latest_bid = None
        latest_ask = None
        market_value = None
        pnl = None
        pnl_pct = None
        holding_seconds = None
        mark_count = 0
        max_drawdown = None
        max_runup = None

        token_books = books.get(token_id, [])
        after_entry = [row for row in token_books if row["_ts"] >= entry_ts]
        relevant_marks = after_entry or ([_latest_before_or_any(token_books, entry_ts)] if token_books else [])
        relevant_marks = [row for row in relevant_marks if row]

        mark_values = []
        for row in relevant_marks:
            bid = row.get("_bid")
            if bid is not None:
                mark_values.append(shares * bid - cost)

        if exit_row:
            exit_ts = parse_ts(exit_row.get("timestamp_utc"))
            exit_price = fnum(exit_row.get("exit_price"))
            proceeds = fnum(exit_row.get("proceeds_usd"))
            logged_pnl = fnum(exit_row.get("pnl_usd"))
            latest_ts = exit_ts
            latest_bid = exit_price
            market_value = proceeds if proceeds is not None else (shares * exit_price if exit_price is not None else None)
            pnl = logged_pnl if logged_pnl is not None else (market_value - cost if market_value is not None else None)
            pnl_pct = pnl / cost if pnl is not None and cost else None
            holding_seconds = fnum(exit_row.get("hold_sec")) or seconds_between(entry_ts, exit_ts)
        else:
            latest = relevant_marks[-1] if relevant_marks else None
            latest_bid = latest.get("_bid") if latest else None
            latest_ask = latest.get("_ask") if latest else None
            latest_ts = latest.get("_ts") if latest else None
            if latest_bid is not None:
                market_value = shares * latest_bid
                pnl = market_value - cost
                pnl_pct = pnl / cost if cost else None
            holding_seconds = seconds_between(entry_ts, latest_ts)

        max_drawdown = min(mark_values) if mark_values else pnl
        max_runup = max(mark_values) if mark_values else pnl
        mark_count = len([row for row in relevant_marks if row.get("_bid") is not None])

        positions.append(PositionMark(
            position_id=f"paper-{idx}",
            scenario_ms=fint(entry.get("scenario_ms")) or 0,
            market_name=entry.get("market_name"),
            token_id=token_id,
            side=entry.get("side") or "BUY",
            entry_time_utc=entry.get("timestamp_utc") or "",
            entry_price=price,
            shares=shares,
            notional_usd=cost,
            latest_book_time_utc=(latest_ts.isoformat(timespec="milliseconds") if isinstance(latest_ts, datetime) else None),
            latest_bid=latest_bid,
            latest_ask=latest_ask,
            market_value_bid_usd=market_value,
            unrealized_pnl_usd=pnl,
            unrealized_pnl_pct=pnl_pct,
            max_drawdown_usd=max_drawdown,
            max_runup_usd=max_runup,
            holding_seconds=holding_seconds,
            mark_count=mark_count,
            status=status,
        ))

    return positions


def build_positions(paper_trades: list[dict], book_events: list[dict]) -> list[PositionMark]:
    if _is_live_position_log(paper_trades):
        return _build_live_positions(paper_trades, book_events)
    return _build_legacy_positions(paper_trades, book_events)


def summarize_positions(paper_trades: list[dict], positions: list[PositionMark]) -> list[dict]:
    scenarios = sorted({fint(t.get("scenario_ms")) or 0 for t in paper_trades} | {p.scenario_ms for p in positions})
    rows = []
    live_schema = _is_live_position_log(paper_trades)

    def build_row(scope: str, scenario: int | None, trades_subset: list[dict], pos_subset: list[PositionMark]) -> dict:
        if live_schema:
            attempts = len([t for t in trades_subset if (t.get("action") or "").strip().lower() == "entry"])
            filled = attempts
        else:
            attempts = len(trades_subset)
            filled = len([t for t in trades_subset if (fnum(t.get("filled_usd")) or 0) > 0])
        notional = sum(p.notional_usd for p in pos_subset)
        market_values = [p.market_value_bid_usd for p in pos_subset if p.market_value_bid_usd is not None]
        pnls = [p.unrealized_pnl_usd for p in pos_subset if p.unrealized_pnl_usd is not None]
        entry_prices = [p.entry_price for p in pos_subset]
        drawdowns = [p.max_drawdown_usd for p in pos_subset if p.max_drawdown_usd is not None]
        runups = [p.max_runup_usd for p in pos_subset if p.max_runup_usd is not None]
        total_mv = sum(market_values) if market_values else None
        total_pnl = sum(pnls) if pnls else None
        return {
            "scope": scope,
            "scenario_ms": "all" if scenario is None else scenario,
            "attempts": attempts,
            "filled": filled,
            "fill_rate": filled / attempts if attempts else None,
            "notional_usd": notional,
            "marked_positions": len([p for p in pos_subset if p.unrealized_pnl_usd is not None]),
            "market_value_bid_usd": total_mv,
            "unrealized_pnl_usd": total_pnl,
            "unrealized_pnl_pct": total_pnl / notional if total_pnl is not None and notional else None,
            "avg_entry_price": mean(entry_prices) if entry_prices else None,
            "worst_drawdown_usd": min(drawdowns) if drawdowns else None,
            "best_runup_usd": max(runups) if runups else None,
        }

    rows.append(build_row("overall", None, paper_trades, positions))
    for scenario in scenarios:
        trades_subset = [t for t in paper_trades if (fint(t.get("scenario_ms")) or 0) == scenario]
        pos_subset = [p for p in positions if p.scenario_ms == scenario]
        rows.append(build_row("scenario", scenario, trades_subset, pos_subset))
    return rows
