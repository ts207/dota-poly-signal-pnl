from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Iterable

from config import CSV_LOG_PATH, PAPER_TRADES_CSV_PATH, DOTA_EVENTS_CSV_PATH, BOOK_EVENTS_CSV_PATH, LIVE_ATTEMPTS_CSV_PATH, LATENCY_CSV_PATH

RAW_SNAPSHOTS_CSV_PATH = "logs/raw_snapshots.csv"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ns_to_iso(ns: int | None) -> str | None:
    if not ns:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat(timespec="milliseconds")


class CsvLogger:
    def __init__(self, filename: str, headers: list[str]):
        self.filename = filename
        self.headers = headers
        self._init_file()

    def _init_file(self):
        parent = os.path.dirname(self.filename)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if not os.path.exists(self.filename):
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.headers).writeheader()

    def append(self, row: dict):
        clean = {key: row.get(key) for key in self.headers}
        with open(self.filename, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.headers).writerow(clean)

    def append_many(self, rows: Iterable[dict]):
        with open(self.filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            for row in rows:
                writer.writerow({key: row.get(key) for key in self.headers})


class SignalLogger(CsvLogger):
    def __init__(self, filename: str = CSV_LOG_PATH):
        super().__init__(filename, [
            "timestamp_utc",
            "match_id", "lobby_id", "league_id", "radiant_team", "dire_team",
            "game_time_sec", "radiant_lead", "radiant_score", "dire_score",
            "market_name", "market_type", "yes_team", "yes_token_id",
            "event_type", "cluster_event_types", "event_direction", "severity",
            "token_id", "side",
            "lag", "expected_move", "fair_price", "executable_price", "executable_edge", "remaining_move",
            "market_move_recent", "price_lookback_sec", "pregame_move",
            "anchor_price", "current_price",
            "bid", "ask", "spread", "ask_size",
            "target_size_usd", "size_multiplier", "phase_mult", "event_kill_lead",
            "decision", "skip_reason",
            "steam_age_ms", "source_update_age_sec", "stream_delay_s", "data_source", "book_age_ms",
        ])

    def log_signal(self, game: dict, mapping: dict, signal: dict, event_type: str = "",
                   event_direction: str = "", severity: str = "",
                   token_id: str = "", side: str = ""):
        self.append({
            "timestamp_utc": utc_now_iso(),
            "match_id": game.get("match_id"),
            "lobby_id": game.get("lobby_id"),
            "league_id": game.get("league_id"),
            "radiant_team": game.get("radiant_team"),
            "dire_team": game.get("dire_team"),
            "game_time_sec": game.get("game_time_sec"),
            "radiant_lead": game.get("radiant_lead"),
            "radiant_score": game.get("radiant_score"),
            "dire_score": game.get("dire_score"),
            "market_name": mapping.get("name"),
            "market_type": mapping.get("market_type"),
            "yes_team": mapping.get("yes_team"),
            "yes_token_id": mapping.get("yes_token_id"),
            "event_type": signal.get("event_type") or event_type,
            "cluster_event_types": signal.get("cluster_event_types"),
            "event_direction": signal.get("event_direction") or event_direction,
            "severity": severity,
            "token_id": token_id,
            "side": side,
            "lag": signal.get("lag"),
            "expected_move": signal.get("expected_move"),
            "market_move_recent": signal.get("market_move_recent"),
            "fair_price": signal.get("fair_price"),
            "executable_price": signal.get("executable_price"),
            "executable_edge": signal.get("executable_edge"),
            "remaining_move": signal.get("remaining_move"),
            "price_lookback_sec": signal.get("price_lookback_sec"),
            "pregame_move": signal.get("pregame_move"),
            "anchor_price": signal.get("anchor_price"),
            "current_price": signal.get("current_price"),
            "bid": signal.get("bid"),
            "ask": signal.get("ask"),
            "spread": signal.get("spread"),
            "ask_size": signal.get("ask_size"),
            "target_size_usd": signal.get("target_size_usd"),
            "size_multiplier": signal.get("size_multiplier"),
            "phase_mult": signal.get("phase_mult"),
            "event_kill_lead": signal.get("event_kill_lead"),
            "decision": signal.get("decision"),
            "skip_reason": signal.get("reason"),
            "steam_age_ms": signal.get("steam_age_ms"),
            "source_update_age_sec": signal.get("source_update_age_sec"),
            "stream_delay_s": signal.get("stream_delay_s"),
            "data_source": signal.get("data_source"),
            "book_age_ms": signal.get("book_age_ms"),
        })


class LatencyLogger(CsvLogger):
    def __init__(self, filename: str = LATENCY_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "match_id", "market_name", "event_type", "cluster_event_types",
            "event_direction", "game_time_sec", "data_source",
            "steam_received_at_ns", "steam_source_update_age_sec", "stream_delay_s",
            "event_detected_ns", "signal_evaluated_ns", "event_detection_latency_ms", "signal_eval_latency_ms",
            "token_id", "side", "book_received_at_ns", "book_age_at_signal_ms",
            "best_bid", "best_ask", "spread", "ask_size",
            "decision", "skip_reason", "fair_price", "executable_price", "executable_edge",
            "remaining_move", "required_edge", "lag",
            "paper_delay_ms", "paper_attempt_ns", "paper_fill_ns", "paper_entry_result",
            "paper_fill_price", "paper_entry_latency_ms",
            "live_submit_start_ns", "live_response_received_ns", "live_submit_latency_ms",
            "live_order_status", "live_reject_reason", "live_submitted_size_usd",
            "live_filled_size_usd", "live_avg_fill_price"
        ])

    def log_latency(self, row: dict):
        # Compute latencies if ns fields exist
        try:
            if row.get("event_detected_ns") and row.get("steam_received_at_ns"):
                row["event_detection_latency_ms"] = round((row["event_detected_ns"] - row["steam_received_at_ns"]) / 1_000_000, 2)
            if row.get("signal_evaluated_ns") and row.get("event_detected_ns"):
                row["signal_eval_latency_ms"] = round((row["signal_evaluated_ns"] - row["event_detected_ns"]) / 1_000_000, 2)
            if row.get("paper_fill_ns") and row.get("paper_attempt_ns"):
                row["paper_entry_latency_ms"] = round((row["paper_fill_ns"] - row["paper_attempt_ns"]) / 1_000_000, 2)
            if row.get("live_response_received_ns") and row.get("live_submit_start_ns"):
                row["live_submit_latency_ms"] = round((row["live_response_received_ns"] - row["live_submit_start_ns"]) / 1_000_000, 2)
        except (TypeError, ZeroDivisionError):
            pass
        
        row["timestamp_utc"] = utc_now_iso()
        self.append(row)


class PositionLogger(CsvLogger):
    """Logs paper trade entries and exits to a single CSV.

    Entries have action='entry' and exit_* fields empty.
    Exits have action='exit' with full P&L data.
    """

    def __init__(self, filename: str = PAPER_TRADES_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "action",
            "token_id", "match_id", "market_name", "side",
            "entry_price", "shares", "cost_usd",
            "event_type", "lag", "expected_move",
            "entry_game_time_sec",
            "exit_price", "proceeds_usd", "pnl_usd", "roi",
            "hold_sec", "exit_game_time_sec", "exit_reason",
        ])

    def log_entry(self, pos) -> None:
        d = pos.to_dict()
        d["timestamp_utc"] = utc_now_iso()
        d["action"] = "entry"
        self.append(d)

    def log_exit(self, cp) -> None:
        d = cp.to_dict()
        d["timestamp_utc"] = ns_to_iso(cp.exit_time_ns) or utc_now_iso()
        d["action"] = "exit"
        self.append(d)


class DotaEventLogger(CsvLogger):
    def __init__(self, filename: str = DOTA_EVENTS_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "match_id", "lobby_id", "league_id", "mapping_name", "yes_team", "yes_token_id",
            "event_type", "severity", "game_time_sec", "radiant_team", "dire_team",
            "radiant_lead", "radiant_score", "dire_score", "tower_state",
            "previous_value", "current_value", "delta", "window_sec", "threshold", "direction",
        ])

    def log_events(self, events):
        rows = []
        now = utc_now_iso()
        for event in events:
            row = event.to_dict() if hasattr(event, "to_dict") else dict(event)
            row["timestamp_utc"] = now
            rows.append(row)
        self.append_many(rows)


class BookEventLogger(CsvLogger):
    def __init__(self, filename: str = BOOK_EVENTS_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "asset_id", "event_type", "best_bid", "best_ask", "bid_size", "ask_size",
            "mid", "spread", "source_event_type",
        ])

    def log_book(self, book: dict, source_event_type: str | None = None):
        bid = _to_float(book.get("best_bid"))
        ask = _to_float(book.get("best_ask"))
        spread = ask - bid if bid is not None and ask is not None else None
        mid = (ask + bid) / 2 if bid is not None and ask is not None else None
        self.append({
            "timestamp_utc": ns_to_iso(book.get("received_at_ns")) or utc_now_iso(),
            "asset_id": book.get("asset_id"),
            "event_type": "BOOK_TOP",
            "best_bid": bid,
            "best_ask": ask,
            "bid_size": book.get("bid_size"),
            "ask_size": book.get("ask_size"),
            "mid": mid,
            "spread": spread,
            "source_event_type": source_event_type,
        })


class RawSnapshotLogger(CsvLogger):
    """Logs every unique Steam API game-state snapshot with nanosecond precision.

    Only writes a row when game_time_sec advances for a given match, so the log
    records exactly when each Valve update arrived at the bot — the DLTV cadence.
    This is the ground-truth timestamp source for lag analysis in reaction_lag.py.
    """

    HEADERS = [
        "received_at_utc", "received_at_ns",
        "match_id", "lobby_id", "league_id",
        "game_time_sec", "radiant_lead",
        "radiant_score", "dire_score",
        "building_state", "tower_state",
        "stream_delay_s", "source_update_age_sec", "data_source", "spectators", "game_over",
    ]

    def __init__(self, filename: str = RAW_SNAPSHOTS_CSV_PATH):
        super().__init__(filename, self.HEADERS)
        # (match_id, game_time_sec) already written — deduplicates Valve update cadence
        self._seen: dict[str, int] = {}

    def log_game(self, game: dict) -> bool:
        """Log snapshot if game_time_sec advanced. Returns True if a row was written."""
        match_id = str(game.get("match_id") or "")
        game_time = game.get("game_time_sec")
        if not match_id or game_time is None:
            return False
        if self._seen.get(match_id) == game_time:
            return False
        self._seen[match_id] = game_time
        ns = game.get("received_at_ns")
        self.append({
            "received_at_utc": ns_to_iso(ns) or utc_now_iso(),
            "received_at_ns": ns,
            "match_id": match_id,
            "lobby_id": game.get("lobby_id"),
            "league_id": game.get("league_id"),
            "game_time_sec": game_time,
            "radiant_lead": game.get("radiant_lead"),
            "radiant_score": game.get("radiant_score"),
            "dire_score": game.get("dire_score"),
            "building_state": game.get("building_state"),
            "tower_state": game.get("tower_state"),
            "stream_delay_s": game.get("stream_delay_s"),
            "source_update_age_sec": game.get("source_update_age_sec"),
            "data_source": game.get("data_source"),
            "spectators": game.get("spectators"),
            "game_over": game.get("game_over"),
        })
        return True


def _to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class LiveAttemptLogger(CsvLogger):
    def __init__(self, filename: str = LIVE_ATTEMPTS_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "phase",
            "event_type", "event_direction", "token_id", "side",
            "market_name", "match_id",
            "fair_price", "best_ask", "price_cap", "edge", "lag", "spread",
            "book_age_ms", "steam_age_ms",
            "order_type", "submitted_size_usd", "filled_size_usd", "avg_fill_price",
            "order_status", "reason_if_rejected",
            "markout_3s", "markout_10s", "markout_30s",
            "raw_response_json",
        ])

    def log_attempt(self, attempt, *, phase: str = "submit", markouts: dict | None = None) -> None:
        d = attempt.to_dict() if hasattr(attempt, "to_dict") else dict(attempt)
        d["timestamp_utc"] = utc_now_iso()
        d["phase"] = phase
        markouts = markouts or {}
        d["markout_3s"] = markouts.get("markout_3s")
        d["markout_10s"] = markouts.get("markout_10s")
        d["markout_30s"] = markouts.get("markout_30s")
        self.append(d)
