from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Iterable

from config import (
    CSV_LOG_PATH, PAPER_TRADES_CSV_PATH, DOTA_EVENTS_CSV_PATH, BOOK_EVENTS_CSV_PATH,
    LIVE_ATTEMPTS_CSV_PATH, LATENCY_CSV_PATH, LIVE_LEAGUE_RAW_JSONL_PATH,
    RICH_CONTEXT_CSV_PATH, SOURCE_DELAY_CSV_PATH,
    BOOK_REFRESH_RESCUE_CSV_PATH, SHADOW_TRADES_CSV_PATH,
    RUN_ID, CODE_VERSION, CONFIG_HASH,
)

RAW_SNAPSHOTS_CSV_PATH = "logs/raw_snapshots.csv"
SIGNAL_MARKOUTS_CSV_PATH = "logs/signal_markouts.csv"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ns_to_iso(ns: int | None) -> str | None:
    if not ns:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat(timespec="milliseconds")


import queue
import threading


class CsvLogger:
    def __init__(self, filename: str, headers: list[str]):
        self.filename = filename
        self.headers = headers
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._init_file()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _init_file(self):
        parent = os.path.dirname(self.filename)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(self.filename) and not self._header_matches():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            os.replace(self.filename, f"{self.filename}.{stamp}.bak")
        if not os.path.exists(self.filename):
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.headers).writeheader()

    def _header_matches(self) -> bool:
        try:
            with open(self.filename, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                existing = next(reader, [])
        except (OSError, StopIteration):
            return False
        return existing == self.headers

    def _worker(self):
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                row = self._queue.get(timeout=1.0)
                with open(self.filename, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=self.headers)
                    writer.writerow(row)
                self._queue.task_done()
            except queue.Empty:
                continue

    def append(self, row: dict):
        clean = {key: row.get(key) for key in self.headers}
        self._queue.put(clean)

    def append_many(self, rows: Iterable[dict]):
        for row in rows:
            self.append(row)

    def stop(self):
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join()


class SignalLogger(CsvLogger):
    def __init__(self, filename: str = CSV_LOG_PATH):
        super().__init__(filename, [
            "timestamp_utc",
            "run_id", "code_version", "config_hash",
            "match_id", "lobby_id", "league_id", "radiant_team", "dire_team",
            "game_time_sec", "radiant_lead", "radiant_score", "dire_score",
            "market_name", "market_type", "yes_team", "yes_token_id",
            "event_type", "cluster_event_types", "event_direction", "severity",
            "event_tier", "event_is_primary", "event_family", "event_quality",
            "event_schema_version", "snapshot_gap_sec", "actual_window_sec",
            "networth_delta", "kill_diff_delta", "total_kills_delta",
            "networth_delta_per_30s", "kill_diff_delta_per_30s", "source_cadence_quality",
            "token_id", "side",
            "lag", "expected_move", "fair_price", "executable_price", "executable_edge", "remaining_move",
            "fair_source",
            "market_move_recent", "price_lookback_sec", "pregame_move",
            "anchor_price", "current_price",
            "bid", "ask", "spread", "ask_size",
            "price_quality_score", "execution_quality_score", "trade_score",
            "target_size_usd", "size_multiplier", "phase_mult", "event_kill_lead",
            "decision", "skip_reason",
            "steam_age_ms", "source_update_age_sec", "stream_delay_s", "data_source", "book_age_ms", "book_age_at_signal_ms",
            "mapping_confidence", "mapping_errors", "team_id_match",
            "market_game_number_match", "duplicate_match_id_error",
            "slow_model_fair", "fast_event_adjustment", "hybrid_fair",
            "hybrid_confidence", "uncertainty_penalty",
        ])

    def log_signal(self, game: dict, mapping: dict, signal: dict, event_type: str = "",
                   event_direction: str = "", severity: str = "",
                   token_id: str = "", side: str = ""):
        self.append({
            "timestamp_utc": utc_now_iso(),
            "run_id": RUN_ID,
            "code_version": CODE_VERSION,
            "config_hash": CONFIG_HASH,
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
            "event_tier": signal.get("event_tier"),
            "event_is_primary": signal.get("event_is_primary"),
            "event_family": signal.get("event_family"),
            "event_quality": signal.get("event_quality"),
            "event_schema_version": signal.get("event_schema_version"),
            "snapshot_gap_sec": signal.get("snapshot_gap_sec"),
            "actual_window_sec": signal.get("actual_window_sec"),
            "networth_delta": signal.get("networth_delta"),
            "kill_diff_delta": signal.get("kill_diff_delta"),
            "total_kills_delta": signal.get("total_kills_delta"),
            "networth_delta_per_30s": signal.get("networth_delta_per_30s"),
            "kill_diff_delta_per_30s": signal.get("kill_diff_delta_per_30s"),
            "source_cadence_quality": signal.get("source_cadence_quality"),
            "token_id": token_id,
            "side": side,
            "lag": signal.get("lag"),
            "expected_move": signal.get("expected_move"),
            "market_move_recent": signal.get("market_move_recent"),
            "fair_price": signal.get("fair_price"),
            "executable_price": signal.get("executable_price"),
            "executable_edge": signal.get("executable_edge"),
            "remaining_move": signal.get("remaining_move"),
            "fair_source": signal.get("fair_source"),
            "price_lookback_sec": signal.get("price_lookback_sec"),
            "pregame_move": signal.get("pregame_move"),
            "anchor_price": signal.get("anchor_price"),
            "current_price": signal.get("current_price"),
            "bid": signal.get("bid"),
            "ask": signal.get("ask"),
            "spread": signal.get("spread"),
            "ask_size": signal.get("ask_size"),
            "price_quality_score": signal.get("price_quality_score"),
            "execution_quality_score": signal.get("execution_quality_score"),
            "trade_score": signal.get("trade_score"),
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
            "book_age_at_signal_ms": signal.get("book_age_at_signal_ms") or signal.get("book_age_ms"),
            "mapping_confidence": signal.get("mapping_confidence") or game.get("mapping_confidence"),
            "mapping_errors": signal.get("mapping_errors") or game.get("mapping_errors"),
            "team_id_match": signal.get("team_id_match") or game.get("team_id_match"),
            "market_game_number_match": signal.get("market_game_number_match") or game.get("market_game_number_match"),
            "duplicate_match_id_error": signal.get("duplicate_match_id_error") or game.get("duplicate_match_id_error"),
            "slow_model_fair": signal.get("slow_model_fair"),
            "fast_event_adjustment": signal.get("fast_event_adjustment"),
            "hybrid_fair": signal.get("hybrid_fair"),
            "hybrid_confidence": signal.get("hybrid_confidence"),
            "uncertainty_penalty": signal.get("uncertainty_penalty"),
        })


class LatencyLogger(CsvLogger):
    def __init__(self, filename: str = LATENCY_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc", "run_id", "code_version", "config_hash",
            "match_id", "market_name", "event_type", "cluster_event_types",
            "event_direction", "game_time_sec", "data_source",
            "steam_received_at_ns", "steam_source_update_age_sec", "stream_delay_s",
            "event_detected_ns", "signal_eval_start_ns", "signal_evaluated_ns", "event_detection_latency_ms", "signal_eval_latency_ms",
            "token_id", "side", "book_received_at_ns", "book_age_at_signal_ms",
            "best_bid", "best_ask", "spread", "ask_size",
            "decision", "skip_reason", "fair_price", "executable_price", "executable_edge",
            "remaining_move", "fair_source", "required_edge", "lag",
            "paper_delay_ms", "paper_attempt_ns", "paper_fill_ns", "paper_entry_result",
            "paper_fill_price", "paper_entry_latency_ms",
            "live_submit_start_ns", "live_response_received_ns", "live_submit_latency_ms",
            "live_order_status", "live_reject_reason", "live_submitted_size_usd",
            "live_filled_size_usd", "live_avg_fill_price",
            "mapping_confidence", "mapping_errors", "team_id_match",
            "market_game_number_match", "duplicate_match_id_error",
            "slow_model_fair", "fast_event_adjustment", "hybrid_fair",
            "hybrid_confidence", "uncertainty_penalty",
        ])

    def log_latency(self, row: dict):
        # Compute latencies if ns fields exist
        try:
            if row.get("event_detected_ns") and row.get("steam_received_at_ns"):
                row["event_detection_latency_ms"] = round((row["event_detected_ns"] - row["steam_received_at_ns"]) / 1_000_000, 2)
            if row.get("signal_evaluated_ns") and row.get("signal_eval_start_ns"):
                row["signal_eval_latency_ms"] = round((row["signal_evaluated_ns"] - row["signal_eval_start_ns"]) / 1_000_000, 2)
            if row.get("paper_fill_ns") and row.get("paper_attempt_ns"):
                row["paper_entry_latency_ms"] = round((row["paper_fill_ns"] - row["paper_attempt_ns"]) / 1_000_000, 2)
            if row.get("live_response_received_ns") and row.get("live_submit_start_ns"):
                row["live_submit_latency_ms"] = round((row["live_response_received_ns"] - row["live_submit_start_ns"]) / 1_000_000, 2)
        except (TypeError, ZeroDivisionError):
            pass
        
        row["timestamp_utc"] = utc_now_iso()
        row["run_id"] = row.get("run_id") or RUN_ID
        row["code_version"] = row.get("code_version") or CODE_VERSION
        row["config_hash"] = row.get("config_hash") or CONFIG_HASH
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
            "timestamp_utc", "run_id", "code_version", "config_hash",
            "match_id", "lobby_id", "league_id", "mapping_name", "yes_team", "yes_token_id",
            "event_type", "event_tier", "event_is_primary", "event_family", "event_quality", "event_dedupe_key",
            "event_schema_version", "snapshot_gap_sec", "actual_window_sec",
            "networth_delta", "kill_diff_delta", "total_kills_delta",
            "networth_delta_per_30s", "kill_diff_delta_per_30s", "source_cadence_quality",
            "component_event_types", "component_deltas", "component_window_sec",
            "severity", "game_time_sec", "radiant_team", "dire_team",
            "radiant_lead", "radiant_score", "dire_score", "tower_state",
            "previous_value", "current_value", "delta", "window_sec", "threshold", "direction",
            "base_pressure_score", "fight_pressure_score", "economic_pressure_score",
            "conversion_score", "event_confidence",
        ])

    def log_events(self, events):
        rows = []
        now = utc_now_iso()
        for event in events:
            row = event.to_dict() if hasattr(event, "to_dict") else dict(event)
            row["timestamp_utc"] = now
            row["run_id"] = RUN_ID
            row["code_version"] = CODE_VERSION
            row["config_hash"] = CONFIG_HASH
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


LIVE_LEAGUE_RAW_CSV_PATH = "logs/liveleague_raw.csv"


class LiveLeagueRawLogger:
    def __init__(self, filename: str = LIVE_LEAGUE_RAW_JSONL_PATH):
        self.filename = filename
        parent = os.path.dirname(self.filename)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def log_raw(self, raw: dict, received_at_ns: int):
        import json as _json
        row = {
            "timestamp_utc": utc_now_iso(),
            "received_at_ns": received_at_ns,
            "match_id": str(raw.get("match_id") or raw.get("lobby_id") or ""),
            "lobby_id": str(raw.get("lobby_id") or ""),
            "league_id": str(raw.get("league_id") or ""),
            "series_id": raw.get("series_id"),
            "series_type": raw.get("series_type"),
            "radiant_team": (raw.get("radiant_team") or {}).get("team_name") if isinstance(raw.get("radiant_team"), dict) else raw.get("radiant_team"),
            "dire_team": (raw.get("dire_team") or {}).get("team_name") if isinstance(raw.get("dire_team"), dict) else raw.get("dire_team"),
            "game_time_sec": int((raw.get("scoreboard") or {}).get("duration") or 0) or None if isinstance(raw.get("scoreboard"), dict) else None,
            "stream_delay_s": int(raw.get("stream_delay_s") or 0),
            "raw": raw,
        }
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(_json.dumps(row, default=str, sort_keys=True) + "\n")


class RichContextLogger(CsvLogger):
    def __init__(self, filename: str = RICH_CONTEXT_CSV_PATH):
        player_fields = [
            "account_id", "player_name", "hero_id", "kills", "deaths", "assists",
            "last_hits", "denies", "gold", "level", "gpm", "xpm", "net_worth",
            "item0", "item1", "item2", "item3", "item4", "item5",
            "backpack0", "backpack1", "backpack2", "neutral_item", "respawn_timer",
        ]
        player_headers = [
            f"{side}_p{idx}_{field}"
            for side in ("radiant", "dire")
            for idx in range(1, 6)
            for field in player_fields
        ]
        super().__init__(filename, [
            "timestamp_utc",
            "received_at_ns",
            "match_id",
            "lobby_id",
            "league_id",
            "series_id",
            "series_type",
            "game_time_sec",
            "radiant_team_id",
            "dire_team_id",
            "radiant_team",
            "dire_team",
            "radiant_team_name",
            "dire_team_name",
            "radiant_score",
            "dire_score",
            "score_diff",
            "radiant_tower_state",
            "dire_tower_state",
            "radiant_barracks_state",
            "dire_barracks_state",
            "radiant_net_worth",
            "dire_net_worth",
            "net_worth_diff",
            "top1_net_worth_diff",
            "top2_net_worth_diff",
            "top3_net_worth_diff",
            "level_diff",
            "gpm_diff",
            "xpm_diff",
            "gold_diff",
            "radiant_dead_count",
            "dire_dead_count",
            "dead_core_count",
            "radiant_max_respawn",
            "dire_max_respawn",
            "max_respawn_timer",
            "radiant_core_dead_count",
            "dire_core_dead_count",
            "radiant_top3_nw",
            "dire_top3_nw",
            "aegis_team",
            "aegis_holder_side",
            "aegis_holder_hero_id",
            "radiant_has_aegis",
            "dire_has_aegis",
            "liveleague_age_ms",
            "game_time_lag_sec",
            "liveleague_context_status",
            "realtime_stats_age_sec",
            "delayed_game_time_sec",
        ] + player_headers)
        # (match_id, delayed_game_time_sec) already written — deduplicates Valve update cadence
        self._seen: dict[str, int] = {}

    def log_rich_context(self, game: dict):
        match_id = str(game.get("match_id") or "")
        # Prefer realtime_game_time_sec if available, fallback to liveleague
        delayed_gt = game.get("realtime_game_time_sec") or game.get("delayed_game_time_sec") or game.get("liveleague_game_time_sec")
        
        if not match_id or delayed_gt is None:
            return
        
        # Deduplicate to avoid bloating the log with identical rows between Valve updates
        if self._seen.get(match_id) == delayed_gt:
            return
        self._seen[match_id] = delayed_gt

        row = {key: game.get(key) for key in self.headers}
        row["timestamp_utc"] = utc_now_iso()
        # If received_at_ns isn't explicitly in game, it might be in liveleague_received_at_ns
        if row.get("received_at_ns") is None:
            row["received_at_ns"] = game.get("liveleague_received_at_ns")
        if row.get("game_time_sec") is None:
             row["game_time_sec"] = game.get("liveleague_game_time_sec")

        self.append(row)


class SourceDelayLogger(CsvLogger):
    def __init__(self, filename: str = SOURCE_DELAY_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc",
            "match_id",
            "lobby_id",
            "league_id",
            "liveleague_received_at_ns",
            "liveleague_game_time_sec",
            "nearest_toplive_received_at_ns",
            "nearest_toplive_game_time_sec",
            "game_time_lag_sec",
            "stream_delay_s",
            "wall_clock_receive_gap_sec",
            "liveleague_usage",
        ])

    def log_source_delay(self, row: dict):
        row["timestamp_utc"] = utc_now_iso()
        self.append(row)


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


class LiveExitLogger(CsvLogger):
    def __init__(self, filename: str = "logs/live_exits.csv"):
        super().__init__(filename, [
            "timestamp_utc",
            "position_id", "token_id", "match_id", "reason",
            "shares_requested", "shares_filled", "best_bid", "price_floor",
            "order_status", "reason_if_rejected",
            "submit_start_ns", "response_received_ns", "submit_latency_ms",
            "raw_response_json",
        ])

    def log_exit_attempt(self, attempt) -> None:
        d = attempt.to_dict() if hasattr(attempt, "to_dict") else dict(attempt)
        d["timestamp_utc"] = utc_now_iso()
        self.append(d)


class ShadowTradeLogger(CsvLogger):
    def __init__(self, filename: str = SHADOW_TRADES_CSV_PATH):
        super().__init__(filename, [
            "shadow_id",
            "timestamp_utc",
            "event_type",
            "event_tier",
            "event_family",
            "market_type",
            "proxy_market_type",
            "is_game3_match_proxy",
            "token_id",
            "side",
            "match_id",
            "market_name",
            "decision",
            "skip_reason",
            "entry_price",
            "bid_at_entry",
            "ask_at_entry",
            "spread_at_entry",
            "fair_price",
            "executable_edge",
            "lag",
            "event_quality",
            "source_cadence_quality",
            "game_time_sec",
            "markout_3s",
            "markout_10s",
            "markout_30s",
            "markout_60s",
            "would_pnl_3s",
            "would_pnl_10s",
            "would_pnl_30s",
            "would_pnl_60s",
        ])

    def log_shadow_trade(self, shadow) -> None:
        d = shadow.to_dict() if hasattr(shadow, "to_dict") else dict(shadow)
        self.append(d)


class BookRefreshRescueLogger(CsvLogger):
    def __init__(self, filename: str = BOOK_REFRESH_RESCUE_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc",
            "match_id",
            "event_type",
            "event_tier",
            "event_direction",
            "token_id",
            "local_book_age_ms",
            "local_bid",
            "local_ask",
            "local_spread",
            "local_ask_size",
            "refresh_request_start_ns",
            "refresh_response_ns",
            "refresh_latency_ms",
            "fresh_bid",
            "fresh_ask",
            "fresh_spread",
            "fresh_ask_size",
            "fresh_book_age_ms_if_available",
            "local_to_fresh_ask_change",
            "fresh_executable_edge",
            "fresh_remaining_move",
            "fresh_fair_source",
            "fresh_hybrid_fair",
            "fresh_decision",
            "fresh_skip_reason",
            "markout_3s",
            "markout_10s",
            "markout_30s",
        ])

    def log_rescue(self, row: dict) -> None:
        row["timestamp_utc"] = utc_now_iso()
        self.append(row)


class SignalMarkoutLogger(CsvLogger):
    def __init__(self, filename: str = SIGNAL_MARKOUTS_CSV_PATH):
        super().__init__(filename, [
            "timestamp_utc",
            "signal_timestamp_utc",
            "match_id",
            "market_name",
            "event_type",
            "event_tier",
            "event_is_primary",
            "event_direction",
            "token_id",
            "side",
            "decision",
            "skip_reason",
            "reference_price",
            "reference_bid",
            "reference_ask",
            "fair_price",
            "hybrid_fair",
            "executable_edge",
            "markout_3s",
            "markout_10s",
            "markout_30s",
            "edge_after_3s",
            "edge_after_10s",
            "edge_after_30s",
        ])

    def log_markout(self, row: dict) -> None:
        row["timestamp_utc"] = utc_now_iso()
        self.append(row)


class MatchWinnerSignalLogger(CsvLogger):
    def __init__(self, log_dir: str):
        filename = os.path.join(log_dir, "match_winner_signals.csv")
        headers = [
            "timestamp_utc", "timestamp_ns", "match_id", "event_type", "event_direction",
            "map_token_id", "map_bid", "map_ask", "map_book_age_ms",
            "match_token_id", "match_bid", "match_ask", "match_book_age_ms",
            "current_map_p_before", "current_map_p_after",
            "p_next_yes", "p_next_source", "neutral_p_next_yes",
            "match_fair_before", "match_fair_after", "match_fair_delta",
            "match_edge", "decision", "skip_reason"
        ]
        super().__init__(filename, headers)

    def log_match_signal(self, row: dict):
        if "timestamp_utc" not in row:
            row["timestamp_utc"] = ns_to_iso(row.get("timestamp_ns")) or utc_now_iso()
        self.append(row)
