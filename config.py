from __future__ import annotations

import os
import json
import hashlib
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

RUN_ID = os.getenv("RUN_ID") or str(int(time.time()))
DOTA_FAIR_MODEL_PATH = os.getenv("DOTA_FAIR_MODEL_PATH", "dota_fair_model/models/dota_fair.joblib")


def _git_code_version() -> str:
    env_version = os.getenv("CODE_VERSION")
    if env_version:
        return env_version
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(__file__),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


CODE_VERSION = _git_code_version()

STEAM_API_KEY = os.getenv("STEAM_API_KEY")
MODE = os.getenv("MODE", "paper").lower()

# Guarded live-path switch. Defaults to false; paper mode remains the default.
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes"}
MAX_TOTAL_LIVE_USD = float(os.getenv("MAX_TOTAL_LIVE_USD", "10"))
MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "1"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
LIVE_ORDER_TYPE = os.getenv("ORDER_TYPE", "FAK").upper()
LIVE_TICK_SIZE = os.getenv("LIVE_TICK_SIZE", "0.01")
LIVE_SAFETY_MARGIN = float(os.getenv("LIVE_SAFETY_MARGIN", "0.02"))
ALLOW_GAME_OVER_ONLY = os.getenv("ALLOW_GAME_OVER_ONLY", "false").lower() in {"1", "true", "yes"}
ALLOW_EVENT_TRADES = os.getenv("ALLOW_EVENT_TRADES", "true").lower() in {"1", "true", "yes"}
DISABLE_STRUCTURE_TRADES = os.getenv("DISABLE_STRUCTURE_TRADES", "false").lower() in {"1", "true", "yes"}
# Default live allowlist is intentionally strict. Confirmation-only events can be
# enabled explicitly after paper/live-attempt logs prove they are fillable.
TRADE_EVENTS = {e.strip() for e in os.getenv("TRADE_EVENTS", "THRONE_EXPOSED,SECOND_T4_TOWER_FALL,OBJECTIVE_CONVERSION_T4,T3_PLUS_T4_CHAIN").split(",") if e.strip()}
ALLOW_CONFIRMATION_ONLY_LIVE_TRADES = os.getenv("ALLOW_CONFIRMATION_ONLY_LIVE_TRADES", "false").lower() in {"1", "true", "yes"}
LIVE_ATTEMPTS_CSV_PATH = os.getenv("LIVE_ATTEMPTS_CSV_PATH", "logs/live_attempts.csv")

# Polling / reconnect
# NOTE: GetLiveLeagueGames carries a ~120s Valve-imposed broadcast delay.
# GetTopLiveGame is ~15–30s. The effective arb window is market_lag minus this delay.
# If market lag is ~60s and Steam delay is ~30s, the actual capture window is ~30s.
STEAM_POLL_SECONDS = float(os.getenv("STEAM_POLL_SECONDS", "3.0"))
WS_RECONNECT_SECONDS = float(os.getenv("WS_RECONNECT_SECONDS", "5"))
# GetLiveLeagueGames refresh interval — it adds ~120s on top of broadcaster delay
# and is only used for team-name enrichment, so polling it slowly is correct.
LLG_REFRESH_SECONDS = int(os.getenv("LLG_REFRESH_SECONDS", "60"))

# Safety thresholds
MAX_STEAM_AGE_MS = int(os.getenv("MAX_STEAM_AGE_MS", "1500"))
# Source freshness guards. received_at_ns only proves the HTTP response is fresh;
# these guards prevent paper trades from slower/stale Dota sources.
# stream_delay_s is spectator/broadcast delay metadata only; it is logged, not used as a skip guard.
REQUIRE_TOP_LIVE_FOR_SIGNALS = os.getenv("REQUIRE_TOP_LIVE_FOR_SIGNALS", "true").lower() in {"1", "true", "yes"}
MAX_SOURCE_UPDATE_AGE_SEC = float(os.getenv("MAX_SOURCE_UPDATE_AGE_SEC", "45"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "750"))
# Signal edge / lag knobs. MIN_EDGE was the old combined knob; keep it as
# a backward-compatible default for MIN_LAG only when MIN_LAG is unset.
MIN_LAG = float(os.getenv("MIN_LAG", os.getenv("MIN_EDGE", "0.08")))
MIN_EXECUTABLE_EDGE = float(os.getenv("MIN_EXECUTABLE_EDGE", "0.03"))
PRICE_LOOKBACK_SEC = float(os.getenv("PRICE_LOOKBACK_SEC", "10"))
DEFAULT_MAX_FILL_PRICE = float(os.getenv("DEFAULT_MAX_FILL_PRICE", "0.80"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.15"))
MIN_ASK_SIZE_USD = float(os.getenv("MIN_ASK_SIZE_USD", "25"))
PAPER_TRADE_SIZE_USD = float(os.getenv("PAPER_TRADE_SIZE_USD", "25"))
PAPER_SLIPPAGE_CENTS = float(os.getenv("PAPER_SLIPPAGE_CENTS", "0.01"))
PAPER_EXECUTION_DELAY_MS = int(os.getenv("PAPER_EXECUTION_DELAY_MS", "0"))
# Hard cap on total open paper exposure per match (USD). Prevents runaway stacking
# when multiple events fire in the same direction within a single game.
MAX_OPEN_USD_PER_MATCH = float(os.getenv("MAX_OPEN_USD_PER_MATCH", "150"))

# Exit thresholds
EXIT_TAKE_PROFIT   = float(os.getenv("EXIT_TAKE_PROFIT",    "0.95"))   # absolute max TP (game-over)
EXIT_STOP_LOSS_ABS = float(os.getenv("EXIT_STOP_LOSS_ABS",  "0.05"))   # floor price
EXIT_STOP_LOSS_REL = float(os.getenv("EXIT_STOP_LOSS_REL",  "0.10"))   # max loss from entry; capped at expected_move
# Time-based exit: per-event horizons calibrated to each event's repricing speed.
# Fallback EXIT_HORIZON_SEC applies for unknown event types or when 0 (disabled).
EXIT_HORIZON_SEC   = int(os.getenv("EXIT_HORIZON_SEC",      "120"))
EXIT_HORIZON_BY_EVENT: dict[str, int] = {
    "SECOND_T4_TOWER_FALL":      120,
    "FIRST_T4_TOWER_FALL":       120,
    "ULTRA_LATE_WIPE":           120,
    "LATE_GAME_WIPE":            120,
    "STOMP_THROW":               120,
    "MULTIPLE_T3_TOWERS_DOWN":   120,
    "T3_TOWER_FALL":             120,
    "MAJOR_COMEBACK":            120,
    "COMEBACK":                  120,
    "LEAD_SWING_60S":            120,
    "LEAD_SWING_30S":            120,
    "KILL_CONFIRMED_LEAD_SWING": 120,
    "KILL_BURST_30S":            90,
    "T2_TOWER_FALL":             90,
    "MULTIPLE_T2_TOWERS_DOWN":   90,
    "ALL_T2_TOWERS_DOWN":        90,
    "OBJECTIVE_CONVERSION_T2":   90,
    "OBJECTIVE_CONVERSION_T3":   120,
    "OBJECTIVE_CONVERSION_T4":   120,
}
# Safety net: force-close any position that stays open longer than this (game_over missed).
MAX_HOLD_HOURS     = float(os.getenv("MAX_HOLD_HOURS",      "4"))

CSV_LOG_PATH = os.getenv("CSV_LOG_PATH", "logs/signals.csv")
PAPER_TRADES_CSV_PATH = os.getenv("PAPER_TRADES_CSV_PATH", "logs/paper_trades.csv")
POSITIONS_CSV_PATH = os.getenv("POSITIONS_CSV_PATH", "logs/positions.csv")
PNL_SUMMARY_CSV_PATH = os.getenv("PNL_SUMMARY_CSV_PATH", "logs/pnl_summary.csv")
LATENCY_CSV_PATH = os.getenv("LATENCY_CSV_PATH", "logs/latency.csv")
LIVE_LEAGUE_RAW_CSV_PATH = os.getenv("LIVE_LEAGUE_RAW_CSV_PATH", "logs/liveleague_raw.csv")
LIVE_LEAGUE_FEATURES_CSV_PATH = os.getenv("LIVE_LEAGUE_FEATURES_CSV_PATH", "logs/liveleague_features.csv")
LIVE_LEAGUE_RAW_JSONL_PATH = os.getenv("LIVE_LEAGUE_RAW_JSONL_PATH", "logs/liveleague_raw.jsonl")
SOURCE_DELAY_CSV_PATH = os.getenv("SOURCE_DELAY_CSV_PATH", "logs/source_delay.csv")
MARKOUTS_CSV_PATH = os.getenv("MARKOUTS_CSV_PATH", "logs/markouts.csv")
BOOK_REFRESH_RESCUE_CSV_PATH = os.getenv("BOOK_REFRESH_RESCUE_CSV_PATH", "logs/book_refresh_rescue.csv")

if MODE not in {"paper", "live"}:
    raise RuntimeError("MODE must be paper or live-test compatible live.")
# Event / reaction-lag logging
DOTA_EVENTS_CSV_PATH = os.getenv("DOTA_EVENTS_CSV_PATH", "logs/dota_events.csv")
BOOK_EVENTS_CSV_PATH = os.getenv("BOOK_EVENTS_CSV_PATH", "logs/book_events.csv")
EVENT_LEAD_SWING_30S = int(os.getenv("EVENT_LEAD_SWING_30S", "1500"))
EVENT_LEAD_SWING_60S = int(os.getenv("EVENT_LEAD_SWING_60S", "3000"))
EVENT_COOLDOWN_GAME_SECONDS = int(os.getenv("EVENT_COOLDOWN_GAME_SECONDS", "15"))
REACTION_WINDOW_SECONDS = int(os.getenv("REACTION_WINDOW_SECONDS", "30"))
BOOK_MOVE_MIN_CENTS = float(os.getenv("BOOK_MOVE_MIN_CENTS", "0.01"))
REALTIME_STATS_ENABLED = os.getenv("REALTIME_STATS_ENABLED", "false").lower() in {"1", "true", "yes"}
REALTIME_STATS_STALE_SEC = int(os.getenv("REALTIME_STATS_STALE_SEC", "30"))


def _config_hash() -> str:
    keys = [
        "LIVE_TRADING", "MAX_TOTAL_LIVE_USD", "MAX_TRADE_USD", "ORDER_TYPE",
        "TRADE_EVENTS", "ALLOW_CONFIRMATION_ONLY_LIVE_TRADES",
        "DISABLE_STRUCTURE_TRADES", "MAX_BOOK_AGE_MS", "MAX_STEAM_AGE_MS",
        "MAX_SOURCE_UPDATE_AGE_SEC", "MIN_LAG", "MIN_EXECUTABLE_EDGE",
        "MAX_SPREAD", "MIN_ASK_SIZE_USD", "PAPER_EXECUTION_DELAY_MS",
        "EVENT_LEAD_SWING_30S", "EVENT_LEAD_SWING_60S",
        "BOOK_REFRESH_RESCUE_CSV_PATH",
    ]
    payload = {key: os.getenv(key) for key in keys}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


CONFIG_HASH = _config_hash()
