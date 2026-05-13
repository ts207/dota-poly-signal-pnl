from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

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
TRADE_EVENTS = {e.strip() for e in os.getenv("TRADE_EVENTS", "ULTRA_LATE_WIPE,LATE_GAME_WIPE,STOMP_THROW,MAJOR_COMEBACK,EXTREME_LEAD_SWING_30S,OBJECTIVE_CONVERSION_T3,OBJECTIVE_CONVERSION_T4,FIRST_T4_TOWER_FALL,SECOND_T4_TOWER_FALL").split(",") if e.strip()}
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
