#!/usr/bin/env bash
set -euo pipefail

# Automated event validation pipeline:
# 1. Archive current logs to the specified run directory
# 2. Clear logs
# 3. Start bot with given PAPER_EXECUTION_DELAY_MS
# 4. Wait for data collection (configurable duration)
# 5. Stop bot
# 6. Run analysis scripts
# 7. Summarize results

DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ="$DIR"
RUN_DIR="${1:?Usage: $0 <run_dir> <delay_ms> <duration_seconds>}"
DELAY_MS="${2:?Usage: $0 <run_dir> <delay_ms> <duration_seconds>}"
DURATION_SEC="${3:?Usage: $0 <run_dir> <delay_ms> <duration_seconds>}"

LOGDIR="$PROJ/logs"

echo "=== Event Validation Run ==="
echo "  Run dir:  $RUN_DIR"
echo "  Delay ms: $DELAY_MS"
echo "  Duration: ${DURATION_SEC}s"
echo ""

# Step 1: Archive any existing logs
mkdir -p "$RUN_DIR/logs"
for f in signals.csv dota_events.csv latency.csv paper_trades.csv book_events.csv raw_snapshots.csv positions.csv pnl_summary.csv book_refresh_rescue.csv source_delay.csv liveleague_features.csv markouts.csv stale_ask_survival.csv reaction_lag.csv raw_lag.csv; do
    if [ -f "$LOGDIR/$f" ]; then
        cp "$LOGDIR/$f" "$RUN_DIR/logs/" 2>/dev/null || true
    fi
done
echo "[1/7] Archived existing logs to $RUN_DIR/logs/"

# Step 2: Clear logs
rm -f "$LOGDIR"/*.csv "$LOGDIR"/*.jsonl 2>/dev/null || true
echo "[2/7] Cleared logs/"

# Step 3: Start bot
BOT_PID_FILE="$RUN_DIR/bot.pid"
env \
  LIVE_TRADING=false \
  PAPER_EXECUTION_DELAY_MS="$DELAY_MS" \
  STEAM_POLL_SECONDS=0.5 \
  MAX_BOOK_AGE_MS=2500 \
  MAX_STEAM_AGE_MS=1500 \
  MAX_SOURCE_UPDATE_AGE_SEC=45 \
  MIN_EXECUTABLE_EDGE=0.08 \
  MIN_LAG=0.08 \
  MAX_SPREAD=0.06 \
  MIN_ASK_SIZE_USD=25 \
  "$PROJ/.venv/bin/python" -u "$PROJ/main.py" \
  > "$RUN_DIR/bot_output.log" 2>&1 &
echo $! > "$BOT_PID_FILE"
BOT_PID=$(cat "$BOT_PID_FILE")
echo "[3/7] Bot started (PID=$BOT_PID, delay=${DELAY_MS}ms)"

# Step 4: Wait for collection
echo "[4/7] Collecting for ${DURATION_SEC}s..."
sleep "$DURATION_SEC"

# Step 5: Stop bot
if kill -0 "$BOT_PID" 2>/dev/null; then
    kill "$BOT_PID" 2>/dev/null || true
    sleep 2
    # Force kill if still running
    if kill -0 "$BOT_PID" 2>/dev/null; then
        kill -9 "$BOT_PID" 2>/dev/null || true
    fi
fi
echo "[5/7] Bot stopped"

# Step 6: Run analysis scripts
cd "$PROJ"
.venv/bin/python reaction_lag.py 2>&1 || true
.venv/bin/python mark_positions.py 2>&1 || true
.venv/bin/python analyze_logs.py 2>&1 || true
echo "[6/7] Analysis scripts complete"

# Step 7: Copy final logs to run dir
for f in signals.csv dota_events.csv latency.csv paper_trades.csv book_events.csv raw_snapshots.csv positions.csv pnl_summary.csv book_refresh_rescue.csv source_delay.csv liveleague_features.csv markouts.csv stale_ask_survival.csv reaction_lag.csv raw_lag.csv; do
    if [ -f "$LOGDIR/$f" ]; then
        cp "$LOGDIR/$f" "$RUN_DIR/logs/" 2>/dev/null || true
    fi
done
echo "[7/7] Final logs archived to $RUN_DIR/logs/"

echo ""
echo "=== Run Complete ==="
echo "  Signals:   $(wc -l < "$RUN_DIR/logs/signals.csv" 2>/dev/null || echo 0)"
echo "  Events:    $(wc -l < "$RUN_DIR/logs/dota_events.csv" 2>/dev/null || echo 0)"
echo "  Trades:    $(wc -l < "$RUN_DIR/logs/paper_trades.csv" 2>/dev/null || echo 0)"
echo "  Rescues:   $(wc -l < "$RUN_DIR/logs/book_refresh_rescue.csv" 2>/dev/null || echo 0)"