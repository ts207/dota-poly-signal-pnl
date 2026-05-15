#!/usr/bin/env bash
set -euo pipefail

# Latency validation pipeline:
#   Sweep mode:  ./run_validation.sh <sweep_run_dir> <duration_seconds>
#   Single mode: ./run_validation.sh --single <run_dir> <delay_ms> <duration_seconds>
#
# Sweep mode runs paper validation at 0,250,500,1000,2000ms, archives each
# scenario, then writes one merged latency-validation report.

DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ="$DIR"
LOGDIR="$PROJ/logs"
PYTHON="$PROJ/.venv/bin/python"
DELAYS_MS=(0 250 500 1000 2000)
LOG_FILES=(
  signals.csv
  dota_events.csv
  latency.csv
  paper_trades.csv
  book_events.csv
  raw_snapshots.csv
  positions.csv
  pnl_summary.csv
  book_refresh_rescue.csv
  source_delay.csv
  liveleague_features.csv
  rich_context.csv
  markouts.csv
  stale_ask_survival.csv
  reaction_lag.csv
  raw_lag.csv
)

usage() {
  cat <<EOF
Usage:
  $0 <sweep_run_dir> <duration_seconds>
  $0 --single <run_dir> <delay_ms> <duration_seconds>

Sweep mode runs delays: ${DELAYS_MS[*]} ms.
EOF
}

utc_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

git_commit() {
  git -C "$PROJ" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

write_metadata() {
  local run_dir="$1"
  local delay_ms="$2"
  local duration_sec="$3"
  local start_utc="$4"
  local end_utc="$5"
  local status="$6"
  local config_hash="$7"

  cat > "$run_dir/metadata.txt" <<EOF
run_dir=$run_dir
delay_ms=$delay_ms
duration_seconds=$duration_sec
start_utc=$start_utc
end_utc=$end_utc
status=$status
git_commit=$(git_commit)
config_hash=$config_hash
env.LIVE_TRADING=false
env.PAPER_EXECUTION_DELAY_MS=$delay_ms
env.STEAM_POLL_SECONDS=0.5
env.MAX_BOOK_AGE_MS=2500
env.MAX_STEAM_AGE_MS=1500
env.MAX_SOURCE_UPDATE_AGE_SEC=45
env.MIN_EXECUTABLE_EDGE=0.08
env.MIN_LAG=0.08
env.MAX_SPREAD=0.06
env.MIN_ASK_SIZE_USD=25
EOF
}

archive_logs() {
  local run_dir="$1"
  mkdir -p "$run_dir/logs"
  for f in "${LOG_FILES[@]}"; do
    if [ -f "$LOGDIR/$f" ]; then
      cp "$LOGDIR/$f" "$run_dir/logs/" 2>/dev/null || true
    fi
  done
}

clear_logs() {
  rm -f "$LOGDIR"/*.csv "$LOGDIR"/*.jsonl 2>/dev/null || true
}

run_analysis() {
  local run_dir="$1"
  cd "$PROJ"
  {
    "$PYTHON" reaction_lag.py 2>&1 || true
    "$PYTHON" mark_positions.py 2>&1 || true
    "$PYTHON" analyze_logs.py 2>&1 || true
  } | tee "$run_dir/analysis_output.log"
}

extract_config_hash() {
  local run_dir="$1"
  if [ -f "$run_dir/logs/latency.csv" ]; then
    awk -F, 'NR==1 {for (i=1; i<=NF; i++) if ($i=="config_hash") c=i} NR==2 && c {print $c; exit}' "$run_dir/logs/latency.csv"
  fi
}

run_single() {
  local run_dir="$1"
  local delay_ms="$2"
  local duration_sec="$3"
  local start_utc
  local end_utc
  local status="complete"

  mkdir -p "$run_dir"
  start_utc="$(utc_now)"

  echo "=== Event Validation Run ==="
  echo "  Run dir:  $run_dir"
  echo "  Delay ms: $delay_ms"
  echo "  Duration: ${duration_sec}s"
  echo ""

  mkdir -p "$LOGDIR"

  # Archive any pre-existing logs for diagnostics before clearing.
  mkdir -p "$run_dir/pre_existing_logs"
  for f in "${LOG_FILES[@]}"; do
    if [ -f "$LOGDIR/$f" ]; then
      cp "$LOGDIR/$f" "$run_dir/pre_existing_logs/" 2>/dev/null || true
    fi
  done
  echo "[1/7] Archived pre-existing logs to $run_dir/pre_existing_logs/"

  clear_logs
  echo "[2/7] Cleared logs/"

  local bot_pid_file="$run_dir/bot.pid"
  env \
    LIVE_TRADING=false \
    PAPER_EXECUTION_DELAY_MS="$delay_ms" \
    STEAM_POLL_SECONDS=0.5 \
    MAX_BOOK_AGE_MS=2500 \
    MAX_STEAM_AGE_MS=1500 \
    MAX_SOURCE_UPDATE_AGE_SEC=45 \
    MIN_EXECUTABLE_EDGE=0.08 \
    MIN_LAG=0.08 \
    MAX_SPREAD=0.06 \
    MIN_ASK_SIZE_USD=25 \
    "$PYTHON" -u "$PROJ/main.py" \
    > "$run_dir/bot_output.log" 2>&1 &
  echo $! > "$bot_pid_file"
  local bot_pid
  bot_pid="$(cat "$bot_pid_file")"
  echo "[3/7] Bot started (PID=$bot_pid, delay=${delay_ms}ms)"

  echo "[4/7] Collecting for ${duration_sec}s..."
  sleep "$duration_sec"

  if kill -0 "$bot_pid" 2>/dev/null; then
    kill "$bot_pid" 2>/dev/null || true
    sleep 2
    if kill -0 "$bot_pid" 2>/dev/null; then
      kill -9 "$bot_pid" 2>/dev/null || true
      status="force_killed"
    fi
  else
    status="bot_exited_early"
  fi
  echo "[5/7] Bot stopped"

  run_analysis "$run_dir"
  echo "[6/7] Analysis scripts complete"

  archive_logs "$run_dir"
  echo "[7/7] Final logs archived to $run_dir/logs/"

  end_utc="$(utc_now)"
  write_metadata "$run_dir" "$delay_ms" "$duration_sec" "$start_utc" "$end_utc" "$status" "$(extract_config_hash "$run_dir")"

  echo ""
  echo "=== Run Complete ==="
  echo "  Signals:   $(wc -l < "$run_dir/logs/signals.csv" 2>/dev/null || echo 0)"
  echo "  Events:    $(wc -l < "$run_dir/logs/dota_events.csv" 2>/dev/null || echo 0)"
  echo "  Trades:    $(wc -l < "$run_dir/logs/paper_trades.csv" 2>/dev/null || echo 0)"
  echo "  Rescues:   $(wc -l < "$run_dir/logs/book_refresh_rescue.csv" 2>/dev/null || echo 0)"
}

run_sweep() {
  local sweep_dir="$1"
  local duration_sec="$2"

  mkdir -p "$sweep_dir"
  cat > "$sweep_dir/metadata.txt" <<EOF
sweep_dir=$sweep_dir
duration_seconds=$duration_sec
delays_ms=${DELAYS_MS[*]}
start_utc=$(utc_now)
git_commit=$(git_commit)
EOF

  echo "=== Latency Validation Sweep ==="
  echo "  Sweep dir: $sweep_dir"
  echo "  Duration:  ${duration_sec}s per delay"
  echo "  Delays:    ${DELAYS_MS[*]} ms"
  echo ""

  for delay_ms in "${DELAYS_MS[@]}"; do
    local scenario_dir
    scenario_dir="$(printf "%s/delay_%04dms" "$sweep_dir" "$delay_ms")"
    run_single "$scenario_dir" "$delay_ms" "$duration_sec"
    echo ""
  done

  {
    echo "end_utc=$(utc_now)"
  } >> "$sweep_dir/metadata.txt"

  "$PYTHON" "$PROJ/scripts/merge_latency_validation.py" "$sweep_dir"

  echo ""
  echo "=== Sweep Complete ==="
  echo "  Summary CSV: $sweep_dir/latency_validation_summary.csv"
  echo "  Report:      $sweep_dir/latency_validation_report.md"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "${1:-}" = "--single" ]; then
  if [ "$#" -ne 4 ]; then
    usage >&2
    exit 2
  fi
  run_single "$2" "$3" "$4"
  exit 0
fi

if [ "$#" -ne 2 ]; then
  usage >&2
  exit 2
fi

run_sweep "$1" "$2"
