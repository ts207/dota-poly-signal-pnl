import pandas as pd
import numpy as np

def analyze_detection_latency(latency_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/latency.csv'):
    try:
        # Load latency logs. 
        # Headers from storage.py:
        # "timestamp_utc", "run_id", "code_version", "config_hash",
        # "match_id", "market_name", "event_type", "cluster_event_types",
        # "event_direction", "game_time_sec", "data_source",
        # "steam_received_at_ns", "steam_source_update_age_sec", "stream_delay_s",
        # "event_detected_ns", "signal_eval_start_ns", "signal_evaluated_ns", "event_detection_latency_ms", "signal_eval_latency_ms",
        # ...
        df = pd.read_csv(latency_path)
    except Exception as e:
        print(f"Error reading latency logs: {e}")
        return

    if df.empty:
        print("No latency data found.")
        return

    print(f"Analyzing {len(df)} latency records.")

    # Convert numeric columns
    numeric_cols = ['event_detection_latency_ms', 'signal_eval_latency_ms', 'steam_source_update_age_sec', 'stream_delay_s']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print("\nLatency Summary (ms):")
    stats = df[['event_detection_latency_ms', 'signal_eval_latency_ms']].describe()
    print(stats)

    print("\nSource Age Summary (sec):")
    source_stats = df[['steam_source_update_age_sec', 'stream_delay_s']].describe()
    print(source_stats)

    print("\nLatency by Event Type (ms):")
    event_stats = df.groupby('event_type')['event_detection_latency_ms'].agg(['mean', 'median', 'max', 'count'])
    print(event_stats)

if __name__ == "__main__":
    analyze_detection_latency()
