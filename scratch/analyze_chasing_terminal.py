import pandas as pd
import numpy as np

def analyze_chasing_terminal_price(signals_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/signals.csv', markouts_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/markouts.csv'):
    try:
        headers = [
            "timestamp_utc", "run_id", "code_version", "config_hash",
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
            "proxy_market_type", "is_game3_match_proxy",
            "series_score_yes", "series_score_no",
            "current_game_number", "series_type",
            "structure_uncertainty_penalty",
        ]
        signals = pd.read_csv(signals_path, names=headers)
    except Exception as e:
        print(f"Error reading signals: {e}")
        return

    chasing_signals = signals[signals['skip_reason'] == 'chasing_terminal_price']
    if chasing_signals.empty:
        print("No 'chasing_terminal_price' rejections found in signals.csv")
        return

    print(f"Found {len(chasing_signals)} 'chasing_terminal_price' rejections.")

    # Join with markouts if possible
    try:
        markouts = pd.read_csv(markouts_path)
        # Assuming markouts can be matched by timestamp_utc or similar. 
        # Actually storage.py shows SignalMarkoutLogger has signal_timestamp_utc.
        # But let's check the markouts header.
    except Exception:
        markouts = None

    results = []
    for _, signal in chasing_signals.iterrows():
        results.append({
            'timestamp': signal['timestamp_utc'],
            'event_type': signal['event_type'],
            'ask': signal['ask'],
            'bid': signal['bid'],
            'executable_edge': signal['executable_edge'],
            'expected_move': signal['expected_move'],
            'fair_price': signal['fair_price'],
            'remaining_move': signal['remaining_move']
        })

    df_results = pd.DataFrame(results)
    print("\nChasing Terminal Price Analysis:")
    print(df_results.to_string())

    print("\nSummary by Event Type:")
    print(df_results.groupby('event_type')['ask'].describe())

if __name__ == "__main__":
    analyze_chasing_terminal_price()
