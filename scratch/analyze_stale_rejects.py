import pandas as pd
import numpy as np
from datetime import datetime

def analyze_stale_rejections(signals_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/signals.csv', book_events_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/book_events.csv'):
    try:
        # Load signals. Use the headers from SignalLogger
        headers = [
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
            "proxy_market_type", "is_game3_match_proxy",
            "series_score_yes", "series_score_no",
            "current_game_number", "series_type",
            "structure_uncertainty_penalty",
        ]
        signals = pd.read_csv(signals_path, names=headers)
    except Exception as e:
        print(f"Error reading signals: {e}")
        return

    stale_signals = signals[signals['skip_reason'] == 'book_stale']
    if stale_signals.empty:
        print("No 'book_stale' rejections found in signals.csv")
        return

    print(f"Found {len(stale_signals)} 'book_stale' rejections.")

    try:
        # book_events headers from BookEventLogger
        book_headers = [
            "timestamp_utc", "asset_id", "event_type", "best_bid", "best_ask", "bid_size", "ask_size",
            "mid", "spread", "source_event_type",
        ]
        book_events = pd.read_csv(book_events_path, names=book_headers)
    except Exception as e:
        print(f"Error reading book_events: {e}")
        return

    results = []
    for _, signal in stale_signals.iterrows():
        token_id = str(signal['token_id'])
        sig_time = pd.to_datetime(signal['timestamp_utc'])
        
        # Find the last book update for this token before the signal
        token_updates = book_events[book_events['asset_id'].astype(str) == token_id].copy()
        if token_updates.empty:
            last_update_age = "Infinity (No updates found)"
        else:
            token_updates['timestamp_utc'] = pd.to_datetime(token_updates['timestamp_utc'])
            prior_updates = token_updates[token_updates['timestamp_utc'] <= sig_time]
            
            if prior_updates.empty:
                last_update_age = "Infinity (No prior updates found)"
            else:
                last_update = prior_updates.iloc[-1]
                last_update_age = (sig_time - last_update['timestamp_utc']).total_seconds()

        results.append({
            'timestamp': signal['timestamp_utc'],
            'token_id': token_id,
            'event_type': signal['event_type'],
            'last_update_age_sec': last_update_age,
            'book_age_ms_reported': signal.get('book_age_ms', 'N/A')
        })

    df_results = pd.DataFrame(results)
    print("\nStale Rejection Analysis:")
    print(df_results.to_string())
    
    print("\nSummary of Update Ages (Seconds):")
    valid_ages = [r['last_update_age_sec'] for r in results if isinstance(r['last_update_age_sec'], (int, float))]
    if valid_ages:
        print(f"Mean: {np.mean(valid_ages):.2f}")
        print(f"Min:  {np.min(valid_ages):.2f}")
        print(f"Max:  {np.max(valid_ages):.2f}")
        print(f"Median: {np.median(valid_ages):.2f}")
    else:
        print("No valid update ages found (all 'Infinity'). This suggests the tokens were never updated via WS.")

if __name__ == "__main__":
    analyze_stale_rejections()
