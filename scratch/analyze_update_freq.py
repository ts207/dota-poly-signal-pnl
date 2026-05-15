import pandas as pd
import numpy as np

def analyze_update_frequency(book_events_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/book_events.csv'):
    try:
        df = pd.read_csv(book_events_path, header=0)
    except Exception as e:
        print(f"Error: {e}")
        return

    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    
    # Analyze by token
    stats = []
    for asset_id, group in df.groupby('asset_id'):
        group = group.sort_values('timestamp_utc')
        gaps = group['timestamp_utc'].diff().dt.total_seconds().dropna()
        if not gaps.empty:
            stats.append({
                'asset_id': asset_id,
                'count': len(group),
                'mean_gap': gaps.mean(),
                'max_gap': gaps.max(),
                'median_gap': gaps.median()
            })
    
    stats_df = pd.DataFrame(stats).sort_values('count', ascending=False)
    print("Token Update Frequency Stats:")
    print(stats_df.head(20).to_string())

if __name__ == "__main__":
    analyze_update_frequency()
