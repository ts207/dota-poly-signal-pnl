import pandas as pd
import numpy as np

def analyze_skipped_profitability(markouts_path='/home/irene/dd/dota/dota-poly-signal-pnl/logs/signal_markouts.csv'):
    try:
        df = pd.read_csv(markouts_path)
    except Exception as e:
        print(f"Error reading markouts: {e}")
        return

    if df.empty:
        print("No markout data found.")
        return

    # Filter to skipped signals
    skipped = df[df['decision'] == 'skip'].copy()
    if skipped.empty:
        print("No skipped signals found in markouts.")
        return

    print(f"Analyzing {len(skipped)} skipped signals.")

    # Convert markout columns to numeric
    for col in ['markout_3s', 'markout_10s', 'markout_30s', 'edge_after_3s', 'edge_after_10s', 'edge_after_30s']:
        skipped[col] = pd.to_numeric(skipped[col], errors='coerce')

    print("\nMarkout Summary for Skipped Signals (Price Move):")
    stats = skipped[['markout_3s', 'markout_10s', 'markout_30s']].describe()
    print(stats)

    # Opportunity cost: How many would have been profitable?
    # A positive markout means the price moved in favor of the signal.
    # For a YES buy, markout > 0 is good.
    
    print("\nProfitable if Executed (Markout > 0):")
    for sec in [3, 10, 30]:
        col = f'markout_{sec}s'
        profitable = skipped[skipped[col] > 0]
        pct = (len(profitable) / len(skipped)) * 100
        print(f"{sec}s: {len(profitable)}/{len(skipped)} ({pct:.1f}%)")

    print("\nMarkout by Skip Reason (Median 10s Markout):")
    reason_stats = skipped.groupby('skip_reason')['markout_10s'].agg(['median', 'mean', 'count'])
    print(reason_stats)

    # Highlight specific high-alpha events that were skipped
    print("\nTop 10 High-Alpha Skipped Signals (by 30s Markout):")
    top_skipped = skipped.sort_values('markout_30s', ascending=False).head(10)
    print(top_skipped[['signal_timestamp_utc', 'event_type', 'skip_reason', 'markout_30s', 'reference_price']])

if __name__ == "__main__":
    analyze_skipped_profitability()
