#!/usr/bin/env python3
"""
Builds a labeled evaluation dataset for the dota_fair_model.
Joins feature logs with external match outcomes and incorporates signal metadata.
"""

from __future__ import annotations

import os
import sys
import pandas as pd
from pathlib import Path

# Paths
ROOT = Path(__file__).parent.parent
LOGS_DIR = ROOT / "logs"
LABELS_DIR = ROOT / "labels"
LABELS_FILE = LABELS_DIR / "match_results.csv"
OUTPUT_FILE = LOGS_DIR / "model_eval_dataset.csv"
COVERAGE_FILE = LOGS_DIR / "model_eval_dataset_coverage.md"

REQUIRED_LOGS = [
    "liveleague_features.csv",
    "raw_snapshots.csv",
    "dota_events.csv",
    "signals.csv",
    "book_events.csv"
]

def get_phase_bucket(game_time_sec: float) -> str:
    minutes = game_time_sec / 60
    if minutes < 10: return "0_10"
    if minutes < 20: return "10_20"
    if minutes < 30: return "20_30"
    if minutes < 40: return "30_40"
    return "40_plus"

def main():
    print("Building model evaluation dataset...")

    # 1. Verification
    if not LABELS_FILE.exists():
        print(f"CRITICAL: {LABELS_FILE} is missing. Refusing to run.")
        sys.exit(1)

    # Load labels
    labels = pd.read_csv(LABELS_FILE)
    if "match_id" not in labels.columns or "radiant_win" not in labels.columns:
        print(f"CRITICAL: {LABELS_FILE} must contain 'match_id' and 'radiant_win'.")
        sys.exit(1)
    
    # Ensure match_id is string for joining
    labels["match_id"] = labels["match_id"].astype(str)
    labeled_match_ids = set(labels["match_id"].unique())

    # Load primary features
    features_path = LOGS_DIR / "liveleague_features.csv"
    if not features_path.exists():
        print(f"CRITICAL: {features_path} missing.")
        sys.exit(1)
    
    df = pd.read_csv(features_path)
    df["match_id"] = df["match_id"].astype(str)
    
    # 2. Refuse if radiant_win is missing for a match present in logs
    log_match_ids = set(df["match_id"].unique())
    missing_labels = log_match_ids - labeled_match_ids
    if missing_labels:
        print(f"CRITICAL: Missing radiant_win labels for matches: {missing_labels}")
        print("All matches in logs must have a corresponding label in match_results.csv.")
        sys.exit(1)

    # 3. Join with labels
    df = df.merge(labels[["match_id", "radiant_win"]], on="match_id", how="inner")

    # 4. Add phase buckets
    df["phase_bucket"] = df["game_time_sec"].apply(get_phase_bucket)

    # 5. Net-worth baseline
    # Some logs use 'net_worth_diff', others might need it computed
    if "net_worth_diff" not in df.columns and "radiant_net_worth" in df.columns:
        df["net_worth_diff"] = df["radiant_net_worth"] - df["dire_net_worth"]
    
    if "net_worth_diff" in df.columns:
        df["nw_sign_prediction"] = (df["net_worth_diff"] > 0).astype(int)
        df["abs_net_worth_diff"] = df["net_worth_diff"].abs()

    # 6. Join with signals metadata
    signals_path = LOGS_DIR / "signals.csv"
    if signals_path.exists():
        signals = pd.read_csv(signals_path)
        signals["match_id"] = signals["match_id"].astype(str)
        # We join on match_id and game_time_sec (nearest match)
        # For simplicity in this script, we'll do a merge on match_id and game_time_sec
        # but in reality snapshots might not perfectly align with signal timestamps.
        # We'll keep the columns as requested.
        sig_cols = [
            "match_id", "game_time_sec", "event_type", "event_tier", 
            "event_family", "event_quality", "fair_price", 
            "executable_price", "executable_edge", "decision", "skip_reason"
        ]
        available_sig_cols = [c for c in sig_cols if c in signals.columns]
        df = df.merge(signals[available_sig_cols], on=["match_id", "game_time_sec"], how="left")

    # 7. Market reaction placeholders (Actual computation requires time-series join with book_events)
    # Since we are building the dataset, we add the columns even if null
    reaction_cols = ["future_bid_3s", "future_bid_10s", "future_bid_30s", "markout_3s", "markout_10s", "markout_30s"]
    for col in reaction_cols:
        if col not in df.columns:
            df[col] = None

    # Try to extract markouts from latency.csv if it exists
    latency_path = LOGS_DIR / "latency.csv"
    if latency_path.exists():
        latency = pd.read_csv(latency_path)
        latency["match_id"] = latency["match_id"].astype(str)
        lat_cols = ["match_id", "game_time_sec", "markout_3s", "markout_10s", "markout_30s"]
        available_lat = [c for c in lat_cols if c in latency.columns]
        df = df.merge(latency[available_lat], on=["match_id", "game_time_sec"], how="left", suffixes=('', '_lat'))
        # Consolidate columns if needed
        for m in ["markout_3s", "markout_10s", "markout_30s"]:
            if f"{m}_lat" in df.columns:
                df[m] = df[m].fillna(df[f"{m}_lat"])
                df.drop(columns=[f"{m}_lat"], inplace=True)

    # 8. Final Export
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Dataset saved to {OUTPUT_FILE}")

    # Coverage Report
    with open(COVERAGE_FILE, "w") as f:
        f.write("# Model Evaluation Dataset Coverage Report\n\n")
        f.write(f"- **Total Rows**: {len(df)}\n")
        f.write(f"- **Total Matches**: {df['match_id'].nunique()}\n")
        f.write("\n### Phase Distribution\n")
        f.write(df["phase_bucket"].value_counts().to_markdown())
        f.write("\n\n### Label Distribution (radiant_win)\n")
        f.write(df["radiant_win"].value_counts().to_markdown())
        f.write("\n\n### Signal/Event Coverage\n")
        if "event_type" in df.columns:
            f.write(f"- Rows with events: {df['event_type'].notnull().sum()}\n")
        else:
            f.write("- Event data: Not available\n")
        
        f.write("\n### Field Availability\n")
        avail = df.notnull().mean() * 100
        f.write(avail.to_markdown())
        f.write("\n")

    print(f"Coverage report saved to {COVERAGE_FILE}")

if __name__ == "__main__":
    main()
