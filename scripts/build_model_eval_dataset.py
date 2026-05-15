#!/usr/bin/env python3
"""
Builds a labeled evaluation dataset for the dota_fair_model.
Joins feature logs with external match outcomes and incorporates signal metadata
using nearest-time joins.
"""

from __future__ import annotations

import argparse
import os
import sys
import pandas as pd
from pathlib import Path

def get_phase_bucket(game_time_sec: float) -> str:
    minutes = game_time_sec / 60
    if minutes < 10: return "0_10"
    if minutes < 20: return "10_20"
    if minutes < 30: return "20_30"
    if minutes < 40: return "30_40"
    return "40_plus"

def main():
    parser = argparse.ArgumentParser(description="Build labeled evaluation dataset.")
    parser.add_argument("--labels", default="labels/match_results.csv", help="Path to external labels CSV")
    parser.add_argument("--logs", default="logs", help="Path to logs directory")
    parser.add_argument("--output", default="logs/model_eval_dataset.csv", help="Path to output CSV")
    parser.add_argument("--strict-labels", type=str, default="false", help="If true, refuse to run if any match is unlabeled")
    args = parser.parse_args()

    strict_labels = args.strict_labels.lower() == "true"
    labels_file = Path(args.labels)
    logs_dir = Path(args.logs)
    output_file = Path(args.output)
    coverage_file = output_file.with_suffix(".md").parent / (output_file.stem + "_coverage.md")

    print(f"Building model evaluation dataset: {output_file}")

    # 1. Verification
    if not labels_file.exists():
        print(f"CRITICAL: {labels_file} is missing. Refusing to run.")
        sys.exit(1)

    labels = pd.read_csv(labels_file)
    if "match_id" not in labels.columns or "radiant_win" not in labels.columns:
        print(f"CRITICAL: {labels_file} must contain 'match_id' and 'radiant_win'.")
        sys.exit(1)
    
    labels["match_id"] = labels["match_id"].astype(str)
    labeled_match_ids = set(labels["match_id"].unique())

    features_path = logs_dir / "rich_context.csv"
    if not features_path.exists():
        features_path = logs_dir / "liveleague_features.csv"
    
    if not features_path.exists():
        print(f"CRITICAL: Rich context features missing (tried rich_context.csv and liveleague_features.csv in {logs_dir}).")
        sys.exit(1)
    
    df_features = pd.read_csv(features_path)
    df_features["match_id"] = df_features["match_id"].astype(str)
    input_rows = len(df_features)
    
    # 2. Labeling Logic
    log_match_ids = set(df_features["match_id"].unique())
    unlabeled_matches = log_match_ids - labeled_match_ids
    
    if strict_labels and unlabeled_matches:
        print(f"CRITICAL: Strict mode. Missing labels for: {unlabeled_matches}")
        sys.exit(1)
    
    dropped_matches = []
    if unlabeled_matches:
        dropped_matches = list(unlabeled_matches)
        df_features = df_features[~df_features["match_id"].isin(unlabeled_matches)]
        print(f"Dropped {len(unlabeled_matches)} unlabeled matches.")

    df = df_features.merge(labels[["match_id", "radiant_win"]], on="match_id", how="inner")
    labeled_matches_count = df["match_id"].nunique()

    # merge_asof requires non-null sorted 'on' field and matching dtypes
    df = df.dropna(subset=["game_time_sec"])
    df["game_time_sec"] = df["game_time_sec"].astype(float)
    df = df.sort_values("game_time_sec")

    # 3. Nearest-Time Join for Signals
    signals_path = logs_dir / "signals.csv"
    if signals_path.exists():
        signals = pd.read_csv(signals_path)
        signals["match_id"] = signals["match_id"].astype(str)
        
        sig_cols = [
            "match_id", "game_time_sec", "event_type", "event_tier", 
            "event_family", "event_quality", "fair_price", 
            "executable_price", "executable_edge", "decision", "skip_reason"
        ]
        available_sig_cols = [c for c in sig_cols if c in signals.columns]
        df_sig = signals[available_sig_cols].copy()
        
        # Suffix signal fields clearly (except match_id and game_time_sec used for merge_asof)
        rename_map = {c: f"sig_{c}" for c in available_sig_cols if c not in ["match_id", "game_time_sec"]}
        df_sig.rename(columns=rename_map, inplace=True)

        df_sig = df_sig.dropna(subset=["game_time_sec"])
        df_sig["game_time_sec"] = df_sig["game_time_sec"].astype(float)
        df_sig = df_sig.sort_values("game_time_sec")
        
        df = pd.merge_asof(
            df, 
            df_sig, 
            on="game_time_sec", 
            by="match_id", 
            tolerance=60, 
            direction="nearest"
        )

    # 4. Nearest-Time Join for Latency/Markouts
    latency_path = logs_dir / "latency.csv"
    if latency_path.exists():
        latency = pd.read_csv(latency_path)
        latency["match_id"] = latency["match_id"].astype(str)
        
        lat_cols = ["match_id", "game_time_sec", "markout_3s", "markout_10s", "markout_30s"]
        available_lat = [c for c in lat_cols if c in latency.columns]
        df_lat = latency[available_lat].copy()
        
        # Suffix markout fields
        rename_map_lat = {c: f"lat_{c}" for c in available_lat if c not in ["match_id", "game_time_sec"]}
        df_lat.rename(columns=rename_map_lat, inplace=True)

        df_lat = df_lat.dropna(subset=["game_time_sec"])
        df_lat["game_time_sec"] = df_lat["game_time_sec"].astype(float)
        df_lat = df_lat.sort_values("game_time_sec")
        
        df = pd.merge_asof(
            df, 
            df_lat, 
            on="game_time_sec", 
            by="match_id", 
            tolerance=60, 
            direction="nearest"
        )

    # 5. Add phase buckets & NW baseline
    df["phase_bucket"] = df["game_time_sec"].apply(get_phase_bucket)
    if "net_worth_diff" not in df.columns and "radiant_net_worth" in df.columns:
        df["net_worth_diff"] = df["radiant_net_worth"] - df["dire_net_worth"]
    
    if "net_worth_diff" in df.columns:
        df["nw_sign_prediction"] = (df["net_worth_diff"] > 0).astype(int)
        df["abs_net_worth_diff"] = df["net_worth_diff"].abs()

    # 6. Final Export
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Dataset saved to {output_file}")

    # 7. Coverage Report
    with open(coverage_file, "w") as f:
        f.write("# Model Evaluation Dataset Coverage Report\n\n")
        f.write(f"- **Input rows**: {input_rows}\n")
        f.write(f"- **Output rows**: {len(df)}\n")
        f.write(f"- **Unique matches in logs**: {len(log_match_ids)}\n")
        f.write(f"- **Labeled matches**: {labeled_matches_count}\n")
        f.write(f"- **Dropped unlabeled matches**: {len(dropped_matches)}\n")
        if dropped_matches:
            f.write(f"  - {', '.join(dropped_matches)}\n")
        
        # Coverage calculations
        event_cov = (df["sig_event_type"].notnull().mean() * 100) if "sig_event_type" in df.columns else 0
        markout_cov = (df["lat_markout_30s"].notnull().mean() * 100) if "lat_markout_30s" in df.columns else 0
        
        f.write(f"- **Event coverage %**: {event_cov:.1f}%\n")
        f.write(f"- **Markout coverage %**: {markout_cov:.1f}%\n")
        
        f.write("\n### Phase Distribution\n")
        f.write(df["phase_bucket"].value_counts().to_string())
        
        f.write("\n\n### Label Distribution (radiant_win)\n")
        f.write(df["radiant_win"].value_counts().to_string())
        
        f.write("\n\n### Null-rate Table for Core Model Fields\n")
        core_fields = [
            "net_worth_diff", "radiant_score", "dire_score", 
            "sig_event_type", "sig_fair_price", "lat_markout_30s"
        ]
        available_core = [c for c in core_fields if c in df.columns]
        null_rates = df[available_core].isnull().mean() * 100
        f.write(null_rates.to_frame("null_rate_%").to_string())
        f.write("\n")

    print(f"Coverage report saved to {coverage_file}")

if __name__ == "__main__":
    main()
