#!/usr/bin/env python3
import os
import yaml
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKETS_YAML = PROJECT_ROOT / "markets.yaml"

def deactivate_stale_mappings(hours=12):
    if not MARKETS_YAML.exists():
        print(f"Error: {MARKETS_YAML} not found.")
        return

    with open(MARKETS_YAML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {"markets": []}
    
    markets = data.get("markets", [])
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    
    changed = False
    deactivated_count = 0
    
    for mapping in markets:
        # We only care about active mappings (confidence 1.0)
        try:
            confidence = float(mapping.get("confidence", 0) or 0)
        except (TypeError, ValueError):
            confidence = 0.0
            
        if confidence < 1.0:
            continue
            
        # Check auto_mapped_at_utc or scheduled_start_utc
        mapped_at_str = mapping.get("auto_mapped_at_utc")
        if not mapped_at_str:
            # Fallback to scheduled_start_utc if auto_mapped_at_utc is missing
            mapped_at_str = mapping.get("scheduled_start_utc")
            
        if not mapped_at_str:
            continue
            
        try:
            # Try parsing ISO format (e.g. 2026-05-13T09:55:40+00:00)
            # Some strings might have spaces instead of T, or different offsets
            clean_str = mapped_at_str.replace(" ", "T")
            if clean_str.endswith("+00"): clean_str += ":00" # Handle +00 instead of +00:00
            
            # Simple ISO parse
            mapped_at = datetime.fromisoformat(clean_str)
            if mapped_at.tzinfo is None:
                mapped_at = mapped_at.replace(tzinfo=timezone.utc)
        except Exception as e:
            # print(f"Warning: Could not parse timestamp '{mapped_at_str}': {e}")
            continue
            
        if mapped_at < cutoff:
            print(f"Deactivating stale mapping: {mapping.get('name')} (mapped at {mapped_at_str})")
            mapping["confidence"] = 0.0
            mapping["deactivated_at_utc"] = now.isoformat(timespec="seconds")
            changed = True
            deactivated_count += 1
            
    if changed:
        with open(MARKETS_YAML, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        print(f"Done. Deactivated {deactivated_count} stale mapping(s).")
    else:
        print("No stale mappings found.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Deactivate stale market mappings (>12h old)")
    parser.add_argument("--hours", type=int, default=12, help="Age in hours to consider stale (default: 12)")
    args = parser.parse_args()
    
    deactivate_stale_mappings(hours=args.hours)
