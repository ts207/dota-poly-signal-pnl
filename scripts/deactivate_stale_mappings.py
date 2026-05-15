from __future__ import annotations

import argparse
import os
import yaml
from datetime import datetime, timezone, timedelta
from pathlib import Path

MARKETS_YAML = os.path.join(os.path.dirname(__file__), "..", "markets.yaml")

def load_markets(path: str) -> dict:
    if not os.path.exists(path):
        return {"markets": []}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {"markets": []}

def write_markets(data: dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age-hours", type=float, default=12.0, help="Max age in hours for active mappings")
    parser.add_argument("--dry-run", action="store_true", help="Print affected markets without updating")
    args = parser.parse_args()

    data = load_markets(MARKETS_YAML)
    markets = data.get("markets", [])
    if not markets:
        print("No markets found.")
        return

    now = datetime.now(timezone.utc)
    max_age = timedelta(hours=args.max_age_hours)
    
    deactivated_count = 0
    
    for m in markets:
        confidence = float(m.get("confidence") or 0.0)
        if confidence < 1.0:
            continue
            
        auto_mapped_at = m.get("auto_mapped_at_utc")
        scheduled_start = m.get("scheduled_start_utc")
        
        should_deactivate = False
        reason = ""
        
        if auto_mapped_at:
            try:
                dt = datetime.fromisoformat(auto_mapped_at.replace("Z", "+00:00"))
                if now - dt > max_age:
                    should_deactivate = True
                    reason = f"auto_mapped_at_utc={auto_mapped_at} older than {args.max_age_hours}h"
            except (TypeError, ValueError):
                pass
                
        if not should_deactivate and scheduled_start:
            try:
                # scheduled_start_utc might be iso format
                dt = datetime.fromisoformat(scheduled_start.replace("Z", "+00:00"))
                if now - dt > max_age:
                    should_deactivate = True
                    reason = f"scheduled_start_utc={scheduled_start} older than {args.max_age_hours}h"
            except (TypeError, ValueError):
                pass
        
        if should_deactivate:
            print(f"Deactivating: {m.get('name')} (ID: {m.get('dota_match_id')}) - {reason}")
            if not args.dry_run:
                m["confidence"] = 0.0
                # Preserve the match ID but set confidence to 0 so it's not "active"
                # OR reset dota_match_id to placeholder? 
                # The user said "sets old active mappings back to inactive".
                # In sync_markets.py, is_active_mapping checks confidence >= 0.98 and not placeholder.
                # So setting confidence to 0.0 is enough.
            deactivated_count += 1

    if deactivated_count > 0:
        if args.dry_run:
            print(f"Dry run: {deactivated_count} markets would be deactivated.")
        else:
            write_markets(data, MARKETS_YAML)
            print(f"Deactivated {deactivated_count} markets.")
    else:
        print("No stale mappings found.")

if __name__ == "__main__":
    main()
