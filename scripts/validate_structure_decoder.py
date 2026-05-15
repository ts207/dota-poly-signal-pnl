import csv
import os
import sys
from datetime import datetime
from collections import defaultdict

# Add parent dir to path to import structure_state
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from structure_state import decode_structure_state, diff_structure_state

def validate_logs():
    raw_path = "logs/raw_snapshots.csv"
    report_path = "structure_decoder_report.md"
    
    if not os.path.exists(raw_path):
        print(f"Missing {raw_path}")
        return
        
    invalid_deltas = []
    suppressed_building_only = 0
    total_snapshots = 0
    
    match_histories = defaultdict(list)
    
    with open(raw_path, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_snapshots += 1
            match_id = row.get("match_id", "unknown")
            match_histories[match_id].append(row)
            
    # Process snapshots per match
    for match_id, history in match_histories.items():
        # Sort by game_time_sec
        history.sort(key=lambda x: int(x.get("game_time_sec") or 0))
        prev_state = None
        
        for snapshot in history:
            cur_state = decode_structure_state(snapshot)
            
            if prev_state:
                delta = diff_structure_state(prev_state, cur_state)
                if not delta.is_valid:
                    invalid_deltas.append({
                        "match_id": match_id,
                        "game_time": snapshot.get("game_time_sec"),
                        "reason": delta.validation_reason,
                        "prev_tower": prev_state.radiant.raw_bits if prev_state.source_field == "tower_state" else "N/A",
                        "cur_tower": cur_state.radiant.raw_bits if cur_state.source_field == "tower_state" else "N/A"
                    })
            
            if cur_state.schema == "building_unknown":
                suppressed_building_only += 1
                
            prev_state = cur_state

    # Report generation
    with open(report_path, "w") as f:
        f.write("# Structure Decoder Validation Report\n\n")
        f.write(f"Generated at: {datetime.utcnow().isoformat()}Z\n\n")
        
        f.write("## Summary\n")
        f.write(f"- Total Raw Snapshots: {total_snapshots}\n")
        f.write(f"- Invalid Deltas Detected: {len(invalid_deltas)}\n")
        f.write(f"- Building-state-only Snapshots (Suppressed): {suppressed_building_only}\n\n")
        
        if invalid_deltas:
            f.write("## Invalid Deltas\n")
            f.write("| Match ID | Game Time | Reason | Prev Bits | Cur Bits |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            for d in invalid_deltas[:20]: # Cap at 20
                f.write(f"| {d['match_id']} | {d['game_time']} | {d['reason']} | {d['prev_tower']} | {d['cur_tower']} |\n")
            if len(invalid_deltas) > 20:
                f.write(f"\n... and {len(invalid_deltas) - 20} more.\n")
        else:
            f.write("## Invalid Deltas\nNo invalid deltas detected in logs.\n")
            
    print(f"Report generated: {report_path}")

if __name__ == "__main__":
    validate_logs()
