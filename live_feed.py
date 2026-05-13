import sys
import time
import yaml
from datetime import datetime

# Load market mappings
try:
    with open('markets.yaml', 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"Error loading markets.yaml: {e}")
    sys.exit(1)

# Create match_id -> team_names mapping
match_map = {}
for m in config.get('markets', []):
    mid = m.get('dota_match_id')
    if mid and mid != 'STEAM_MATCH_OR_LOBBY_ID_HERE':
        match_map[mid] = f"{m.get('yes_team')} vs {m.get('no_team')}"

def format_lead(val):
    try:
        n = int(val)
        color = "\033[92m" if n >= 0 else "\033[91m" # Green if Radiant leads, Red if Dire
        return f"{color}{n:+6}\033[0m"
    except:
        return f"{val:>6}"

print(f"{'TIME (UTC)':<12} | {'MATCH':<35} | {'LEAD':<6} | {'SCORE':<7} | {'GT':<5}")
print("-" * 75)

try:
    with open('logs/raw_snapshots.csv', 'r') as f:
        # Go to end of file
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue
            
            parts = line.strip().split(',')
            if len(parts) < 10 or parts[0] == 'received_at_utc':
                continue
            
            # parts[0]: received_at_utc (2026-05-13T07:58:07.338+00:00)
            # parts[2]: match_id
            # parts[5]: game_time_sec
            # parts[6]: radiant_lead
            # parts[7]: radiant_score
            # parts[8]: dire_score
            
            ts = parts[0][11:19] # Just HH:MM:SS
            mid = parts[2]
            gt = int(parts[5])
            gt_fmt = f"{gt//60}:{gt%60:02d}"
            lead = format_lead(parts[6])
            score = f"{parts[7]}-{parts[8]}"
            teams = match_map.get(mid, f"Match {mid}")
            
            print(f"{ts:<12} | {teams[:35]:<35} | {lead:<6} | {score:<7} | {gt_fmt:<5}")
except KeyboardInterrupt:
    print("\nStopped.")
