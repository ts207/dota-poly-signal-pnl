import sys

with open("dota-poly-signal-pnl/dashboard.py", "r") as f:
    code = f.read()

# Add a function to get data_source
if "def _latest_data_sources" not in code:
    code = code.replace("def _live_games() -> list[dict]:", """def _latest_data_sources() -> dict[str, str]:
    from config import CSV_LOG_PATH
    import os
    raw_path = os.path.join(os.path.dirname(CSV_LOG_PATH), "raw_snapshots.csv")
    rows = _read_csv(raw_path)
    sources = {}
    for r in rows:
        mid = r.get("match_id") or r.get("lobby_id")
        src = r.get("data_source")
        if mid and src:
            sources[mid] = src
    return sources

def _live_games() -> list[dict]:""")

    old_games_append = """        games.append({
            "match_id": mid,
            "radiant_team": r.get("radiant_team", "Radiant"),
            "dire_team": r.get("dire_team", "Dire"),"""
    new_games_append = """        sources = _latest_data_sources()
        games.append({
            "match_id": mid,
            "data_source": sources.get(mid, "live_league"),
            "radiant_team": r.get("radiant_team", "Radiant"),
            "dire_team": r.get("dire_team", "Dire"),"""
    
    code = code.replace(old_games_append, new_games_append)

    # HTML headers
    code = code.replace("<th>Radiant</th><th>Dire</th><th>Game Time</th>", "<th>Source</th><th>Radiant</th><th>Dire</th><th>Game Time</th>")
    
    # JS body
    code = code.replace("<td>${g.radiant_team}</td>", "<td>${g.data_source || 'live_league'}</td>\n        <td>${g.radiant_team}</td>")

with open("dota-poly-signal-pnl/dashboard.py", "w") as f:
    f.write(code)
