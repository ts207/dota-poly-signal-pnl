import asyncio
import aiohttp
import time
import json

STEAM_API_KEY = "5715C5A721591E946229DDA658FD1AFD"
TOP_LIVE_URL = "https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/"
REALTIME_STATS_URL = "https://api.steampowered.com/IDOTA2MatchStats_570/GetRealtimeStats/v1/"

async def fetch_json(session, url, params):
    async with session.get(url, params=params) as r:
        if r.status != 200: return None
        return await r.json()

async def compare():
    async with aiohttp.ClientSession() as session:
        top_data = await fetch_json(session, TOP_LIVE_URL, {"key": STEAM_API_KEY, "partner": 0})
        if not top_data or "game_list" not in top_data: return

        games = [g for g in top_data["game_list"] if g.get("server_steam_id") and g.get("game_time", 0) > 300][:3]
        
        print(f"{'MATCH_ID':<12} | {'SOURCE':<8} | {'TIME':<5} | {'SCORE':<7} | {'LEAD':<6}")
        print("-" * 50)
        
        for target in games:
            match_id = target.get("match_id")
            server_id = target.get("server_steam_id")
            
            rt_data = await fetch_json(session, REALTIME_STATS_URL, {"key": STEAM_API_KEY, "server_steam_id": server_id})
            if not rt_data: continue
            
            res = rt_data.get("result") or rt_data
            match = res.get("match", {})
            teams = res.get("teams", [])
            
            radiant_nw = sum(p.get("net_worth", 0) for p in (teams[0].get("players", []) if len(teams) > 0 else []))
            dire_nw = sum(p.get("net_worth", 0) for p in (teams[1].get("players", []) if len(teams) > 1 else []))
            
            rt_r_score = teams[0].get("score", 0) if len(teams) > 0 else 0
            rt_d_score = teams[1].get("score", 0) if len(teams) > 1 else 0

            print(f"{match_id:<12} | {'TopLive':<8} | {target.get('game_time'):>5} | {target.get('radiant_score'):>2}-{target.get('dire_score'):<2} | {target.get('radiant_lead'):>6}")
            print(f"{'':<12} | {'RealTime':<8} | {match.get('game_time'):>5} | {rt_r_score:>2}-{rt_d_score:<2} | {radiant_nw - dire_nw:>6}")
            
            diff = match.get('game_time', 0) - target.get('game_time', 0)
            print(f"{'':<12} | DELTA: {diff:>4}s {'(RT lagging)' if diff < 0 else '(RT fresher)'}")
            print("-" * 50)

if __name__ == "__main__":
    asyncio.run(compare())
