import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv
from steam_client import fetch_all_live_games
from realtime_enrichment import maybe_enrich_realtime

load_dotenv()

async def find_and_track_league_match():
    async with aiohttp.ClientSession() as session:
        print("Searching for a live league match with Realtime Stats...")
        games = await fetch_all_live_games(session)
        
        # Filter for games with a server_steam_id (required for RealtimeStats)
        # And preferably ones that look like league games
        league_games = [g for g in games if g.get("server_steam_id") and g.get("lobby_id")]
        
        if not league_games:
            print("No suitable league games found right now.")
            return

        target = league_games[0]
        match_id = target.get("match_id")
        print(f"Tracking Match: {match_id}")
        
        for i in range(10): # Record 10 samples
            # Enrich
            await maybe_enrich_realtime(target, session)
            
            top_lead = target.get("radiant_lead")
            rt_lead = target.get("realtime_lead_nw")
            drift = (top_lead - rt_lead) if top_lead is not None and rt_lead is not None else None
            
            print(f"Sample {i+1}: GT={target.get('game_time_sec')} | TopLead={top_lead} | RTLead={rt_lead} | Drift={drift}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(find_and_track_league_match())
