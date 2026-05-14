import asyncio
import aiohttp
from steam_client import fetch_all_live_games
from realtime_enrichment import maybe_enrich_realtime

async def run():
    async with aiohttp.ClientSession() as s:
        games = await fetch_all_live_games(s)
        # Find any match with stats available
        targets = [g for g in games if g.get('server_steam_id') and (g.get('game_time_sec') or 0) > 300]
        if not targets:
            print("No matches with >5min game time found.")
            return
            
        target = targets[0]
        print(f"Target Match: {target.get('match_id')} | GT: {target.get('game_time_sec')}")
        await maybe_enrich_realtime(target, s)
        
        top_lead = target.get('radiant_lead')
        rt_lead = target.get('realtime_lead_nw')
        print(f"  TopLive Lead: {top_lead}")
        print(f"  Realtime Lead: {rt_lead}")
        if top_lead is not None and rt_lead is not None:
            print(f"  Current Drift: {top_lead - rt_lead}")

if __name__ == "__main__":
    asyncio.run(run())
