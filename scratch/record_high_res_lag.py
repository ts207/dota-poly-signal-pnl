import asyncio
import aiohttp
import time
import os
import json
from dotenv import load_dotenv

# Load config
load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def record_data(duration_sec=300):
    async with aiohttp.ClientSession() as session:
        print(f"Recording high-res data for {duration_sec}s (1s polling)...")
        start_time = time.time()
        samples = []
        
        while time.time() - start_time < duration_sec:
            poll_start = time.time()
            
            # 1. Get Top Live Games
            url = f"https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/?key={STEAM_API_KEY}&partner=0"
            try:
                async with session.get(url) as resp:
                    data = await resp.json()
                    game = data.get("game_list", [{}])[0]
                    match_id = game.get("match_id")
                    gt = game.get("game_time_sec")
                    lead = game.get("radiant_lead")
            except Exception as e:
                print(f"Error fetching Steam: {e}")
                continue

            # 2. Get Realtime Stats (the 120s base)
            server_id = game.get("server_steam_id")
            rt_lead = None
            if server_id:
                rt_url = f"https://api.steampowered.com/IDOTA2Match_570/GetRealtimeStats/v1/?key={STEAM_API_KEY}&server_steam_id={server_id}"
                try:
                    async with session.get(rt_url) as resp:
                        rt_data = await resp.json()
                        teams = rt_data.get("teams", [])
                        if len(teams) >= 2:
                            r_nw = sum(p.get("net_worth", 0) for p in teams[0].get("players", []))
                            d_nw = sum(p.get("net_worth", 0) for p in teams[1].get("players", []))
                            rt_lead = r_nw - d_nw
                except Exception as e:
                    pass

            sample = {
                "ts": time.time(),
                "match_id": match_id,
                "game_time_sec": gt,
                "toplive_lead": lead,
                "realtime_lead": rt_lead,
                "drift": (lead - rt_lead) if lead is not None and rt_lead is not None else None
            }
            samples.append(sample)
            print(f"GT: {gt} | TopLead: {lead} | RTLead: {rt_lead} | Drift: {sample['drift']}")
            
            # Wait for next second
            elapsed = time.time() - poll_start
            await asyncio.sleep(max(0, 1.0 - elapsed))
            
        # Save to file
        with open("scratch/live_data_sample.json", "w") as f:
            json.dump(samples, f, indent=2)
        print(f"Saved {len(samples)} samples to scratch/live_data_sample.json")

if __name__ == "__main__":
    asyncio.run(record_data(60)) # Record for 1 minute for quick demo
