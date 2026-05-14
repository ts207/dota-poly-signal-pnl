import os
import asyncio
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def check():
    url_top = "https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/"
    async with aiohttp.ClientSession() as session:
        for p in [0, 1, 2]:
            params = {"key": STEAM_API_KEY, "partner": p}
            async with session.get(url_top, params=params) as r:
                data = await r.json()
                games = data.get("game_list", [])
                print(f"Partner {p}: {len(games)} games")
                for g in games:
                    if str(g.get("match_id")) == "8809888784":
                        print(f"FOUND IN PARTNER {p}!")
                        return

asyncio.run(check())
