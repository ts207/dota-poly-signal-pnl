import os
import asyncio
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def check():
    url_top = "https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/"
    params = {"key": STEAM_API_KEY, "partner": 1}
    async with aiohttp.ClientSession() as session:
        async with session.get(url_top, params=params) as r:
            raw = await r.read()
            data = json.loads(raw.decode("utf-8", errors="replace"))
            games = data.get("game_list", [])
            for g in games:
                if str(g.get("match_id")) == "8809888784":
                    print(json.dumps(g, indent=2))
                    return

asyncio.run(check())
