import os
import asyncio
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def check():
    url_league = "https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url_league, params={"key": STEAM_API_KEY}) as r:
            raw = await r.read()
            data = json.loads(raw.decode("utf-8", errors="replace"))
            games = data.get("result", {}).get("games", [])
            for g in games:
                if str(g.get("match_id")) == "8809888784":
                    print(f"Match: {g.get('match_id')}")
                    print(f"Radiant_name: {g.get('radiant_team', {}).get('team_name')}")
                    print(f"Radiant_wins: {g.get('radiant_series_wins')}")
                    print(f"Dire_name: {g.get('dire_team', {}).get('team_name')}")
                    print(f"Dire_wins: {g.get('dire_series_wins')}")

asyncio.run(check())
