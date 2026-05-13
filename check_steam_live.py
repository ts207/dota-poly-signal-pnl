import os
import asyncio
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def check_live():
    if not STEAM_API_KEY or STEAM_API_KEY == "replace_me":
        print("Missing STEAM_API_KEY in .env")
        return

    async with aiohttp.ClientSession() as session:
        # Check Top Live
        url_top = "https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/"
        params = {"key": STEAM_API_KEY, "partner": 0}
        async with session.get(url_top, params=params) as r:
            if r.status == 200:
                raw = await r.read()
                data = json.loads(raw.decode("utf-8", errors="replace"))
                games = data.get("game_list", [])
                print(f"--- Top Live Games ({len(games)}) ---")
                for g in games[:5]:
                    match_id = g.get("match_id")
                    team_radiant = g.get("team_name_radiant", "Unknown")
                    team_dire = g.get("team_name_dire", "Unknown")
                    print(f"[{match_id}] {team_radiant} vs {team_dire}")

        # Check Live League
        url_league = "https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/"
        async with session.get(url_league, params={"key": STEAM_API_KEY}) as r:
            if r.status == 200:
                raw = await r.read()
                data = json.loads(raw.decode("utf-8", errors="replace"))
                games = data.get("result", {}).get("games", [])
                print(f"\n--- Live League Games ({len(games)}) ---")
                for g in games:
                    match_id = g.get("match_id")
                    radiant = g.get("radiant_team", {}).get("team_name", "Unknown")
                    dire = g.get("dire_team", {}).get("team_name", "Unknown")
                    league_id = g.get("league_id")
                    print(f"[{match_id}] {radiant} vs {dire} (League: {league_id})")

if __name__ == "__main__":
    asyncio.run(check_live())
