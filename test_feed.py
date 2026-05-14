import asyncio
import aiohttp
from steam_client import fetch_all_live_games

async def main():
    async with aiohttp.ClientSession() as session:
        games = await fetch_all_live_games(session)
        for g in games:
            if g.get("match_id") == "8809888784":
                print(f"FOUND 8809888784: data_source={g.get('data_source')} game_time={g.get('game_time_sec')}")
                return
        print("NOT FOUND")

asyncio.run(main())
