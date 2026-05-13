import asyncio
import aiohttp
import steam_client
from steam_client import LeagueGameCache

async def main():
    async with aiohttp.ClientSession() as session:
        cache = LeagueGameCache()
        games = await steam_client.fetch_all_live_games(session, cache)
        found = False
        for g in games:
            r = str(g.get("radiant_team") or "")
            d = str(g.get("dire_team") or "")
            if "GLYPH" in r.upper() or "GLYPH" in d.upper() or "GRIND" in r.upper() or "GRIND" in d.upper():
                print(f"FOUND: ID={g.get('match_id')} | {r} vs {d} | Source={g.get('data_source')} | Time={g.get('game_time_sec')}s")
                found = True
        if not found:
            print("No GLYPH or Grind Back games found in currently live Steam data.")

if __name__ == "__main__":
    asyncio.run(main())
