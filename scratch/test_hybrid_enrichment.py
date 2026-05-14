import asyncio
import aiohttp
import time
import json
import os
from steam_client import fetch_all_live_games
from realtime_enrichment import maybe_enrich_realtime
from hybrid_nowcast import compute_hybrid_nowcast

# Use the API key from .env
from dotenv import load_dotenv
load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

async def test_enrichment():
    async with aiohttp.ClientSession() as session:
        print("1. Fetching all live games (TopLive + League)...")
        games = await fetch_all_live_games(session)
        if not games:
            print("No games found.")
            return

        # Find a match with a server_steam_id
        game = next((g for g in games if g.get("server_steam_id") and g.get("game_time_sec", 0) > 300), games[0])
        match_id = game.get("match_id")
        print(f"Target Match: {match_id}")

        print("\n2. Enriching with Realtime Stats...")
        # maybe_enrich_realtime modifies the game dict in-place
        await maybe_enrich_realtime(game, session)
        
        print(f"Enriched Fields:")
        for k in ["realtime_radiant_nw", "realtime_dire_nw", "realtime_lead_nw", "radiant_dead_count", "dire_dead_count", "max_respawn_timer"]:
            print(f"  {k}: {game.get(k)}")

        print("\n3. Testing Hybrid Nowcast...")
        # Mimic event detection
        fake_events = [] 
        # Calculate lag (assume 120s as discussed)
        lag = 120 
        
        nowcast = compute_hybrid_nowcast(
            latest_liveleague_features=game.get("liveleague_context"),
            latest_toplive_snapshot=game,
            toplive_event_cluster=fake_events,
            source_delay_metrics={"game_time_lag_sec": lag},
            slow_model_fair=0.5, # Assume 50/50 base
            game_time_sec=game.get("game_time_sec")
        )

        print(f"Nowcast Result:")
        print(f"  Hybrid Fair: {nowcast.hybrid_fair}")
        print(f"  Fast Adjustment: {nowcast.fast_event_adjustment} (should include drift)")
        print(f"  Uncertainty Penalty: {nowcast.uncertainty_penalty}")

        # Verify drift logic
        top_lead = game.get("radiant_lead")
        rt_lead = game.get("realtime_lead_nw")
        if top_lead is not None and rt_lead is not None:
            drift = top_lead - rt_lead
            print(f"\nDrift Analysis:")
            print(f"  TopLive Lead: {top_lead}")
            print(f"  Realtime Lead: {rt_lead}")
            print(f"  Drift: {drift}")
            expected_drift_adj = min(max(drift / 1000.0 * 0.01, -0.10), 0.10)
            print(f"  Expected Drift Adj: {expected_drift_adj:.4f}")
            # The fast_event_adjustment in nowcast should match this since we have no other events
            print(f"  Actual Nowcast Adj: {nowcast.fast_event_adjustment}")

if __name__ == "__main__":
    asyncio.run(test_enrichment())
