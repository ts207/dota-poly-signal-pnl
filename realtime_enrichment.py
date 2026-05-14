"""GetRealtimeStats parser and enrichment cache — SHADOW-ONLY SCAFFOLD.

This module is intentionally NOT integrated into the live signal path (main.py).
It exists for shadow-log validation of per-player net worth data from
GetRealtimeStats before any trading use. Do NOT call maybe_enrich_realtime()
before confirming the parser handles real payloads correctly.

The REALTIME_STATS_ENABLED flag defaults to false. Enable it only after
shadow-logging real payloads and confirming player net worth extraction works.
When enabled, it attaches fields to the game dict (realtime_radiant_nw, etc.)
but these must NOT change expected_move, edge, sizing, or live entry decisions
until freshness is proven and the data is validated.
"""

import time
import logging
from typing import Any

from config import STEAM_API_KEY, REALTIME_STATS_ENABLED, REALTIME_STATS_STALE_SEC

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger(__name__)

REALTIME_STATS_URL = "https://api.steampowered.com/IDOTA2MatchStats_570/GetRealtimeStats/v1/"

_cache: dict[str, tuple[dict | None, float]] = {}


async def _fetch_realtime_stats(session: Any, server_steam_id: str) -> tuple[dict | None, float]:
    if aiohttp is None or not STEAM_API_KEY:
        return None, 0.0
    try:
        async with session.get(
            REALTIME_STATS_URL,
            params={"key": STEAM_API_KEY, "server_steam_id": server_steam_id},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            r.raise_for_status()
            raw = await r.read()
            import json
            data = json.loads(raw.decode("utf-8", errors="replace"))
            return data, time.time()
    except Exception as exc:
        logger.debug("GetRealtimeStats failed for %s: %s", server_steam_id, exc)
        return None, 0.0


def parse_player_net_worth(data: dict | None) -> dict[str, Any] | None:
    if not data:
        return None
    result = data.get("result") or data
    teams = result.get("teams") or []
    if len(teams) < 2:
        return None
    
    out = {}
    radiant_nw = 0
    dire_nw = 0
    radiant_dead = 0
    dire_dead = 0
    radiant_cores_dead = 0
    dire_cores_dead = 0
    max_respawn = 0

    # Teams: 0=Radiant, 1=Dire
    for side_idx, side_name in [(0, "radiant"), (1, "dire")]:
        players = teams[side_idx].get("players") or []
        for p_idx, player in enumerate(players):
            nw = player.get("net_worth") or 0
            out[f"{side_name}_p{p_idx+1}_net_worth"] = nw
            if side_name == "radiant": radiant_nw += nw
            else: dire_nw += nw
            
            # Death status
            respawn_timer = player.get("respawn_timer") or 0
            if respawn_timer > 0:
                if side_name == "radiant": radiant_dead += 1
                else: dire_dead += 1
                
                # Assume players 1-3 are cores for heuristic (simplified)
                if p_idx < 3:
                    if side_name == "radiant": radiant_cores_dead += 1
                    else: dire_cores_dead += 1
                
                if respawn_timer > max_respawn:
                    max_respawn = respawn_timer

    out.update({
        "realtime_radiant_nw": radiant_nw,
        "realtime_dire_nw": dire_nw,
        "realtime_lead_nw": radiant_nw - dire_nw,
        "radiant_net_worth": radiant_nw,
        "dire_net_worth": dire_nw,
        "radiant_dead_count": radiant_dead,
        "dire_dead_count": dire_dead,
        "radiant_core_dead_count": radiant_cores_dead,
        "dire_core_dead_count": dire_cores_dead,
        "max_respawn_timer": max_respawn,
    })
    return out


async def maybe_enrich_realtime(game: dict, session: Any = None) -> dict:
    if not REALTIME_STATS_ENABLED:
        return game
    server_steam_id = game.get("server_steam_id") or game.get("lobby_id")
    if not server_steam_id:
        return game

    now = time.time()
    cached = _cache.get(str(server_steam_id))
    if cached:
        cached_data, cached_time = cached
        if cached_data is not None and (now - cached_time) < REALTIME_STATS_STALE_SEC:
            parsed = parse_player_net_worth(cached_data)
            if parsed:
                game.update(parsed)
                game["realtime_stats_age_sec"] = round(now - cached_time, 2)
            return game

    if session is None:
        return game

    data, fetch_time = await _fetch_realtime_stats(session, str(server_steam_id))
    _cache[str(server_steam_id)] = (data, fetch_time or now)

    if data is not None:
        parsed = parse_player_net_worth(data)
        if parsed:
            game.update(parsed)
            game["realtime_stats_age_sec"] = round(now - (fetch_time or now), 2)

    return game


def clear_cache() -> None:
    _cache.clear()