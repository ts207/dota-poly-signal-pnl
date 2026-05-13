from __future__ import annotations

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
    radiant_nw = 0
    radiant_players = 0
    dire_nw = 0
    dire_players = 0
    for player in (teams[0].get("players") or []):
        nw = player.get("net_worth")
        if isinstance(nw, (int, float)):
            radiant_nw += nw
            radiant_players += 1
    for player in (teams[1].get("players") or []):
        nw = player.get("net_worth")
        if isinstance(nw, (int, float)):
            dire_nw += nw
            dire_players += 1
    if radiant_players == 0 and dire_players == 0:
        return None
    return {
        "realtime_radiant_nw": radiant_nw,
        "realtime_dire_nw": dire_nw,
        "realtime_radiant_players": radiant_players,
        "realtime_dire_players": dire_players,
        "realtime_lead_nw": radiant_nw - dire_nw,
    }


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