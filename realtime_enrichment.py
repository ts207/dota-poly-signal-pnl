"""GetRealtimeStats delayed rich-context parser.

GetTopLiveGame is the fast execution source for duration, score, building
state, and aggregate radiant net-worth lead. GetRealtimeStats is delayed, but
it contains richer player/detail fields. This module attaches those delayed
details without overwriting fast fields used for event detection and current
win-probability anchoring.
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


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _to_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _team_side(team: dict, fallback_idx: int | None = None) -> str | None:
    """Resolve a GetRealtimeStats team object to radiant/dire.

    Prefer explicit API fields when present. Fall back to Valve's common team
    numbers, then array order for older/minimal payloads.
    """
    if not isinstance(team, dict):
        return None
    for key in ("side", "team_side", "faction"):
        value = team.get(key)
        if isinstance(value, str):
            lowered = value.lower()
            if "radiant" in lowered:
                return "radiant"
            if "dire" in lowered:
                return "dire"
    for key in ("is_radiant", "radiant"):
        value = team.get(key)
        if isinstance(value, bool):
            return "radiant" if value else "dire"
    for key in ("team_number", "team_slot", "side_id"):
        value = _to_int(team.get(key))
        if value in (0, 2):
            return "radiant"
        if value in (1, 3):
            return "dire"
    if fallback_idx == 0:
        return "radiant"
    if fallback_idx == 1:
        return "dire"
    return None


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
    radiant_level = 0
    dire_level = 0

    side_teams: dict[str, dict] = {}
    for idx, team in enumerate(teams):
        side = _team_side(team, idx)
        if side in ("radiant", "dire") and side not in side_teams:
            side_teams[side] = team
    if "radiant" not in side_teams or "dire" not in side_teams:
        return None

    for side_name in ("radiant", "dire"):
        players = side_teams[side_name].get("players") or []
        for p_idx, player in enumerate(players):
            nw = _to_int(player.get("net_worth")) or 0
            level = _to_int(player.get("level")) or 0
            out[f"{side_name}_p{p_idx+1}_net_worth"] = nw
            out[f"{side_name}_p{p_idx+1}_hero_id"] = _to_int(player.get("hero_id"))
            out[f"{side_name}_p{p_idx+1}_level"] = level
            if side_name == "radiant":
                radiant_nw += nw
                radiant_level += level
            else:
                dire_nw += nw
                dire_level += level
            
            # Death status
            respawn_timer = _to_int(_first_present(player.get("respawn_timer"), player.get("respawn_time"))) or 0
            out[f"{side_name}_p{p_idx+1}_respawn_timer"] = respawn_timer
            if respawn_timer > 0:
                if side_name == "radiant": radiant_dead += 1
                else: dire_dead += 1
                
                # Assume players 1-3 are cores for heuristic (simplified)
                if p_idx < 3:
                    if side_name == "radiant": radiant_cores_dead += 1
                    else: dire_cores_dead += 1
                
                if respawn_timer > max_respawn:
                    max_respawn = respawn_timer

    delayed_game_time = _to_int(_first_present(
        result.get("game_time"),
        result.get("duration"),
        (result.get("scoreboard") or {}).get("duration") if isinstance(result.get("scoreboard"), dict) else None,
    ))

    out.update({
        "realtime_game_time_sec": delayed_game_time,
        "delayed_game_time_sec": delayed_game_time,
        "realtime_radiant_nw": radiant_nw,
        "realtime_dire_nw": dire_nw,
        "realtime_lead_nw": radiant_nw - dire_nw,
        "delayed_radiant_net_worth": radiant_nw,
        "delayed_dire_net_worth": dire_nw,
        "delayed_net_worth_diff": radiant_nw - dire_nw,
        "radiant_level": radiant_level,
        "dire_level": dire_level,
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
                game["delayed_field_age_sec"] = game["realtime_stats_age_sec"]
            return game

    if session is None:
        return game

    data, fetch_time = await _fetch_realtime_stats(session, str(server_steam_id))
    _cache[str(server_steam_id)] = (data, fetch_time or now)

    if data is not None:
        parsed = parse_player_net_worth(data)
        if parsed:
            game.update(parsed)
            game["realtime_stats_age_sec"] = 0.0
            game["delayed_field_age_sec"] = game["realtime_stats_age_sec"]

    return game


def clear_cache() -> None:
    _cache.clear()
