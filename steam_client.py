from __future__ import annotations

import asyncio
import json
import time
import aiohttp

from config import STEAM_API_KEY, LLG_REFRESH_SECONDS

TOP_LIVE_URL = "https://api.steampowered.com/IDOTA2Match_570/GetTopLiveGame/v1/"
LIVE_LEAGUE_URL = "https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/"
REALTIME_STATS_URL = "https://api.steampowered.com/IDOTA2MatchStats_570/GetRealtimeStats/v1/"
SIDE_TOWER_ALIVE_MASK = 0x7FF


async def _get_json(session: aiohttp.ClientSession, url: str, params: dict, timeout: float = 5) -> dict:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        r.raise_for_status()
        raw = await r.read()
        return json.loads(raw.decode("utf-8", errors="replace"))


def _source_update_age_sec(last_update_time, received_at_ns: int) -> float | None:
    """Best-effort age of the Dota source update at receive time.

    Some Steam payloads include last_update_time as a Unix timestamp in seconds.
    If it is absent or implausible, return None rather than guessing.
    """
    if last_update_time in (None, ""):
        return None
    try:
        ts = float(last_update_time)
    except (TypeError, ValueError):
        return None
    # Treat only plausible Unix-second timestamps as source update times.
    if ts < 1_000_000_000 or ts > 4_000_000_000:
        return None
    received_s = received_at_ns / 1_000_000_000
    return max(0.0, received_s - ts)


def decode_top_live_tower_state(building_state) -> int | None:
    """Convert GetTopLiveGame building_state into the standard tower alive mask.

    TopLive does not expose the normal per-side tower_state alive bitmask. For
    lane towers it appears to expose per-lane progress bits:
      side A: 0..2 top, 3..5 mid, 6..8 bot
      side B: 16..18 top, 19..21 mid, 22..24 bot

    The highest set bit in each 3-bit lane group is the deepest exposed tier:
      0 => T1 alive, 1 => T1 down/T2 alive, 2 => T1+T2 down/T3 alive.

    Bits beyond lane towers are not treated as T4s here. Those bits can turn on
    around base/rax states and caused false early T3/T4 signals when interpreted
    as the standard mask. We keep T4 bits alive until separately validated.
    """
    try:
        raw = int(building_state)
    except (TypeError, ValueError):
        return None

    low_side = _decode_top_live_side_towers(raw & 0x1FF)
    high_side = _decode_top_live_side_towers((raw >> 16) & 0x1FF)
    return low_side | (high_side << 11)


def _decode_top_live_side_towers(progress_bits: int) -> int:
    alive = (1 << 9) | (1 << 10)  # T4 state not decoded from TopLive.
    for lane_base in (0, 3, 6):
        group = (progress_bits >> lane_base) & 0b111
        if group == 0:
            destroyed_count = 3
        else:
            destroyed_count = max(i for i in range(3) if group & (1 << i))
        for tier in range(destroyed_count, 3):
            alive |= 1 << (lane_base + tier)
    return alive & SIDE_TOWER_ALIVE_MASK


def normalize_top_live(g: dict, received_at_ns: int) -> dict:
    """Normalize a GetTopLiveGame entry into the standard game dict.

    GetTopLiveGame provides radiant_lead, scores, building_state, game_time,
    server_steam_id, and deactivate_time directly — no secondary API call needed
    for basic signal data.
    """
    last_update_time = g.get("last_update_time")
    source_update_age = _source_update_age_sec(last_update_time, received_at_ns)

    return {
        "match_id": str(g.get("match_id") or g.get("lobby_id") or ""),
        "lobby_id": str(g.get("lobby_id") or ""),
        "league_id": str(g.get("league_id") or ""),
        "radiant_team": g.get("team_name_radiant") or None,
        "dire_team": g.get("team_name_dire") or None,
        "radiant_team_id": str(g.get("team_id_radiant") or ""),
        "dire_team_id": str(g.get("team_id_dire") or ""),
        "game_time_sec": int(g.get("game_time") or 0) or None,
        "radiant_lead": int(g.get("radiant_lead") or 0),
        "radiant_score": g.get("radiant_score"),
        "dire_score": g.get("dire_score"),
        "radiant_net_worth": None,
        "dire_net_worth": None,
        "building_state": g.get("building_state"),
        "tower_state": decode_top_live_tower_state(g.get("building_state")),
        "radiant_barracks_state": None,
        "dire_barracks_state": None,
        "server_steam_id": str(g.get("server_steam_id") or ""),
        "stream_delay_s": int(g.get("delay") or 0),
        "game_over": int(g.get("deactivate_time") or 0) > 0,
        "deactivate_time": int(g.get("deactivate_time") or 0),
        "activate_time": int(g.get("activate_time") or 0),
        "last_update_time": last_update_time,
        "source_update_age_sec": source_update_age,
        "spectators": g.get("spectators"),
        "received_at_ns": received_at_ns,
        "data_source": "top_live",
        "raw": g,
    }


async def fetch_top_live_games(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch all live games from GetTopLiveGame across all partner buckets.

    partner=0: general ranked/unranked high-MMR games
    partner=1: league/tournament games
    partner=2: same pool as 0 with different sort

    Returns deduplicated list normalized to standard game dicts.
    """
    received_at_ns = time.time_ns()
    results: dict[str, dict] = {}

    async def fetch_partner(partner: int):
        try:
            data = await _get_json(session, TOP_LIVE_URL, {"key": STEAM_API_KEY, "partner": partner})
            for g in data.get("game_list", []):
                mid = str(g.get("match_id") or "")
                if mid and mid not in results:
                    results[mid] = normalize_top_live(g, received_at_ns)
        except Exception as e:
            print(f"fetch_top_live_games partner={partner} error: {e}")

    await asyncio.gather(fetch_partner(0), fetch_partner(1), fetch_partner(2))
    return list(results.values())


async def fetch_live_league_games(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch GetLiveLeagueGames for team-name enrichment of league games.

    This API provides team names and confirmed league_id for registered teams,
    which GetTopLiveGame omits for some games. Use to enrich top_live entries
    that have empty team names.
    """
    sent_at = time.time_ns()
    try:
        data = await _get_json(session, LIVE_LEAGUE_URL, {"key": STEAM_API_KEY})
    except Exception:
        return []
    received_at = time.time_ns()

    games = data.get("result", {}).get("games", [])
    for g in games:
        g["_sent_at_ns"] = sent_at
        g["_received_at_ns"] = received_at
    return games


async def fetch_realtime_stats(session: aiohttp.ClientSession, server_steam_id: str) -> tuple[dict | None, int]:
    """Call GetRealtimeStats for per-player net worth data.

    Only needed when you want player-level NW breakdown beyond what
    GetTopLiveGame's aggregate radiant_lead provides.
    Returns (data, received_at_ns) or (None, 0) on failure.
    """
    try:
        data = await _get_json(session, REALTIME_STATS_URL, {"key": STEAM_API_KEY, "server_steam_id": server_steam_id})
        return data, time.time_ns()
    except Exception:
        return None, 0


def normalize_league_game(raw: dict) -> dict:
    """Normalize a GetLiveLeagueGames entry to the standard game dict."""
    radiant_meta = raw.get("radiant_team") or {}
    dire_meta = raw.get("dire_team") or {}
    scoreboard = raw.get("scoreboard") or {}
    radiant_sb = scoreboard.get("radiant") or {}
    dire_sb = scoreboard.get("dire") or {}

    def _sum_nw(players):
        return sum(int(p.get("net_worth") or 0) for p in (players or []) if isinstance(p, dict))

    radiant_nw = _sum_nw(radiant_sb.get("players"))
    dire_nw = _sum_nw(dire_sb.get("players"))

    stream_delay_s = int(raw.get("stream_delay_s") or 0)
    # stream_delay_s is spectator/broadcast delay metadata, not API freshness.
    # Do not subtract it from received_at_ns. Signal freshness should be based
    # on the actual receive timestamp plus source/book freshness checks.
    received_at_ns = raw.get("_received_at_ns", time.time_ns())
    return {
        "match_id": str(raw.get("match_id") or raw.get("lobby_id") or ""),
        "lobby_id": str(raw.get("lobby_id") or ""),
        "league_id": str(raw.get("league_id") or ""),
        "radiant_team": radiant_meta.get("team_name"),
        "dire_team": dire_meta.get("team_name"),
        "radiant_team_id": str(radiant_meta.get("team_id") or ""),
        "dire_team_id": str(dire_meta.get("team_id") or ""),
        "game_time_sec": int(scoreboard.get("duration") or 0) or None,
        "radiant_lead": radiant_nw - dire_nw,
        "radiant_score": radiant_sb.get("score"),
        "dire_score": dire_sb.get("score"),
        "radiant_net_worth": radiant_nw,
        "dire_net_worth": dire_nw,
        "building_state": None,
        "tower_state": radiant_sb.get("tower_state"),
        "radiant_barracks_state": radiant_sb.get("barracks_state"),
        "dire_barracks_state": dire_sb.get("barracks_state"),
        "server_steam_id": "",
        "stream_delay_s": stream_delay_s,
        # GetLiveLeagueGames has no deactivate_time equivalent; game_over detection
        # requires GetTopLiveGame. If a game is only visible via this source,
        # positions will not exit via game_over — they will fall through to max_hold_timeout.
        "game_over": False,
        "deactivate_time": 0,
        "activate_time": 0,
        "last_update_time": None,
        "source_update_age_sec": None,
        "spectators": raw.get("spectators"),
        "received_at_ns": received_at_ns,
        "data_source": "live_league",
        "raw": raw,
    }




class LeagueGameCache:
    """Slow cache for GetLiveLeagueGames enrichment.

    GetTopLiveGame is the fast signal source. GetLiveLeagueGames is useful for
    team-name metadata but should not block every high-frequency Steam poll.
    """

    def __init__(self, refresh_seconds: float = LLG_REFRESH_SECONDS):
        self.refresh_seconds = float(refresh_seconds)
        self._last_refresh_monotonic = 0.0
        self._games_raw: list[dict] = []

    async def get(self, session: aiohttp.ClientSession, *, force: bool = False) -> list[dict]:
        now = time.monotonic()
        if force or not self._games_raw or now - self._last_refresh_monotonic >= self.refresh_seconds:
            self._games_raw = await fetch_live_league_games(session)
            self._last_refresh_monotonic = now
        return list(self._games_raw)

async def fetch_all_live_games(
    session: aiohttp.ClientSession,
    league_cache: LeagueGameCache | None = None,
    *,
    include_league: bool = True,
) -> list[dict]:
    """Primary entry point: fetch all live games with full signal data.

    Merges GetTopLiveGame (real-time radiant_lead, building_state, game_over,
    server_steam_id) with GetLiveLeagueGames (league games not in GetTopLiveGame,
    richer team metadata). GetTopLiveGame entries take precedence when both
    sources have the same match_id.
    """
    top_games = await fetch_top_live_games(session)
    if include_league:
        if league_cache is not None:
            lg_raw = await league_cache.get(session)
        else:
            lg_raw = await fetch_live_league_games(session)
    else:
        lg_raw = []

    merged: dict[str, dict] = {}

    # League games as baseline (lower priority)
    for raw in lg_raw:
        mid = str(raw.get("match_id") or "")
        if mid:
            merged[mid] = normalize_league_game(raw)

    # Top live games override (higher priority — better real-time data)
    for game in top_games:
        mid = game["match_id"]
        existing = merged.get(mid)
        if existing:
            # Prefer top_live fields but keep team metadata from league if missing
            if not game["radiant_team"]:
                game["radiant_team"] = existing["radiant_team"]
                game["radiant_team_id"] = existing["radiant_team_id"]
            if not game["dire_team"]:
                game["dire_team"] = existing["dire_team"]
                game["dire_team_id"] = existing["dire_team_id"]
            if not game["league_id"] or game["league_id"] == "0":
                game["league_id"] = existing["league_id"]
        merged[mid] = game

    return list(merged.values())
