from __future__ import annotations

import time
from typing import Any

AEGIS_ITEM_IDS = {117}
MAX_PLAYERS_PER_SIDE = 5


def first_present(*values):
    for value in values:
        if value is not None:
            return value
    return None


def extract_items(player: dict) -> list[int]:
    ids = []
    for key, value in player.items():
        if key.startswith("item") or key.startswith("backpack"):
            try:
                iv = int(value)
                if iv > 0:
                    ids.append(iv)
            except (TypeError, ValueError):
                pass
    return ids


def parse_players(players: list[dict]) -> list[dict]:
    out = []
    for p in players:
        if not isinstance(p, dict):
            continue
        items = extract_items(p)
        out.append({
            "account_id": first_present(p.get("account_id"), p.get("player_slot")),
            "player_name": first_present(p.get("name"), p.get("player_name")),
            "hero_id": p.get("hero_id"),
            "kills": p.get("kills"),
            "deaths": first_present(p.get("death"), p.get("deaths")),
            "assists": p.get("assists"),
            "last_hits": p.get("last_hits"),
            "denies": p.get("denies"),
            "gold": p.get("gold"),
            "level": p.get("level"),
            "gpm": first_present(p.get("gold_per_min"), p.get("gpm")),
            "xpm": first_present(p.get("xp_per_min"), p.get("xpm")),
            "net_worth": p.get("net_worth"),
            "item0": p.get("item0"),
            "item1": p.get("item1"),
            "item2": p.get("item2"),
            "item3": p.get("item3"),
            "item4": p.get("item4"),
            "item5": p.get("item5"),
            "backpack0": p.get("backpack0"),
            "backpack1": p.get("backpack1"),
            "backpack2": p.get("backpack2"),
            "neutral_item": first_present(p.get("item_neutral"), p.get("neutral_item")),
            "respawn_timer": first_present(p.get("respawn_timer"), p.get("respawn_time")),
            "items": items,
            "has_aegis": bool(set(items) & AEGIS_ITEM_IDS),
        })
    return out


def extract_liveleague_features(raw: dict, received_at_ns: int) -> dict:
    scoreboard = raw.get("scoreboard") or {}
    radiant = scoreboard.get("radiant") or {}
    dire = scoreboard.get("dire") or {}

    radiant_players_list = radiant.get("players") or []
    dire_players_list = dire.get("players") or []

    radiant_parsed = parse_players(radiant_players_list) if isinstance(radiant_players_list, list) else []
    dire_parsed = parse_players(dire_players_list) if isinstance(dire_players_list, list) else []

    aegis_team: str | None = None
    aegis_holder_hero_id: int | None = None
    for pp in radiant_parsed:
        if pp.get("has_aegis"):
            aegis_team = "radiant"
            aegis_holder_hero_id = pp.get("hero_id")
            break
    if aegis_team is None:
        for pp in dire_parsed:
            if pp.get("has_aegis"):
                aegis_team = "dire"
                aegis_holder_hero_id = pp.get("hero_id")
                break

    radiant_dead_count = sum(1 for p in radiant_parsed if (p.get("respawn_timer") or 0) > 0)
    dire_dead_count = sum(1 for p in dire_parsed if (p.get("respawn_timer") or 0) > 0)

    def _max_respawn(parsed_players: list[dict]) -> int:
        timers = [int(p.get("respawn_timer") or 0) for p in parsed_players if (p.get("respawn_timer") or 0) > 0]
        return max(timers) if timers else 0

    def _core_dead_count(parsed_players: list[dict]) -> int:
        return sum(1 for p in parsed_players if (p.get("respawn_timer") or 0) >= 50)

    def _top3_nw(parsed_players: list[dict]) -> int:
        nws = _top_n_values(parsed_players, "net_worth", 3)
        return sum(nws[:3]) if nws else 0

    series_raw = raw.get("series_id")
    series_type_raw = raw.get("series_type")
    try:
        series_id = int(series_raw) if series_raw is not None else None
    except (TypeError, ValueError):
        series_id = None
    try:
        series_type = int(series_type_raw) if series_type_raw is not None else None
    except (TypeError, ValueError):
        series_type = None

    radiant_meta = raw.get("radiant_team") or {}
    dire_meta = raw.get("dire_team") or {}

    game_time = None
    dur = scoreboard.get("duration")
    if dur is not None:
        try:
            game_time = int(dur)
            if game_time == 0:
                game_time = None
        except (TypeError, ValueError):
            game_time = None

    radiant_net_worth = _sum_player_field(radiant_parsed, "net_worth")
    dire_net_worth = _sum_player_field(dire_parsed, "net_worth")
    radiant_level = _sum_player_field(radiant_parsed, "level")
    dire_level = _sum_player_field(dire_parsed, "level")
    radiant_gpm = _sum_player_field(radiant_parsed, "gpm")
    dire_gpm = _sum_player_field(dire_parsed, "gpm")
    radiant_xpm = _sum_player_field(radiant_parsed, "xpm")
    dire_xpm = _sum_player_field(dire_parsed, "xpm")
    radiant_gold = _sum_player_field(radiant_parsed, "gold")
    dire_gold = _sum_player_field(dire_parsed, "gold")
    radiant_top = _top_n_values(radiant_parsed, "net_worth", 3)
    dire_top = _top_n_values(dire_parsed, "net_worth", 3)

    features = {
        "received_at_ns": received_at_ns,
        "match_id": str(raw.get("match_id") or raw.get("lobby_id") or ""),
        "lobby_id": str(raw.get("lobby_id") or ""),
        "league_id": str(raw.get("league_id") or ""),
        "series_id": series_id,
        "series_type": series_type,
        "game_time_sec": game_time,
        "stream_delay_s": int(raw.get("stream_delay_s") or 0),

        "radiant_team_id": radiant_meta.get("team_id") if isinstance(radiant_meta, dict) else None,
        "dire_team_id": dire_meta.get("team_id") if isinstance(dire_meta, dict) else None,
        "radiant_team": radiant_meta.get("team_name") if isinstance(radiant_meta, dict) else None,
        "dire_team": dire_meta.get("team_name") if isinstance(dire_meta, dict) else None,
        "radiant_team_name": radiant_meta.get("team_name") if isinstance(radiant_meta, dict) else None,
        "dire_team_name": dire_meta.get("team_name") if isinstance(dire_meta, dict) else None,

        "radiant_score": radiant.get("score"),
        "dire_score": dire.get("score"),
        "score_diff": _diff(radiant.get("score"), dire.get("score")),
        "radiant_tower_state": radiant.get("tower_state"),
        "dire_tower_state": dire.get("tower_state"),
        "radiant_barracks_state": radiant.get("barracks_state"),
        "dire_barracks_state": dire.get("barracks_state"),

        "radiant_net_worth": radiant_net_worth,
        "dire_net_worth": dire_net_worth,
        "net_worth_diff": _diff(radiant_net_worth, dire_net_worth),
        "top1_net_worth_diff": _diff(_nth(radiant_top, 0), _nth(dire_top, 0)),
        "top2_net_worth_diff": _diff(sum(radiant_top[:2]) if radiant_top else None, sum(dire_top[:2]) if dire_top else None),
        "top3_net_worth_diff": _diff(sum(radiant_top[:3]) if radiant_top else None, sum(dire_top[:3]) if dire_top else None),
        "level_diff": _diff(radiant_level, dire_level),
        "gpm_diff": _diff(radiant_gpm, dire_gpm),
        "xpm_diff": _diff(radiant_xpm, dire_xpm),
        "gold_diff": _diff(radiant_gold, dire_gold),

        "radiant_players": radiant_parsed,
        "dire_players": dire_parsed,

        "aegis_team": aegis_team,
        "aegis_holder_side": aegis_team,
        "aegis_holder_hero_id": aegis_holder_hero_id,
        "radiant_has_aegis": aegis_team == "radiant",
        "dire_has_aegis": aegis_team == "dire",
        "radiant_dead_count": radiant_dead_count,
        "dire_dead_count": dire_dead_count,
        "dead_core_count": radiant_dead_count + dire_dead_count,
        "radiant_max_respawn": _max_respawn(radiant_parsed),
        "dire_max_respawn": _max_respawn(dire_parsed),
        "max_respawn_timer": max(_max_respawn(radiant_parsed), _max_respawn(dire_parsed)),
        "radiant_core_dead_count": _core_dead_count(radiant_parsed),
        "dire_core_dead_count": _core_dead_count(dire_parsed),
        "radiant_top3_nw": _top3_nw(radiant_parsed),
        "dire_top3_nw": _top3_nw(dire_parsed),
    }

    features.update(_flatten_players("radiant", radiant_parsed))
    features.update(_flatten_players("dire", dire_parsed))
    return features


def _to_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum_player_field(players: list[dict], field: str) -> int | None:
    values = [_to_number(p.get(field)) for p in players]
    values = [v for v in values if v is not None]
    if not values:
        return None
    return int(sum(values))


def _diff(a: Any, b: Any) -> int | None:
    av = _to_number(a)
    bv = _to_number(b)
    if av is None or bv is None:
        return None
    return int(av - bv)


def _top_n_values(players: list[dict], field: str, n: int) -> list[int]:
    values = [_to_number(p.get(field)) for p in players]
    return [int(v) for v in sorted((v for v in values if v is not None), reverse=True)[:n]]


def _nth(values: list[int], index: int) -> int | None:
    return values[index] if len(values) > index else None


def _flatten_players(side: str, players: list[dict]) -> dict:
    row: dict[str, Any] = {}
    fields = [
        "account_id", "player_name", "hero_id", "kills", "deaths", "assists",
        "last_hits", "denies", "gold", "level", "gpm", "xpm", "net_worth",
        "item0", "item1", "item2", "item3", "item4", "item5",
        "backpack0", "backpack1", "backpack2", "neutral_item", "respawn_timer",
    ]
    for idx in range(MAX_PLAYERS_PER_SIDE):
        player = players[idx] if idx < len(players) else {}
        for field_name in fields:
            row[f"{side}_p{idx + 1}_{field_name}"] = player.get(field_name)
    return row


def classify_liveleague_lag(game_time_lag_sec: float | int | None) -> str:
    if game_time_lag_sec is None:
        return "unknown"
    lag = float(game_time_lag_sec)
    if lag <= 10:
        return "direct"
    if lag <= 60:
        return "prior"
    return "background"


def compute_derived_events(ctx: dict, game_time_sec: int | None = None) -> list[str]:
    events: list[str] = []

    aegis = ctx.get("aegis_team")
    if aegis == "radiant":
        events.append("AEGIS_HELD_BY_RADIANT")
    elif aegis == "dire":
        events.append("AEGIS_HELD_BY_DIRE")

    radiant_core_dead = ctx.get("radiant_core_dead_count") or 0
    dire_core_dead = ctx.get("dire_core_dead_count") or 0
    gt = game_time_sec if game_time_sec is not None else ctx.get("game_time_sec")

    if gt is not None and gt >= 2400:
        if radiant_core_dead >= 2:
            events.append("TWO_CORES_DEAD_50S_PLUS_RADIANT")
        if dire_core_dead >= 2:
            events.append("TWO_CORES_DEAD_50S_PLUS_DIRE")
        if radiant_core_dead >= 1:
            events.append("CORE_DEAD_60S_PLUS_RADIANT")
        if dire_core_dead >= 1:
            events.append("CORE_DEAD_60S_PLUS_DIRE")

    return events


class LiveLeagueContextCache:
    """Cache of LiveLeagueGames parsed features, keyed by match_id.

    Updated every LLG_REFRESH_SECONDS cycle. Context is attached to TopLive
    games as metadata only — it does NOT change expected_move, edge, or sizing.
    """

    def __init__(self):
        self.by_match_id: dict[str, dict] = {}

    def update(self, raw_games: list[dict], received_at_ns: int) -> None:
        for raw in raw_games:
            features = extract_liveleague_features(raw, received_at_ns)
            mid = features.get("match_id")
            if mid:
                self.by_match_id[mid] = features

    def get(self, match_id: str) -> dict | None:
        return self.by_match_id.get(str(match_id))

    def attach_to_game(self, game: dict, feature_logger=None) -> dict:
        """Attach cached LiveLeague context to a TopLive game dict as metadata.

        Does NOT modify expected_move, edge, sizing, or live entry decisions.
        Returns freshness metadata so downstream code can decide whether to use it.
        """
        mid = str(game.get("match_id") or "")
        ctx = self.get(mid)
        if ctx is None:
            game["liveleague_context_status"] = "missing"
            return game

        now_ns = time.time_ns()
        ctx_age_ms = (now_ns - ctx.get("received_at_ns", now_ns)) / 1_000_000
        ctx_gt = ctx.get("game_time_sec")
        game_gt = game.get("game_time_sec")

        game["liveleague_context"] = ctx
        game["liveleague_received_at_ns"] = ctx.get("received_at_ns")
        game["liveleague_age_ms"] = round(ctx_age_ms, 1)
        game["liveleague_game_time_sec"] = ctx_gt

        if ctx_gt is not None and game_gt is not None:
            game["game_time_lag_sec"] = game_gt - ctx_gt
        else:
            game["game_time_lag_sec"] = None

        ctx_fresh = (
            ctx_age_ms <= 3000
            and game.get("game_time_lag_sec") is not None
            and abs(game["game_time_lag_sec"]) <= 2
        )

        if ctx_fresh:
            game["liveleague_context_status"] = "fresh"
        else:
            game["liveleague_context_status"] = "stale"

        derived = compute_derived_events(ctx, game_gt)
        game["liveleague_derived_events"] = derived

        if feature_logger is not None:
            log_row = {
                "match_id": mid,
                "lobby_id": ctx.get("lobby_id"),
                "league_id": ctx.get("league_id"),
                "series_id": ctx.get("series_id"),
                "series_type": ctx.get("series_type"),
                "game_time_sec": ctx_gt,
                "radiant_team": ctx.get("radiant_team"),
                "dire_team": ctx.get("dire_team"),
                "radiant_score": ctx.get("radiant_score"),
                "dire_score": ctx.get("dire_score"),
                "radiant_tower_state": ctx.get("radiant_tower_state"),
                "dire_tower_state": ctx.get("dire_tower_state"),
                "radiant_barracks_state": ctx.get("radiant_barracks_state"),
                "dire_barracks_state": ctx.get("dire_barracks_state"),
                "radiant_net_worth": ctx.get("radiant_net_worth"),
                "dire_net_worth": ctx.get("dire_net_worth"),
                "radiant_dead_count": ctx.get("radiant_dead_count"),
                "dire_dead_count": ctx.get("dire_dead_count"),
                "radiant_max_respawn": ctx.get("radiant_max_respawn"),
                "dire_max_respawn": ctx.get("dire_max_respawn"),
                "radiant_core_dead_count": ctx.get("radiant_core_dead_count"),
                "dire_core_dead_count": ctx.get("dire_core_dead_count"),
                "radiant_top3_nw": ctx.get("radiant_top3_nw"),
                "dire_top3_nw": ctx.get("dire_top3_nw"),
                "aegis_team": ctx.get("aegis_team"),
                "aegis_holder_hero_id": ctx.get("aegis_holder_hero_id"),
                "liveleague_age_ms": game.get("liveleague_age_ms"),
                "game_time_lag_sec": game.get("game_time_lag_sec"),
                "liveleague_context_status": game.get("liveleague_context_status"),
            }
            feature_logger.log_features(log_row)

        return game

    def validate_mapping(self, game: dict, mapping: dict) -> list[str]:
        """Validate team/league identity between LiveLeague context and market mapping.

        Returns a list of mismatch descriptions. Empty list means no mismatches found.
        This is always safe to use — mismatches indicate a serious mapping error.
        """
        mid = str(game.get("match_id") or "")
        ctx = self.get(mid)
        if ctx is None:
            return []

        from team_utils import norm_team
        mismatches: list[str] = []

        mapped_yes = norm_team(mapping.get("yes_team") or "")
        mapped_radiant = norm_team(game.get("radiant_team") or "")
        mapped_dire = norm_team(game.get("dire_team") or "")

        ctx_radiant = norm_team(ctx.get("radiant_team") or "")
        ctx_dire = norm_team(ctx.get("dire_team") or "")

        if ctx_radiant and mapped_radiant and ctx_radiant != mapped_radiant:
            mismatches.append(f"radiant_team: toplive={mapped_radiant} llg={ctx_radiant}")
        if ctx_dire and mapped_dire and ctx_dire != mapped_dire:
            mismatches.append(f"dire_team: toplive={mapped_dire} llg={ctx_dire}")

        ctx_league = str(ctx.get("league_id") or "")
        mapped_league = str(game.get("league_id") or "")
        if ctx_league and mapped_league and ctx_league != mapped_league and ctx_league != "0" and mapped_league != "0":
            mismatches.append(f"league_id: toplive={mapped_league} llg={ctx_league}")

        ctx_series = ctx.get("series_id")
        ctx_series_type = ctx.get("series_type")
        if ctx_series is not None and ctx_series_type is not None:
            game_num = mapping.get("game_number")
            if game_num is not None:
                try:
                    gn = int(game_num)
                    if ctx_series_type in (0, 1) and gn > 3:
                        mismatches.append(f"series_type={ctx_series_type} but game_number={gn}")
                except (TypeError, ValueError):
                    pass

        return mismatches
