#!/usr/bin/env python3
"""Build train-compatible historical features from OpenDota parsed pro matches.

This creates synthetic per-minute snapshots from replay-derived OpenDota fields.
It is not identical to Steam Live League snapshots, but it uses the same core
feature names consumed by dota_fair_model.features.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import aiohttp


OPENDOTA_BASE = "https://api.opendota.com/api"

FIELDNAMES = [
    "match_id",
    "league_id",
    "series_id",
    "series_type",
    "game_time_sec",
    "radiant_team_id",
    "dire_team_id",
    "radiant_team",
    "dire_team",
    "radiant_team_name",
    "dire_team_name",
    "radiant_score",
    "dire_score",
    "score_diff",
    "radiant_tower_state",
    "dire_tower_state",
    "radiant_barracks_state",
    "dire_barracks_state",
    "radiant_net_worth",
    "dire_net_worth",
    "net_worth_diff",
    "top1_net_worth_diff",
    "top2_net_worth_diff",
    "top3_net_worth_diff",
    "level_diff",
    "gpm_diff",
    "xpm_diff",
    "gold_diff",
    "radiant_dead_count",
    "dire_dead_count",
    "radiant_core_dead_count",
    "dire_core_dead_count",
    "max_respawn_timer",
    "radiant_has_aegis",
    "dire_has_aegis",
    "radiant_win",
]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Build historical OpenDota training rows.")
    parser.add_argument("--matches", type=int, default=50, help="Number of pro match stubs to inspect")
    parser.add_argument("--output", default="logs/opendota_historical_features.csv")
    parser.add_argument("--sample-sec", type=int, default=60)
    parser.add_argument("--tier1", action="store_true", help="Restrict to OpenDota premium leagues")
    parser.add_argument("--sleep-sec", type=float, default=1.2)
    parser.add_argument("--append", action="store_true")
    args = parser.parse_args()

    rows: list[dict[str, Any]] = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        tier1_ids = await fetch_tier1_league_ids(session) if args.tier1 else set()
        stubs = await collect_match_stubs(session, args.matches, tier1_ids, sleep_sec=args.sleep_sec)

        for idx, stub in enumerate(stubs, start=1):
            match_id = str(stub.get("match_id") or "")
            if not match_id:
                continue
            await asyncio.sleep(args.sleep_sec)
            match = await fetch_json(session, f"{OPENDOTA_BASE}/matches/{match_id}")
            if not isinstance(match, dict):
                print(f"[{idx}/{len(stubs)}] {match_id}: fetch_failed")
                continue
            match_rows = build_snapshots(match, sample_sec=args.sample_sec)
            if not match_rows:
                print(f"[{idx}/{len(stubs)}] {match_id}: no_timeline_data")
                continue
            rows.extend(match_rows)
            print(
                f"[{idx}/{len(stubs)}] {match_id}: rows={len(match_rows)} "
                f"radiant_win={int(bool(match.get('radiant_win')))}"
            )

    if not rows:
        print("No historical rows built.")
        return

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_header = not args.append or not output.exists() or output.stat().st_size == 0
    mode = "a" if args.append else "w"
    with output.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {output}")


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict[str, Any] | list[Any] | None:
    try:
        async with session.get(url) as response:
            if response.status == 429:
                retry_after = int(response.headers.get("retry-after") or 60)
                print(f"rate_limited sleep={retry_after}s")
                await asyncio.sleep(retry_after)
                return await fetch_json(session, url)
            if response.status != 200:
                print(f"fetch_status={response.status} url={url}")
                return None
            return await response.json()
    except Exception as exc:
        print(f"fetch_error={type(exc).__name__} url={url} detail={exc}")
        return None


async def fetch_tier1_league_ids(session: aiohttp.ClientSession) -> set[int]:
    data = await fetch_json(session, f"{OPENDOTA_BASE}/leagues")
    if not isinstance(data, list):
        return set()
    return {int(row["leagueid"]) for row in data if row.get("tier") == "premium" and row.get("leagueid")}


async def collect_match_stubs(
    session: aiohttp.ClientSession,
    wanted: int,
    tier1_ids: set[int],
    *,
    sleep_sec: float,
) -> list[dict[str, Any]]:
    stubs: list[dict[str, Any]] = []
    seen: set[str] = set()
    less_than: int | None = None

    while len(stubs) < wanted:
        url = f"{OPENDOTA_BASE}/proMatches"
        if less_than:
            url += f"?less_than_match_id={less_than}"
        page = await fetch_json(session, url)
        if not isinstance(page, list) or not page:
            break

        ids_on_page = []
        for stub in page:
            match_id = str(stub.get("match_id") or "")
            if not match_id or match_id in seen:
                continue
            seen.add(match_id)
            ids_on_page.append(int(match_id))
            if tier1_ids and int(stub.get("leagueid") or 0) not in tier1_ids:
                continue
            stubs.append(stub)
            if len(stubs) >= wanted:
                break

        if not ids_on_page:
            break
        less_than = min(ids_on_page)
        print(f"collected_stubs={len(stubs)} page_min_id={less_than}")
        if len(stubs) < wanted and sleep_sec > 0:
            await asyncio.sleep(sleep_sec)

    return stubs[:wanted]


def build_snapshots(match: dict[str, Any], *, sample_sec: int = 60) -> list[dict[str, Any]]:
    gold_adv = match.get("radiant_gold_adv") or []
    players = [p for p in (match.get("players") or []) if isinstance(p, dict)]
    if not gold_adv or not players or match.get("radiant_win") is None:
        return []

    radiant_players = [p for p in players if is_radiant_player(p)]
    dire_players = [p for p in players if not is_radiant_player(p)]
    if not radiant_players or not dire_players:
        return []

    max_minute = len(gold_adv) - 1
    step_minutes = max(1, int(sample_sec / 60))
    kill_counts = cumulative_kills_by_minute(players)

    rows = []
    for minute in range(0, max_minute + 1, step_minutes):
        game_time_sec = minute * 60
        radiant_nw = sum_at_minute(radiant_players, "gold_t", minute)
        dire_nw = sum_at_minute(dire_players, "gold_t", minute)
        radiant_gold = sum_at_minute(radiant_players, "gold_t", minute)
        dire_gold = sum_at_minute(dire_players, "gold_t", minute)
        radiant_lh = sum_at_minute(radiant_players, "lh_t", minute)
        dire_lh = sum_at_minute(dire_players, "lh_t", minute)
        radiant_xp = sum_at_minute(radiant_players, "xp_t", minute)
        dire_xp = sum_at_minute(dire_players, "xp_t", minute)
        radiant_top = top_values_at_minute(radiant_players, "gold_t", minute, 3)
        dire_top = top_values_at_minute(dire_players, "gold_t", minute, 3)
        radiant_score = kill_counts["radiant"][minute]
        dire_score = kill_counts["dire"][minute]

        rows.append(
            {
                "match_id": str(match.get("match_id") or ""),
                "league_id": match.get("leagueid"),
                "series_id": match.get("series_id"),
                "series_type": match.get("series_type"),
                "game_time_sec": game_time_sec,
                "radiant_team_id": match.get("radiant_team_id"),
                "dire_team_id": match.get("dire_team_id"),
                "radiant_team": team_name(match, "radiant"),
                "dire_team": team_name(match, "dire"),
                "radiant_team_name": team_name(match, "radiant"),
                "dire_team_name": team_name(match, "dire"),
                "radiant_score": radiant_score,
                "dire_score": dire_score,
                "score_diff": diff(radiant_score, dire_score),
                # OpenDota exposes final tower/barracks bitmasks here. Do not
                # stamp final structure state onto earlier snapshots.
                "radiant_tower_state": None,
                "dire_tower_state": None,
                "radiant_barracks_state": None,
                "dire_barracks_state": None,
                "radiant_net_worth": radiant_nw,
                "dire_net_worth": dire_nw,
                "net_worth_diff": value_or_adv(diff(radiant_nw, dire_nw), gold_adv, minute),
                "top1_net_worth_diff": diff(nth(radiant_top, 0), nth(dire_top, 0)),
                "top2_net_worth_diff": diff(sum(radiant_top[:2]), sum(dire_top[:2])),
                "top3_net_worth_diff": diff(sum(radiant_top[:3]), sum(dire_top[:3])),
                "level_diff": None,
                "gpm_diff": per_minute_diff(radiant_gold, dire_gold, minute),
                "xpm_diff": per_minute_diff(radiant_xp, dire_xp, minute),
                "gold_diff": diff(radiant_gold, dire_gold),
                "radiant_dead_count": None,
                "dire_dead_count": None,
                "radiant_core_dead_count": None,
                "dire_core_dead_count": None,
                "max_respawn_timer": None,
                "radiant_has_aegis": False,
                "dire_has_aegis": False,
                "radiant_win": int(bool(match.get("radiant_win"))),
            }
        )
    return rows


def is_radiant_player(player: dict[str, Any]) -> bool:
    if "isRadiant" in player:
        return bool(player.get("isRadiant"))
    slot = player.get("player_slot")
    try:
        return int(slot) < 128
    except (TypeError, ValueError):
        return False


def cumulative_kills_by_minute(players: list[dict[str, Any]]) -> dict[str, list[int]]:
    max_minute = 0
    kills: dict[str, dict[int, int]] = {"radiant": defaultdict(int), "dire": defaultdict(int)}
    for player in players:
        side = "radiant" if is_radiant_player(player) else "dire"
        for event in player.get("kills_log") or []:
            try:
                minute = max(0, int(event.get("time") or 0) // 60)
            except (TypeError, ValueError):
                continue
            kills[side][minute] += 1
            max_minute = max(max_minute, minute)

    out = {"radiant": [], "dire": []}
    r_total = d_total = 0
    for minute in range(max_minute + 500):
        r_total += kills["radiant"][minute]
        d_total += kills["dire"][minute]
        out["radiant"].append(r_total)
        out["dire"].append(d_total)
    return out


def sum_at_minute(players: list[dict[str, Any]], field: str, minute: int) -> int | None:
    values = [value_at_minute(player.get(field), minute) for player in players]
    values = [value for value in values if value is not None]
    return int(sum(values)) if values else None


def top_values_at_minute(players: list[dict[str, Any]], field: str, minute: int, n: int) -> list[int]:
    values = [value_at_minute(player.get(field), minute) for player in players]
    return sorted((int(value) for value in values if value is not None), reverse=True)[:n]


def value_at_minute(series: Any, minute: int) -> int | None:
    if not isinstance(series, list) or not series:
        return None
    index = min(minute, len(series) - 1)
    try:
        return int(series[index])
    except (TypeError, ValueError):
        return None


def diff(left: Any, right: Any) -> int | None:
    if left is None or right is None:
        return None
    return int(left) - int(right)


def per_minute_diff(left: int | None, right: int | None, minute: int) -> int | None:
    delta = diff(left, right)
    if delta is None:
        return None
    return int(delta / max(minute, 1))


def value_or_adv(value: int | None, gold_adv: list[Any], minute: int) -> int | None:
    if value is not None:
        return value
    if minute >= len(gold_adv):
        return None
    try:
        return int(gold_adv[minute])
    except (TypeError, ValueError):
        return None


def nth(values: list[int], index: int) -> int | None:
    return values[index] if len(values) > index else None


def team_name(match: dict[str, Any], side: str) -> str | None:
    direct = match.get(f"{side}_name")
    if direct:
        return str(direct)
    team = match.get(f"{side}_team")
    if isinstance(team, dict) and team.get("name"):
        return str(team["name"])
    return None


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    asyncio.run(main())
