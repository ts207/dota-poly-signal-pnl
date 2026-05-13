from __future__ import annotations

import argparse
import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from team_utils import norm_team, teams_match

import aiohttp
import yaml

from discover_markets import MARKETS_YAML, main as discover_main
from steam_client import fetch_all_live_games


PLACEHOLDER_MATCH_ID = "STEAM_MATCH_OR_LOBBY_ID_HERE"



def match_direction(mapping: dict, game: dict) -> str | None:
    yes = mapping.get("yes_team")
    no = mapping.get("no_team")
    radiant = game.get("radiant_team")
    dire = game.get("dire_team")
    if teams_match(yes, radiant) and teams_match(no, dire):
        return "normal"
    if teams_match(yes, dire) and teams_match(no, radiant):
        return "reversed"
    return None


def game_number(mapping: dict) -> int:
    text = " ".join(str(mapping.get(k) or "") for k in ("name", "slug"))
    match = re.search(r"\bGame\s*(\d+)\b", text, flags=re.I)
    return int(match.group(1)) if match else 999


def is_placeholder_match_id(value: Any) -> bool:
    text = str(value or "")
    return not text or PLACEHOLDER_MATCH_ID in text


def is_active_mapping(mapping: dict) -> bool:
    try:
        confidence = float(mapping.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    return confidence >= 0.98 and not is_placeholder_match_id(mapping.get("dota_match_id"))


def live_match_id(game: dict) -> str:
    return str(game.get("match_id") or game.get("lobby_id") or "")


def choose_mapping_for_live_game(markets: list[dict], game: dict) -> tuple[dict | None, str]:
    matching = [m for m in markets if match_direction(m, game)]
    if not matching:
        return None, "no_team_match"

    existing_same_match = [
        m for m in matching
        if str(m.get("dota_match_id") or "") == live_match_id(game) and is_active_mapping(m)
    ]
    if existing_same_match:
        return None, "already_mapped_current_match"

    candidates = [
        m for m in matching
        if is_placeholder_match_id(m.get("dota_match_id")) or not is_active_mapping(m)
    ]
    if not candidates:
        return None, "no_inactive_candidate"

    # Activate only one map for a live Steam match. Re-running after the next
    # Steam match_id appears advances to the next Game N market.
    candidates.sort(key=game_number)
    return candidates[0], "matched"


def sync_markets_to_games(
    markets: list[dict],
    games: list[dict],
    *,
    only_pair: tuple[str, str] | None = None,
) -> list[dict]:
    updates: list[dict] = []
    used_market_ids: set[int] = set()

    named_games = [
        g for g in games
        if live_match_id(g) and (g.get("radiant_team") or g.get("dire_team"))
    ]

    for game in named_games:
        if only_pair:
            game_pair = sorted([norm_team(game.get("radiant_team")), norm_team(game.get("dire_team"))])
            target_pair = sorted([norm_team(only_pair[0]), norm_team(only_pair[1])])
            if game_pair != target_pair:
                continue
        market, reason = choose_mapping_for_live_game(markets, game)
        if not market or id(market) in used_market_ids:
            continue

        direction = match_direction(market, game)
        market["dota_match_id"] = live_match_id(game)
        market["confidence"] = 1.0
        market["auto_mapped_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        market["auto_mapped_source"] = game.get("data_source")
        market["steam_radiant_team"] = game.get("radiant_team")
        market["steam_dire_team"] = game.get("dire_team")
        market["steam_side_mapping"] = direction

        used_market_ids.add(id(market))
        updates.append({
            "market_name": market.get("name"),
            "dota_match_id": market.get("dota_match_id"),
            "radiant_team": game.get("radiant_team"),
            "dire_team": game.get("dire_team"),
            "game_time_sec": game.get("game_time_sec"),
            "direction": direction,
        })

    return updates


def load_markets(path: str | Path = MARKETS_YAML) -> dict:
    p = Path(path)
    if not p.exists():
        return {"markets": []}
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {"markets": []}


def write_markets(data: dict, path: str | Path = MARKETS_YAML) -> None:
    p = Path(path)
    with p.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


async def sync_once(
    *,
    discover: bool = True,
    write: bool = True,
    only_pair: tuple[str, str] | None = None,
) -> list[dict]:
    if discover:
        await discover_main(auto_write=True)

    data = load_markets()
    markets = data.setdefault("markets", [])

    async with aiohttp.ClientSession() as session:
        games = await fetch_all_live_games(session)

    updates = sync_markets_to_games(markets, games, only_pair=only_pair)
    if updates and write:
        write_markets(data)
    return updates


async def watch(interval_seconds: float, *, discover: bool, only_pair: tuple[str, str] | None = None) -> None:
    while True:
        updates = await sync_once(discover=discover, write=True, only_pair=only_pair)
        if updates:
            for update in updates:
                print(
                    f"mapped {update['market_name']} -> {update['dota_match_id']} "
                    f"({update['radiant_team']} vs {update['dire_team']}, t={update['game_time_sec']})"
                )
        else:
            print("no new live market mappings")
        await asyncio.sleep(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-discover", action="store_true", help="Skip Polymarket discovery before Steam sync")
    parser.add_argument("--dry-run", action="store_true", help="Do not write Steam mappings")
    parser.add_argument("--watch", action="store_true", help="Keep syncing live Steam matches")
    parser.add_argument("--interval", type=float, default=30.0, help="Watch interval seconds")
    parser.add_argument("--teams", nargs=2, metavar=("TEAM_A", "TEAM_B"), help="Only sync this team pair")
    args = parser.parse_args()
    only_pair = tuple(args.teams) if args.teams else None

    if args.watch:
        asyncio.run(watch(args.interval, discover=not args.no_discover, only_pair=only_pair))
        return

    updates = asyncio.run(sync_once(discover=not args.no_discover, write=not args.dry_run, only_pair=only_pair))
    if updates:
        for update in updates:
            print(
                f"mapped {update['market_name']} -> {update['dota_match_id']} "
                f"({update['radiant_team']} vs {update['dire_team']}, t={update['game_time_sec']})"
            )
    else:
        print("no new live market mappings")


if __name__ == "__main__":
    main()
