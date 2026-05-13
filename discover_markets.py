from __future__ import annotations

import asyncio
import json
import os
import re
from html import unescape
from urllib.parse import urljoin

import aiohttp
import yaml

from poly_gamma import fetch_active_markets, filter_dota_markets, parse_clob_token_ids

MARKETS_YAML = os.path.join(os.path.dirname(__file__), "markets.yaml")
POLYMARKET_DOTA_GAMES_URL = "https://polymarket.com/esports/dota-2/games"
POLYMARKET_ORIGIN = "https://polymarket.com"


def _load_existing_token_ids(path: str) -> set[str]:
    """Return all yes/no token IDs already in markets.yaml."""
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return set()
    existing: set[str] = set()
    for m in data.get("markets", []):
        existing.add(str(m.get("yes_token_id", "")))
        existing.add(str(m.get("no_token_id", "")))
    return existing


def _parse_teams(question: str) -> tuple[str, str]:
    """Best-effort team name extraction from a 'Team A vs Team B' question string."""
    question = re.sub(r"^Dota\s*2:\s*", "", question.strip(), flags=re.I)
    question = re.sub(r"\s*-\s*Game\s*\d+\s*Winner\s*$", "", question, flags=re.I)
    question = re.sub(r"\s*\([^)]*\)\s*-\s*.*$", "", question)
    match = re.search(r"^(.+?)\s+vs\.?\s+(.+?)$", question.strip())
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return "Team A", "Team B"




def _parse_outcomes(outcomes) -> list[str]:
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return []
    if not isinstance(outcomes, list):
        return []
    return [str(o).strip() for o in outcomes]


def _outcome_token_pairs(market: dict) -> list[tuple[str, str]]:
    """Return (outcome_label, token_id) pairs using Polymarket outcome order.

    clobTokenIds are ordered to match the outcomes array. For team-winner
    markets this is safer than inferring teams from the question text, whose
    order can differ from the token/outcome order in the payload.
    """
    outcomes = _parse_outcomes(market.get("outcomes"))
    token_ids = market.get("clobTokenIds")
    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except Exception:
            return []
    if not isinstance(token_ids, list):
        return []
    n = min(len(outcomes), len(token_ids))
    return [(outcomes[i], str(token_ids[i])) for i in range(n) if outcomes[i] and str(token_ids[i])]


def _is_map_winner_market(market: dict) -> bool:
    question = str(market.get("question") or market.get("title") or "")
    outcomes = _parse_outcomes(market.get("outcomes"))
    if len(outcomes) != 2:
        return False
    if {str(o).casefold() for o in outcomes} <= {"yes", "no", "over", "under"}:
        return False
    return bool(re.search(r"\bGame\s*\d+\s+Winner\b", question, flags=re.I))


def _extract_next_data(html: str) -> dict:
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.S)
    if not match:
        return {}
    try:
        return json.loads(unescape(match.group(1)))
    except json.JSONDecodeError:
        return {}


def _walk_markets(obj) -> list[dict]:
    out: list[dict] = []
    if isinstance(obj, dict):
        if "clobTokenIds" in obj and ("question" in obj or "title" in obj):
            out.append(obj)
        for value in obj.values():
            out.extend(_walk_markets(value))
    elif isinstance(obj, list):
        for value in obj:
            out.extend(_walk_markets(value))
    return out


def _extract_dota_event_urls(html: str) -> list[str]:
    hrefs = re.findall(r'href="([^"]*?/esports/dota-2/[^"]+)"', html)
    urls: list[str] = []
    seen: set[str] = set()
    for href in hrefs:
        href = unescape(href)
        if "/dota2-" not in href:
            continue
        url = urljoin(POLYMARKET_ORIGIN, href)
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


async def fetch_polymarket_dota_page_markets(session: aiohttp.ClientSession) -> list[dict]:
    """Fallback discovery via the public Dota games page.

    Gamma's generic active-market endpoint can miss esports inventory that is
    visible on the current Polymarket web app. The web app embeds event market
    data in __NEXT_DATA__ on each event page; extract only Game N Winner markets.
    """
    headers = {"user-agent": "Mozilla/5.0"}
    async with session.get(POLYMARKET_DOTA_GAMES_URL, timeout=15, headers=headers) as r:
        r.raise_for_status()
        listing_html = await r.text()

    event_urls = _extract_dota_event_urls(listing_html)
    markets: list[dict] = []

    async def fetch_event(url: str) -> None:
        try:
            async with session.get(url, timeout=15, headers=headers) as r:
                r.raise_for_status()
                html = await r.text()
        except Exception as e:
            print(f"Skipping event page fetch error: {url} ({e})")
            return

        data = _extract_next_data(html)
        for market in _walk_markets(data):
            if not _is_map_winner_market(market):
                continue
            row = dict(market)
            row["source_url"] = url
            markets.append(row)

    await asyncio.gather(*(fetch_event(url) for url in event_urls))

    deduped: list[dict] = []
    seen_conditions: set[str] = set()
    for market in markets:
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        if condition_id and condition_id in seen_conditions:
            continue
        if condition_id:
            seen_conditions.add(condition_id)
        deduped.append(market)
    return deduped


def _append_provisional(path: str, entries: list[dict]) -> None:
    """Append provisional entries (confidence=0.0) to markets.yaml."""
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        data = {}
    data.setdefault("markets", [])
    data["markets"].extend(entries)
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


async def main(auto_write: bool = False):
    async with aiohttp.ClientSession() as session:
        markets = await fetch_active_markets(session)
        dota_markets = [m for m in filter_dota_markets(markets) if _is_map_winner_market(m)]
        if not dota_markets:
            print("Gamma discovery found no map-winner Dota markets; trying public Polymarket Dota page fallback...")
            dota_markets = await fetch_polymarket_dota_page_markets(session)

    if not dota_markets:
        print("No obvious Dota/esports markets found in the active market fetch.")
        return

    existing_tokens = _load_existing_token_ids(MARKETS_YAML)
    new_entries: list[dict] = []

    for m in dota_markets:
        # Keep the legacy names yes/no for downstream compatibility, but map
        # them from outcome order instead of question-string order.
        pairs = _outcome_token_pairs(m)
        if len(pairs) >= 2:
            yes_team, yes = pairs[0]
            no_team, no = pairs[1]
        else:
            yes, no = parse_clob_token_ids(m)
            yes_team, no_team = _parse_teams(m.get("question") or m.get("title") or "")
        question = m.get("question") or m.get("title") or ""
        market_id = str(m.get("id") or m.get("marketId") or "")
        condition_id = str(m.get("conditionId") or m.get("condition_id") or "")

        print("-" * 80)
        print("question:", question)
        print("slug:", m.get("slug"))
        print("market_id:", market_id)
        print("condition_id:", condition_id)
        print("yes_token_id:", yes)
        print("no_token_id:", no)
        if m.get("gameStartTime"):
            print("game_start_time:", m.get("gameStartTime"))
        if m.get("source_url"):
            print("source_url:", m.get("source_url"))
        outcomes = m.get("outcomes")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                pass
        print("outcomes:", outcomes)

        if yes and yes not in existing_tokens and no not in existing_tokens:
            entry = {
                "name": question,
                "market_id": market_id,
                "condition_id": condition_id,
                "yes_token_id": yes,
                "no_token_id": no or "",
                "market_type": "MAP_WINNER",
                "yes_team": yes_team,
                "no_team": no_team,
                "outcome_order_verified": bool(pairs),
                "dota_match_id": "STEAM_MATCH_OR_LOBBY_ID_HERE",
                "confidence": 0.0,
            }
            if m.get("gameStartTime"):
                entry["scheduled_start_utc"] = str(m.get("gameStartTime"))
            if m.get("source_url"):
                entry["source_url"] = str(m.get("source_url"))
            new_entries.append(entry)
            print("  [NEW — will add as provisional if --write passed]")
        else:
            print("  [already in markets.yaml]")

    if new_entries and auto_write:
        _append_provisional(MARKETS_YAML, new_entries)
        print(f"\nAppended {len(new_entries)} provisional entries to {MARKETS_YAML}.")
        print("Set dota_match_id and confidence=1.0 for each entry you want to activate.")
    elif new_entries:
        print(f"\n{len(new_entries)} new market(s) found. Run with --write to add them to {MARKETS_YAML}.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Append new markets to markets.yaml with confidence=0.0")
    args = parser.parse_args()
    asyncio.run(main(auto_write=args.write))
