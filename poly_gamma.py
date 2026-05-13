import aiohttp
import json

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

async def fetch_active_markets(session: aiohttp.ClientSession) -> list[dict]:
    params = {
        "active": "true",
        "closed": "false",
        "limit": 500,
    }

    async with session.get(GAMMA_MARKETS_URL, params=params, timeout=10) as r:
        r.raise_for_status()
        markets = await r.json()

    return markets

def filter_dota_markets(markets: list[dict]) -> list[dict]:
    out = []

    keywords = ["dota", "dota 2", "esports", "the international", "dreamleague", "esl one"]

    for m in markets:
        text = " ".join(
            str(m.get(k, "") or "")
            for k in ["question", "title", "slug", "description"]
        ).lower()

        if any(k in text for k in keywords):
            out.append(m)

    return out

def parse_clob_token_ids(market: dict) -> tuple[str | None, str | None]:
    token_ids = market.get("clobTokenIds")

    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except Exception:
            return None, None

    if not isinstance(token_ids, list) or len(token_ids) < 2:
        return None, None

    yes_token_id = str(token_ids[0])
    no_token_id = str(token_ids[1])
    return yes_token_id, no_token_id
