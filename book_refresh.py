from __future__ import annotations

import asyncio
import time
import aiohttp

CLOB_BOOK_URL = "https://clob.polymarket.com/book"


async def fetch_fresh_book(
    session: aiohttp.ClientSession,
    token_id: str,
    timeout_ms: int = 2000,
) -> dict | None:
    """Fetch a fresh top-of-book snapshot from Polymarket's REST CLOB API.

    Returns a dict with best_bid, best_ask, bid_size, ask_size, spread, mid,
    received_at_ns, or None on failure.
    """
    params = {"token_id": token_id}
    start_ns = time.time_ns()
    try:
        async with session.get(
            CLOB_BOOK_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout_ms / 1000.0),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        return None

    response_ns = time.time_ns()

    bids = data.get("bids") or []
    asks = data.get("asks") or []

    best_bid = None
    best_ask = None
    bid_size = None
    ask_size = None

    if asks:
        try:
            best_ask = float(asks[0]["price"])
            ask_size = float(asks[0].get("size", 0))
        except (ValueError, KeyError, IndexError, TypeError):
            best_ask = None
            ask_size = None

    if bids:
        try:
            best_bid = float(bids[0]["price"])
            bid_size = float(bids[0].get("size", 0))
        except (ValueError, KeyError, IndexError, TypeError):
            best_bid = None
            bid_size = None

    if best_ask is None and best_bid is None:
        return None

    spread = (best_ask - best_bid) if (best_ask is not None and best_bid is not None) else None
    mid = ((best_ask + best_bid) / 2.0) if (best_ask is not None and best_bid is not None) else (best_ask or best_bid)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread": spread,
        "mid": mid,
        "received_at_ns": response_ns,
        "refresh_latency_ns": response_ns - start_ns,
    }