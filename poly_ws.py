from __future__ import annotations

import asyncio
import json
import time
from typing import Callable

import websockets

from config import WS_RECONNECT_SECONDS

POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class BookStore:
    """In-memory top-of-book store.

    Maintains level-2 price maps when book snapshots / price-change payloads
    contain bids and asks. Falls back to direct bestBid/bestAsk fields when
    those are present.
    """

    def __init__(self):
        self.books = {}

    def _ensure(self, asset_id: str) -> dict:
        return self.books.setdefault(asset_id, {
            "asset_id": asset_id,
            "bids": {},
            "asks": {},
            "best_bid": None,
            "best_ask": None,
            "bid_size": None,
            "ask_size": None,
            "received_at_ns": None,
            "raw": None,
        })

    def _recompute_top(self, book: dict):
        bids = book.get("bids", {})
        asks = book.get("asks", {})

        live_bids = {p: s for p, s in bids.items() if s and s > 0}
        live_asks = {p: s for p, s in asks.items() if s and s > 0}

        if live_bids:
            best_bid = max(live_bids)
            book["best_bid"] = best_bid
            book["bid_size"] = live_bids[best_bid]
        else:
            book["best_bid"] = None
            book["bid_size"] = None

        if live_asks:
            best_ask = min(live_asks)
            book["best_ask"] = best_ask
            book["ask_size"] = live_asks[best_ask]
        else:
            book["best_ask"] = None
            book["ask_size"] = None

    def update_direct(self, asset_id: str, *, best_bid=None, best_ask=None, bid_size=None, ask_size=None, raw=None) -> dict:
        book = self._ensure(asset_id)
        if best_bid is not None:
            book["best_bid"] = best_bid
        if best_ask is not None:
            book["best_ask"] = best_ask
        if bid_size is not None:
            book["bid_size"] = bid_size
        if ask_size is not None:
            book["ask_size"] = ask_size
        book["received_at_ns"] = time.time_ns()
        book["raw"] = raw
        return book

    def replace_snapshot(self, asset_id: str, bids: list, asks: list, raw=None) -> dict:
        book = self._ensure(asset_id)
        book["bids"] = {}
        book["asks"] = {}

        for level in bids or []:
            price = _level_value(level, "price")
            size = _level_value(level, "size")
            if price is not None and size is not None:
                book["bids"][price] = size

        for level in asks or []:
            price = _level_value(level, "price")
            size = _level_value(level, "size")
            if price is not None and size is not None:
                book["asks"][price] = size

        self._recompute_top(book)
        book["received_at_ns"] = time.time_ns()
        book["raw"] = raw
        return book

    def apply_price_change(self, asset_id: str, side: str, price: float, size: float, raw=None) -> dict:
        book = self._ensure(asset_id)
        side = (side or "").upper()
        levels = book["bids"] if side in {"BUY", "BID", "BIDS"} else book["asks"] if side in {"SELL", "ASK", "ASKS"} else None
        if levels is not None:
            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            self._recompute_top(book)
        book["received_at_ns"] = time.time_ns()
        book["raw"] = raw
        return book

    def get(self, asset_id: str) -> dict | None:
        return self.books.get(str(asset_id))


def _level_value(level, key: str):
    if isinstance(level, dict):
        return _to_float(level.get(key))
    # Some feeds encode levels as [price, size].
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return _to_float(level[0 if key == "price" else 1])
    return None


def ingest_ws_event(event: dict, store: BookStore) -> list[tuple[dict, str | None]]:
    """Parse common Polymarket market-channel payloads defensively.

    Returns a list of (book, source_event_type) pairs that changed. Tests can
    ignore the return value; the live loop uses it for CSV book logging.
    """
    changed: list[tuple[dict, str | None]] = []
    if not isinstance(event, dict):
        return changed

    source_event_type = str(event.get("event_type") or event.get("type") or "") or None

    # Book snapshot: usually has asset_id plus bids/asks arrays.
    asset_id = str(event.get("asset_id") or event.get("assetId") or "")
    if asset_id and ("bids" in event or "asks" in event):
        book = store.replace_snapshot(asset_id, event.get("bids", []), event.get("asks", []), raw=event)
        return [(book, source_event_type)]

    # Direct best bid/ask payloads.
    if asset_id:
        best_bid = _to_float(event.get("best_bid") or event.get("bestBid"))
        best_ask = _to_float(event.get("best_ask") or event.get("bestAsk"))
        bid_size = _to_float(event.get("bid_size") or event.get("bidSize"))
        ask_size = _to_float(event.get("ask_size") or event.get("askSize"))
        if best_bid is not None or best_ask is not None:
            book = store.update_direct(asset_id, best_bid=best_bid, best_ask=best_ask, bid_size=bid_size, ask_size=ask_size, raw=event)
            return [(book, source_event_type)]

    # Price changes often arrive as a list of changes under one event.
    for change in event.get("changes", []) or []:
        change_asset_id = str(change.get("asset_id") or change.get("assetId") or asset_id or "")
        price = _to_float(change.get("price"))
        size = _to_float(change.get("size"))
        side = change.get("side")
        if change_asset_id and price is not None and size is not None:
            book = store.apply_price_change(change_asset_id, side, price, size, raw=event)
            changed.append((book, source_event_type))
    return changed


async def listen_books(
    asset_ids: list[str],
    store: BookStore,
    book_logger=None,
    on_book_update: Callable[[], None] | None = None,
):
    """Listen to Polymarket WS. asset_ids is a shared mutable list; new IDs added
    by the mapping refresh in steam_loop trigger a reconnect to re-subscribe."""
    subscribed_ids: set[str] = set()

    while True:
        clean_ids = [str(a) for a in asset_ids if a and "TOKEN_ID_HERE" not in str(a)]
        if not clean_ids:
            await asyncio.sleep(WS_RECONNECT_SECONDS)
            continue

        # Reconnect whenever the set of IDs has changed
        if set(clean_ids) == subscribed_ids:
            # Still connected; normal reconnect path handles this
            pass

        try:
            async with websockets.connect(POLY_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                # Polymarket CLOB WS uses "assets_ids" (with trailing s) per their docs.
                # Verify against https://docs.polymarket.com if subscriptions stop working.
                await ws.send(json.dumps({"assets_ids": clean_ids, "type": "market"}))
                subscribed_ids = set(clean_ids)
                print(f"Subscribed to {len(clean_ids)} Polymarket assets")

                last_msg_at = time.time()
                while True:
                    try:
                        # Use a 5s timeout on recv to check set-changes and heartbeats
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        last_msg_at = time.time()
                    except asyncio.TimeoutError:
                        # Heartbeat check: if we haven't received a message in 15s,
                        # the connection might be stalled (silent death).
                        if time.time() - last_msg_at > 15.0:
                            print(f"WS heartbeat timeout (15s), forcing reconnect...")
                            break
                        
                        # Periodically check if asset IDs changed while waiting
                        current_ids = [str(a) for a in asset_ids if a and "TOKEN_ID_HERE" not in str(a)]
                        if set(current_ids) != subscribed_ids:
                            print(f"New asset IDs detected, reconnecting WS ({len(current_ids)} assets)...")
                            break
                        continue

                    data = json.loads(msg)
                    events = data if isinstance(data, list) else [data]
                    changed = False
                    for event in events:
                        for book, source_event_type in ingest_ws_event(event, store):
                            changed = True
                            if book_logger:
                                book_logger.log_book(book, source_event_type=source_event_type)
                    if changed and on_book_update:
                        on_book_update()
        except Exception as e:
            print(f"WebSocket error: {e}. Reconnecting in {WS_RECONNECT_SECONDS}s...")
            await asyncio.sleep(WS_RECONNECT_SECONDS)
