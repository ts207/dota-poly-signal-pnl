from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class ShadowTrade:
    shadow_id: str
    timestamp_utc: str
    event_type: str
    event_tier: str | None
    event_family: str | None
    market_type: str | None
    proxy_market_type: str | None
    is_game3_match_proxy: bool
    token_id: str
    side: str
    match_id: str
    market_name: str | None
    decision: str
    skip_reason: str | None
    entry_price: float | None
    bid_at_entry: float | None
    ask_at_entry: float | None
    spread_at_entry: float | None
    fair_price: float | None
    executable_edge: float | None
    lag: float | None
    event_quality: float | None
    source_cadence_quality: str | None
    game_time_sec: int | None
    markout_3s: float | None = None
    markout_10s: float | None = None
    markout_30s: float | None = None
    markout_60s: float | None = None
    would_pnl_3s: float | None = None
    would_pnl_10s: float | None = None
    would_pnl_30s: float | None = None
    would_pnl_60s: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_shadow_trade(*, signal: dict, mapping: dict, game: dict, token_id: str, side: str) -> ShadowTrade:
    bid = _to_float(signal.get("bid"))
    ask = _to_float(signal.get("ask"))
    entry = _to_float(signal.get("executable_price")) or ask

    spread = None
    if bid is not None and ask is not None:
        spread = ask - bid

    shadow_id = f"{game.get('match_id') or game.get('lobby_id')}:{token_id}:{time.time_ns()}"

    return ShadowTrade(
        shadow_id=shadow_id,
        timestamp_utc=_utc_now(),
        event_type=str(signal.get("event_type") or ""),
        event_tier=signal.get("event_tier"),
        event_family=signal.get("event_family"),
        market_type=mapping.get("market_type"),
        proxy_market_type=signal.get("proxy_market_type"),
        is_game3_match_proxy=bool(signal.get("is_game3_match_proxy")),
        token_id=str(token_id or signal.get("token_id") or ""),
        side=str(side or signal.get("side") or ""),
        match_id=str(game.get("match_id") or game.get("lobby_id") or ""),
        market_name=mapping.get("name"),
        decision=str(signal.get("decision") or ""),
        skip_reason=signal.get("reason") or signal.get("skip_reason"),
        entry_price=entry,
        bid_at_entry=bid,
        ask_at_entry=ask,
        spread_at_entry=spread,
        fair_price=_to_float(signal.get("fair_price")),
        executable_edge=_to_float(signal.get("executable_edge")),
        lag=_to_float(signal.get("lag")),
        event_quality=_to_float(signal.get("event_quality")),
        source_cadence_quality=signal.get("source_cadence_quality"),
        game_time_sec=game.get("game_time_sec"),
    )


async def log_shadow_markouts(shadow: ShadowTrade, *, book_store, logger, delays=(3, 10, 30, 60)) -> None:
    entry = shadow.entry_price
    if entry is None:
        logger.log_shadow_trade(shadow)
        return

    values: dict[int, tuple[float | None, float | None]] = {}

    elapsed = 0
    for delay in delays:
        await asyncio.sleep(max(0, delay - elapsed))
        elapsed = delay
        book = book_store.get(shadow.token_id) if book_store else None
        future_bid = _to_float((book or {}).get("best_bid"))
        markout = (future_bid - entry) if future_bid is not None else None
        pnl = (markout / entry) if markout is not None and entry > 0 else None
        values[delay] = (markout, pnl)

    shadow.markout_3s, shadow.would_pnl_3s = values.get(3, (None, None))
    shadow.markout_10s, shadow.would_pnl_10s = values.get(10, (None, None))
    shadow.markout_30s, shadow.would_pnl_30s = values.get(30, (None, None))
    shadow.markout_60s, shadow.would_pnl_60s = values.get(60, (None, None))

    logger.log_shadow_trade(shadow)
