from __future__ import annotations

import asyncio
import json
import math
import os
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any

from config import (
    ALLOW_EVENT_TRADES,
    ALLOW_CONFIRMATION_ONLY_LIVE_TRADES,
    ALLOW_GAME_OVER_ONLY,
    DEFAULT_MAX_FILL_PRICE,
    DISABLE_STRUCTURE_TRADES,
    LIVE_ORDER_TYPE,
    LIVE_SAFETY_MARGIN,
    LIVE_TICK_SIZE,
    MAX_BOOK_AGE_MS,
    MAX_OPEN_POSITIONS,
    MAX_SPREAD,
    MAX_STEAM_AGE_MS,
    MAX_TOTAL_LIVE_USD,
    MAX_TRADE_USD,
    MIN_EXECUTABLE_EDGE,
    MIN_LAG,
    TRADE_EVENTS,
)
from event_taxonomy import event_tier
from signal_engine import age_ms
from mapping_validator import validate_mapping_identity

STRUCTURE_EVENTS = frozenset({
    "T2_TOWER_FALL",
    "T3_TOWER_FALL",
    "MULTIPLE_T3_TOWERS_DOWN",
    "FIRST_T4_TOWER_FALL",
    "SECOND_T4_TOWER_FALL",
})

_ALLOWED_ORDER_TYPES = {"FAK", "FOK"}


def round_down_to_tick(price: float, tick_size: str | float = LIVE_TICK_SIZE) -> float:
    """Round a probability price down to Polymarket's tick grid."""
    try:
        p = Decimal(str(price))
        tick = Decimal(str(tick_size))
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise ValueError(f"invalid price/tick: price={price!r} tick_size={tick_size!r}") from exc
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    return float((p / tick).to_integral_value(rounding=ROUND_DOWN) * tick)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if hasattr(value, "dict"):
        return _jsonable(value.dict())
    if hasattr(value, "__dict__"):
        return _jsonable(vars(value))
    return str(value)


def _response_to_dict(resp: Any) -> dict[str, Any]:
    data = _jsonable(resp)
    if isinstance(data, dict):
        return data
    return {"raw": data}


def _status_from_response(resp: dict[str, Any]) -> str:
    status = resp.get("status") or resp.get("orderStatus") or resp.get("state")
    if status:
        return str(status)
    if resp.get("success") is True:
        return "success"
    if resp.get("success") is False:
        return "rejected"
    return "unknown"


def _error_from_response(resp: dict[str, Any]) -> str:
    return str(resp.get("errorMsg") or resp.get("error") or resp.get("message") or "")


def _filled_usd_from_response(resp: dict[str, Any], requested_usd: float) -> float:
    """Best-effort filled-spend extraction for a BUY market order response.

    Polymarket response schemas can differ by client version. Prefer explicit USD
    filled/spent fields. If the response is a successful matched response without
    explicit fields, conservatively assume the requested amount filled so budget
    accounting errs on the safe side.
    """
    explicit_keys = (
        "filledSizeUsd", "filled_size_usd", "filledAmountUsd", "filled_amount_usd",
        "amountFilled", "filledAmount", "filled", "filled_size",
    )
    for key in explicit_keys:
        value = _to_float(resp.get(key))
        if value is not None and value >= 0:
            return min(value, requested_usd)

    taking = _to_float(resp.get("takingAmount") or resp.get("taking_amount"))
    making = _to_float(resp.get("makingAmount") or resp.get("making_amount"))
    # For a BUY market order, clients may report either USDC spent or shares
    # received depending on perspective. Avoid treating a large share count as USD.
    if taking is not None and 0 <= taking <= requested_usd * 1.05:
        return min(taking, requested_usd)
    if making is not None and 0 <= making <= requested_usd * 1.05:
        return min(making, requested_usd)

    status = _status_from_response(resp).lower()
    if resp.get("success") is True and status in {"matched", "success", "delayed", "live"}:
        return requested_usd
    if status in {"matched", "delayed", "live"}:
        return requested_usd
    return 0.0


def _avg_fill_price(resp: dict[str, Any], default_price: float, filled_usd: float) -> float | None:
    for key in ("avgFillPrice", "avg_fill_price", "averagePrice", "price"):
        value = _to_float(resp.get(key))
        if value is not None and value > 0:
            return value
    shares = _to_float(resp.get("shares") or resp.get("filledShares") or resp.get("filled_shares"))
    if shares and filled_usd > 0:
        return filled_usd / shares
    return default_price if filled_usd > 0 else None


@dataclass
class LiveOrderAttempt:
    event_type: str
    event_direction: str
    token_id: str
    side: str
    fair_price: float | None
    best_ask: float | None
    price_cap: float | None
    edge: float | None
    lag: float | None
    spread: float | None
    book_age_ms: int | None
    steam_age_ms: int | None
    order_type: str
    submitted_size_usd: float
    filled_size_usd: float = 0.0
    avg_fill_price: float | None = None
    order_status: str = "not_submitted"
    reason_if_rejected: str = ""
    market_name: str | None = None
    match_id: str | None = None
    raw_response_json: str = ""
    created_at_ns: int = 0
    submit_start_ns: int | None = None
    response_received_ns: int | None = None
    submit_latency_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LiveCLOBClient:
    """Thin optional wrapper around py-clob-client-v2.

    The dependency is imported lazily so paper mode and tests do not require live
    trading packages or wallet credentials.
    """

    def __init__(self):
        try:
            from py_clob_client_v2 import (  # type: ignore
                ApiCreds,
                ClobClient,
                MarketOrderArgs,
                OrderType,
                PartialCreateOrderOptions,
                Side,
            )
        except Exception as exc:  # pragma: no cover - exercised only in live env
            raise RuntimeError(
                "Live trading requires py-clob-client-v2. Install with: "
                "pip install -r requirements-live.txt"
            ) from exc

        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
        private_key = os.getenv("POLY_PRIVATE_KEY") or os.getenv("PK")
        if not private_key:
            raise RuntimeError("Missing POLY_PRIVATE_KEY/PK for live trading")

        creds = ApiCreds(
            api_key=os.getenv("POLY_CLOB_API_KEY") or os.getenv("CLOB_API_KEY"),
            api_secret=os.getenv("POLY_CLOB_SECRET") or os.getenv("CLOB_SECRET"),
            api_passphrase=os.getenv("POLY_CLOB_PASS_PHRASE") or os.getenv("CLOB_PASS_PHRASE"),
        )
        kwargs: dict[str, Any] = {
            "host": host,
            "chain_id": chain_id,
            "key": private_key,
            "creds": creds,
        }
        signature_type = os.getenv("POLY_SIGNATURE_TYPE")
        if signature_type:
            kwargs["signature_type"] = int(signature_type)
        funder = os.getenv("POLY_FUNDER_ADDRESS") or os.getenv("FUNDER_ADDRESS")
        if funder:
            kwargs["funder"] = funder

        self._client = ClobClient(**kwargs)
        self._MarketOrderArgs = MarketOrderArgs
        self._OrderType = OrderType
        self._Options = PartialCreateOrderOptions
        self._Side = Side

    async def buy_fak_market(self, *, token_id: str, amount_usd: float, price_cap: float, tick_size: str, neg_risk: bool) -> dict[str, Any]:
        order_type = getattr(self._OrderType, LIVE_ORDER_TYPE)
        side_buy = getattr(self._Side, "BUY")
        order_args = self._MarketOrderArgs(
            token_id=str(token_id),
            amount=float(amount_usd),
            side=side_buy,
            price=float(price_cap),
        )
        options = self._Options(tick_size=str(tick_size), neg_risk=bool(neg_risk))
        resp = await asyncio.to_thread(
            self._client.create_and_post_market_order,
            order_args=order_args,
            options=options,
            order_type=order_type,
        )
        return _response_to_dict(resp)


class LiveExecutor:
    """Guarded $10 live-test executor.

    It only sends capped BUY market orders with FAK/FOK semantics and keeps hard
    in-process budget counters. Persistence is intentionally simple: this is for
    a tiny path test, not unattended trading.
    """

    def __init__(self, client: Any | None = None):
        self.client = client or LiveCLOBClient()
        self.total_submitted_usd = 0.0
        self.total_filled_usd = 0.0
        self.open_positions = 0

    def remaining_budget(self) -> float:
        return max(0.0, MAX_TOTAL_LIVE_USD - self.total_submitted_usd)

    def _reject(self, signal: dict, mapping: dict, game: dict, reason: str, **extra) -> LiveOrderAttempt:
        return LiveOrderAttempt(
            event_type=str(signal.get("event_type") or ""),
            event_direction=str(signal.get("event_direction") or ""),
            token_id=str(signal.get("token_id") or ""),
            side=str(signal.get("side") or ""),
            fair_price=_to_float(signal.get("fair_price")),
            best_ask=_to_float(signal.get("ask")),
            price_cap=extra.get("price_cap"),
            edge=_to_float(signal.get("executable_edge")),
            lag=_to_float(signal.get("lag")),
            spread=_to_float(signal.get("spread")),
            book_age_ms=signal.get("book_age_ms"),
            steam_age_ms=signal.get("steam_age_ms"),
            order_type=LIVE_ORDER_TYPE,
            submitted_size_usd=0.0,
            order_status="rejected_precheck",
            reason_if_rejected=reason,
            market_name=mapping.get("name"),
            match_id=str(game.get("match_id") or game.get("lobby_id") or ""),
            created_at_ns=time.time_ns(),
        )

    async def try_buy(self, *, signal: dict, mapping: dict, game: dict, book_store) -> LiveOrderAttempt:
        mapping_result = validate_mapping_identity(mapping, game)
        if not mapping_result.ok:
            return self._reject(
                signal, mapping, game,
                f"mapping_invalid:{';'.join(mapping_result.mapping_errors) or 'confidence_not_1'}",
            )
        if not ALLOW_EVENT_TRADES:
            return self._reject(signal, mapping, game, "event_trades_disabled")
        if ALLOW_GAME_OVER_ONLY and not game.get("game_over"):
            return self._reject(signal, mapping, game, "game_over_only")
        if LIVE_ORDER_TYPE not in _ALLOWED_ORDER_TYPES:
            return self._reject(signal, mapping, game, "order_type_not_allowed")
        if self.total_submitted_usd >= MAX_TOTAL_LIVE_USD:
            return self._reject(signal, mapping, game, "max_total_live_usd_reached")
        if self.open_positions >= MAX_OPEN_POSITIONS:
            return self._reject(signal, mapping, game, "max_open_positions_reached")

        event_type = str(signal.get("event_type") or "")
        cluster_types = {e for e in str(signal.get("cluster_event_types") or event_type).split("+") if e}
        if event_type == "OBJECTIVE_CONVERSION_T3":
            ask = _to_float(signal.get("ask"))
            edge = _to_float(signal.get("executable_edge"))
            if ask is not None and ask > 0.85 and (edge is None or edge < 0.08):
                return self._reject(signal, mapping, game, "objective_conversion_t3_requires_8c_edge_above_85c")
        if _to_float(signal.get("ask")) is not None and _to_float(signal.get("ask")) >= 0.95 and event_type != "THRONE_EXPOSED":
            return self._reject(signal, mapping, game, "chasing_terminal_price")
        if DISABLE_STRUCTURE_TRADES and (event_type in STRUCTURE_EVENTS or cluster_types <= STRUCTURE_EVENTS):
            return self._reject(signal, mapping, game, "structure_trade_disabled")
        if TRADE_EVENTS and not (event_type in TRADE_EVENTS or cluster_types & TRADE_EVENTS):
            return self._reject(signal, mapping, game, "event_not_allowed")
        if event_tier(event_type) == "C" and not ALLOW_CONFIRMATION_ONLY_LIVE_TRADES:
            return self._reject(signal, mapping, game, "confirmation_only_event")

        fair = _to_float(signal.get("fair_price"))
        lag = _to_float(signal.get("lag"))
        edge = _to_float(signal.get("executable_edge"))
        if fair is None:
            return self._reject(signal, mapping, game, "missing_fair_price")
        if edge is None or edge < MIN_EXECUTABLE_EDGE:
            return self._reject(signal, mapping, game, "edge_too_small")
        if lag is None or lag < MIN_LAG:
            return self._reject(signal, mapping, game, "lag_too_small")

        steam_age = age_ms(game.get("received_at_ns"))
        if steam_age > MAX_STEAM_AGE_MS:
            return self._reject(signal, mapping, game, "steam_stale")

        token_id = str(signal.get("token_id") or "")
        book = book_store.get(token_id) if book_store else None
        if not book:
            return self._reject(signal, mapping, game, "missing_live_book")
        book_age = age_ms(book.get("received_at_ns"))
        if book_age > MAX_BOOK_AGE_MS:
            return self._reject(signal, mapping, game, "book_stale")
        ask = _to_float(book.get("best_ask"))
        bid = _to_float(book.get("best_bid"))
        if ask is None or bid is None:
            return self._reject(signal, mapping, game, "missing_bid_or_ask")
        spread = ask - bid
        if spread > MAX_SPREAD:
            return self._reject(signal, mapping, game, "spread_too_wide")
        event_max_fill = _to_float(signal.get("max_fill_price")) or DEFAULT_MAX_FILL_PRICE
        event_max_fill = min(max(event_max_fill, 0.0), 0.99)
        if ask > event_max_fill:
            return self._reject(signal, mapping, game, "ask_above_event_max_fill")
        if ask >= 0.95 and event_type != "THRONE_EXPOSED":
            return self._reject(signal, mapping, game, "chasing_terminal_price")

        # Recompute edge against the fresh best ask immediately before submission.
        fresh_edge = fair - ask
        if fresh_edge < MIN_EXECUTABLE_EDGE:
            return self._reject(signal, mapping, game, "fresh_edge_too_small")
        if event_type == "OBJECTIVE_CONVERSION_T3" and ask > 0.85 and fresh_edge < 0.08:
            return self._reject(signal, mapping, game, "objective_conversion_t3_requires_8c_fresh_edge_above_85c")

        tick_size = str(mapping.get("tick_size") or LIVE_TICK_SIZE)
        price_cap = round_down_to_tick(fair - LIVE_SAFETY_MARGIN, tick_size)
        price_cap = min(price_cap, event_max_fill)
        price_cap = round_down_to_tick(price_cap, tick_size)
        if price_cap <= 0 or price_cap > 0.99 or not math.isfinite(price_cap):
            return self._reject(signal, mapping, game, "invalid_price_cap", price_cap=price_cap)
        if ask > price_cap:
            return self._reject(signal, mapping, game, "best_ask_above_price_cap", price_cap=price_cap)

        order_usd = min(MAX_TRADE_USD, self.remaining_budget())
        if order_usd <= 0:
            return self._reject(signal, mapping, game, "no_remaining_live_budget", price_cap=price_cap)

        self.total_submitted_usd += order_usd
        neg_risk = bool(mapping.get("neg_risk", False))
        attempt = LiveOrderAttempt(
            event_type=event_type,
            event_direction=str(signal.get("event_direction") or ""),
            token_id=token_id,
            side=str(signal.get("side") or ""),
            fair_price=fair,
            best_ask=ask,
            price_cap=price_cap,
            edge=round(fresh_edge, 4),
            lag=lag,
            spread=round(spread, 4),
            book_age_ms=book_age,
            steam_age_ms=steam_age,
            order_type=LIVE_ORDER_TYPE,
            submitted_size_usd=order_usd,
            market_name=mapping.get("name"),
            match_id=str(game.get("match_id") or game.get("lobby_id") or ""),
            created_at_ns=time.time_ns(),
        )

        attempt.submit_start_ns = time.time_ns()
        try:
            resp = await self.client.buy_fak_market(
                token_id=token_id,
                amount_usd=order_usd,
                price_cap=price_cap,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            attempt.response_received_ns = time.time_ns()
        except Exception as exc:
            attempt.response_received_ns = time.time_ns()
            attempt.order_status = "exception"
            attempt.reason_if_rejected = repr(exc)
            if attempt.submit_start_ns and attempt.response_received_ns:
                attempt.submit_latency_ms = round((attempt.response_received_ns - attempt.submit_start_ns) / 1_000_000, 2)
            return attempt

        if attempt.submit_start_ns and attempt.response_received_ns:
            attempt.submit_latency_ms = round((attempt.response_received_ns - attempt.submit_start_ns) / 1_000_000, 2)

        attempt.raw_response_json = json.dumps(_jsonable(resp), sort_keys=True)[:4000]
        attempt.order_status = _status_from_response(resp)
        attempt.reason_if_rejected = _error_from_response(resp)
        attempt.filled_size_usd = round(_filled_usd_from_response(resp, order_usd), 6)
        attempt.avg_fill_price = _avg_fill_price(resp, price_cap, attempt.filled_size_usd)
        if attempt.filled_size_usd > 0:
            self.total_filled_usd += attempt.filled_size_usd
            self.open_positions += 1
        return attempt
