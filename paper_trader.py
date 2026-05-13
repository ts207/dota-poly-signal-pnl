from __future__ import annotations

import time
from dataclasses import dataclass, asdict
from typing import Any

from config import (
    PAPER_SLIPPAGE_CENTS, PAPER_TRADE_SIZE_USD, MAX_OPEN_USD_PER_MATCH,
    EXIT_TAKE_PROFIT, EXIT_STOP_LOSS_ABS, EXIT_STOP_LOSS_REL,
    EXIT_HORIZON_SEC, EXIT_HORIZON_BY_EVENT, MAX_HOLD_HOURS,
)


@dataclass
class Position:
    token_id: str
    match_id: str
    market_name: str | None
    side: str                     # "YES" or "NO"
    entry_price: float
    shares: float
    cost_usd: float
    entry_time_ns: int
    entry_game_time_sec: int | None
    event_type: str
    lag: float
    expected_move: float          # expected repricing at entry; drives TP and stop
    fair_price: float = 0.0       # model fair at entry; caps TP target

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ClosedPosition:
    token_id: str
    match_id: str
    market_name: str | None
    side: str
    entry_price: float
    exit_price: float
    shares: float
    cost_usd: float
    proceeds_usd: float
    pnl_usd: float
    roi: float
    hold_sec: float
    entry_game_time_sec: int | None
    exit_game_time_sec: int | None
    event_type: str
    lag: float
    expected_move: float
    exit_reason: str
    entry_time_ns: int
    exit_time_ns: int
    fair_price: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PaperTrader:
    """Paper trader with real position tracking and exit logic.

    Entry: fills immediately at current best ask, one position per token.
    Opposing-side guard: refuses entry if the binary market's other token is already open.

    Exit priority (checked in order each cycle):
      1. take_profit  — current bid >= entry_price + expected_move (signal target), capped at EXIT_TAKE_PROFIT
      2. stop_loss    — bid <= max(EXIT_STOP_LOSS_ABS, entry - min(EXIT_STOP_LOSS_REL, expected_move))
      3. horizon      — position age >= EXIT_HORIZON_BY_EVENT[event_type] (per-event calibrated)
      4. game_over    — game ended (settled at current bid)
      5. max_hold     — safety net if game_over never fires (stale Steam data)

    Adverse event exit: when an event fires against an open position, main.py calls
    force_exit() before attempting to enter the opposing side.

    check_exits() is called from two paths:
      - Steam poll loop  (every ~0.5s): passes real game_over_match_ids
      - Book WS callback (every book tick): passes empty game_over set,
        so only conditions 1–3 and 5 can fire there
    """

    def __init__(self):
        # token_id → open Position
        self.positions: dict[str, Position] = {}
        self.closed: list[ClosedPosition] = []
        # match_id → total USD currently open for that match
        self._match_open_usd: dict[str, float] = {}

    def enter(
        self,
        signal: dict,
        token_id: str,
        side: str,           # "YES" or "NO"
        book_store,
        match_id: str,
        market_name: str | None,
        opposing_token_id: str = "",
    ) -> tuple[Position | None, str]:
        """Attempt to enter a position. Returns (position, reason)."""
        if token_id in self.positions:
            return None, "already_in_position"

        # Guard: refuse if the other side of the same binary market is already open.
        if opposing_token_id and opposing_token_id in self.positions:
            return None, "opposing_position_open"

        match_open = self._match_open_usd.get(match_id, 0.0)
        if match_open >= MAX_OPEN_USD_PER_MATCH:
            return None, f"match_exposure_cap ({match_open:.0f}>={MAX_OPEN_USD_PER_MATCH:.0f})"

        book = book_store.get(token_id) or {}
        ask = book.get("best_ask")
        bid = book.get("best_bid")
        ask_size = book.get("ask_size")

        if ask is None or bid is None:
            return None, "no_ask_or_bid"

        ask = float(ask)
        bid = float(bid)

        # Realistic taker simulation: a BUY fills at the displayed ask, not mid.
        # The limit cap allows a small move from the signal-time ask, but it still
        # rejects entries where the current ask has outrun estimated fair value.
        signal_ask = float(signal.get("ask", ask))
        fair_price = float(signal.get("fair_price", 0.99))
        max_price = min(signal_ask + PAPER_SLIPPAGE_CENTS, fair_price - 0.005, 0.99)

        if ask > max_price:
            return None, f"ask_moved_above_limit ({ask:.4f} > {max_price:.4f})"

        fill_price = ask
        size_usd = float(signal.get("target_size_usd") or PAPER_TRADE_SIZE_USD)

        if ask_size is not None:
            available = ask * float(ask_size)
            size_usd = min(size_usd, available)

        # Trim to remaining match headroom
        remaining_cap = MAX_OPEN_USD_PER_MATCH - self._match_open_usd.get(match_id, 0.0)
        size_usd = min(size_usd, remaining_cap)

        if size_usd <= 0:
            return None, "no_available_size"

        shares = size_usd / fill_price
        expected_move = float(signal.get("expected_move") or 0.0)

        pos = Position(
            token_id=token_id,
            match_id=match_id,
            market_name=market_name,
            side=side,
            entry_price=fill_price,
            shares=shares,
            cost_usd=size_usd,
            entry_time_ns=time.time_ns(),
            entry_game_time_sec=signal.get("game_time_sec"),
            event_type=signal.get("event_type", ""),
            lag=float(signal.get("lag", 0)),
            expected_move=expected_move,
            fair_price=fair_price,
        )
        self.positions[token_id] = pos
        self._match_open_usd[match_id] = self._match_open_usd.get(match_id, 0.0) + size_usd
        return pos, "filled"

    def force_exit(self, token_id: str, book_store, reason: str) -> ClosedPosition | None:
        """Immediately close a position regardless of TP/SL/horizon (e.g. adverse event)."""
        pos = self.positions.get(token_id)
        if pos is None:
            return None
        book = book_store.get(token_id) or {}
        bid = book.get("best_bid")
        ask = book.get("best_ask")
        # Realistic taker exit for a long token position: SELL at bid, not mid.
        exit_px = float(bid) if bid is not None else (float(ask) if ask is not None else pos.entry_price)
        if bid is None and ask is None:
            print(
                f"WARNING: force_exit({token_id}, {reason}) — book empty, "
                f"recording exit at entry_price={pos.entry_price:.4f}; P&L will show 0"
            )
        return self._close_position(pos, exit_px, reason, exit_game_time=None)

    def check_exits(
        self,
        book_store,
        game_over_match_ids: set[str],
        current_game_times: dict[str, int | None] | None = None,
    ) -> list[ClosedPosition]:
        """Check all open positions for exit conditions. Returns newly closed positions."""
        closed_now: list[ClosedPosition] = []
        to_close: list[tuple[str, float, str]] = []  # (token_id, exit_price, reason)

        max_hold_sec = MAX_HOLD_HOURS * 3600

        for token_id, pos in self.positions.items():
            book = book_store.get(token_id) or {}
            raw_bid = book.get("best_bid")
            raw_ask = book.get("best_ask")
            bid = float(raw_bid) if raw_bid is not None else None
            ask = float(raw_ask) if raw_ask is not None else None

            # Realistic taker exit for a long token position: SELL at bid, not mid.
            exit_px = bid if bid is not None else None
            if exit_px is None:
                exit_px = ask

            age_sec = (time.time_ns() - pos.entry_time_ns) / 1e9

            # TP: target the model fair price when available. expected_move is
            # measured from the signal anchor, so entry + expected_move can
            # overshoot fair if the ask has already repriced before fill.
            model_target = pos.fair_price if pos.fair_price > pos.entry_price else None
            if model_target is None and pos.expected_move > 0:
                model_target = pos.entry_price + pos.expected_move
            take_profit_price = min(model_target or EXIT_TAKE_PROFIT, EXIT_TAKE_PROFIT)
            # Stop: tighten to expected_move if it's less than the configured relative stop
            stop_offset = min(EXIT_STOP_LOSS_REL, pos.expected_move) if pos.expected_move > 0 else EXIT_STOP_LOSS_REL
            stop_price = max(EXIT_STOP_LOSS_ABS, pos.entry_price - stop_offset)
            # Horizon: per-event calibrated, fallback to EXIT_HORIZON_SEC
            event_horizon = EXIT_HORIZON_BY_EVENT.get(pos.event_type, EXIT_HORIZON_SEC)

            if exit_px is not None:
                if exit_px >= take_profit_price:
                    to_close.append((token_id, exit_px, "take_profit"))
                elif exit_px <= stop_price:
                    to_close.append((token_id, exit_px, "stop_loss"))
                elif event_horizon > 0 and age_sec >= event_horizon:
                    to_close.append((token_id, exit_px, "horizon"))
                elif pos.match_id in game_over_match_ids:
                    to_close.append((token_id, exit_px, "game_over"))
                elif age_sec >= max_hold_sec:
                    to_close.append((token_id, exit_px, "max_hold_timeout"))
            else:
                # No book data — force-close only when we must
                if pos.match_id in game_over_match_ids:
                    to_close.append((token_id, pos.entry_price, "game_over"))
                elif age_sec >= max_hold_sec:
                    to_close.append((token_id, pos.entry_price, "max_hold_timeout"))

        game_times = current_game_times or {}
        for token_id, exit_price, reason in to_close:
            pos = self.positions[token_id]
            exit_game_time = game_times.get(pos.match_id)
            cp = self._close_position(pos, exit_price, reason, exit_game_time)
            closed_now.append(cp)

        return closed_now

    def _close_position(
        self,
        pos: Position,
        exit_price: float,
        reason: str,
        exit_game_time: int | None,
    ) -> ClosedPosition:
        self.positions.pop(pos.token_id)
        self._match_open_usd[pos.match_id] = max(
            0.0, self._match_open_usd.get(pos.match_id, 0.0) - pos.cost_usd
        )
        proceeds = exit_price * pos.shares
        pnl = proceeds - pos.cost_usd
        hold_sec = (time.time_ns() - pos.entry_time_ns) / 1e9
        cp = ClosedPosition(
            token_id=pos.token_id,
            match_id=pos.match_id,
            market_name=pos.market_name,
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            cost_usd=pos.cost_usd,
            proceeds_usd=proceeds,
            pnl_usd=pnl,
            roi=pnl / pos.cost_usd if pos.cost_usd > 0 else 0,
            hold_sec=hold_sec,
            entry_game_time_sec=pos.entry_game_time_sec,
            exit_game_time_sec=exit_game_time,
            event_type=pos.event_type,
            lag=pos.lag,
            expected_move=pos.expected_move,
            exit_reason=reason,
            entry_time_ns=pos.entry_time_ns,
            exit_time_ns=time.time_ns(),
            fair_price=pos.fair_price,
        )
        self.closed.append(cp)
        return cp

    def summary(self) -> dict:
        if not self.closed:
            return {"trades": 0, "pnl_usd": 0.0, "win_rate": 0.0}
        wins = sum(1 for c in self.closed if c.pnl_usd > 0)
        return {
            "trades": len(self.closed),
            "open": len(self.positions),
            "pnl_usd": round(sum(c.pnl_usd for c in self.closed), 4),
            "win_rate": round(wins / len(self.closed), 3),
            "avg_hold_sec": round(sum(c.hold_sec for c in self.closed) / len(self.closed), 1),
        }
