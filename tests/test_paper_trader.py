import time

import pytest

from paper_trader import PaperTrader, Position


class Store:
    def __init__(self, books):
        self.books = books

    def get(self, token_id):
        return self.books.get(token_id)


def _signal(**overrides):
    data = {
        "ask": 0.50,
        "bid": 0.48,
        "fair_price": 0.70,
        "target_size_usd": 25,
        "game_time_sec": 1200,
        "event_type": "FIRST_T4_TOWER_FALL",
        "lag": 0.15,
        "expected_move": 0.22,
    }
    data.update(overrides)
    return data


def test_paper_entry_fills_at_ask_not_mid():
    trader = PaperTrader()
    store = Store({"YES": {"best_ask": 0.50, "best_bid": 0.40, "ask_size": 100}})

    pos, reason = trader.enter(
        signal=_signal(), token_id="YES", side="YES", book_store=store,
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )

    assert reason == "filled"
    assert pos is not None
    assert pos.entry_price == pytest.approx(0.50)
    assert pos.shares == pytest.approx(50.0)


def test_paper_entry_rejects_when_ask_moves_above_limit():
    trader = PaperTrader()
    store = Store({"YES": {"best_ask": 0.53, "best_bid": 0.40, "ask_size": 100}})

    pos, reason = trader.enter(
        signal=_signal(ask=0.50, fair_price=0.70), token_id="YES", side="YES", book_store=store,
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )

    assert pos is None
    assert reason.startswith("ask_moved_above_limit")


def test_force_exit_sells_at_bid_not_mid():
    trader = PaperTrader()
    trader.positions["YES"] = Position(
        token_id="YES", match_id="M1", market_name="Test", side="YES",
        entry_price=0.50, shares=50, cost_usd=25, entry_time_ns=time.time_ns(),
        entry_game_time_sec=1200, event_type="FIRST_T4_TOWER_FALL", lag=0.1, expected_move=0.2,
    )
    trader._match_open_usd["M1"] = 25
    store = Store({"YES": {"best_bid": 0.60, "best_ask": 0.80}})

    closed = trader.force_exit("YES", store, "test")

    assert closed is not None
    assert closed.exit_price == pytest.approx(0.60)
    assert closed.pnl_usd == pytest.approx(5.0)


def test_take_profit_uses_fair_price_not_entry_plus_expected_move():
    trader = PaperTrader()
    pos, reason = trader.enter(
        signal=_signal(ask=0.56, fair_price=0.67, expected_move=0.22),
        token_id="YES", side="YES",
        book_store=Store({"YES": {"best_ask": 0.56, "best_bid": 0.54, "ask_size": 100}}),
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )
    assert reason == "filled"
    # Bid reaches model fair price (0.67) but not entry+expected_move (0.78).
    closed = trader.check_exits(Store({"YES": {"best_bid": 0.67, "best_ask": 0.69}}), set())
    assert len(closed) == 1
    assert closed[0].exit_reason == "take_profit"
