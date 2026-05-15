import time
import csv

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
        "event_type": "BASE_PRESSURE_T4",
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
        entry_game_time_sec=1200, event_type="BASE_PRESSURE_T4", lag=0.1, expected_move=0.2,
    )
    trader._match_open_usd["M1"] = 25
    store = Store({"YES": {"best_bid": 0.60, "best_ask": 0.80}})

    closed = trader.force_exit("YES", store, "test")

    assert closed is not None
    assert closed.exit_price == pytest.approx(0.60)
    assert closed.pnl_usd == pytest.approx(5.0)


def test_reentry_cooldown_blocks_immediate_rebuy_after_exit():
    trader = PaperTrader()
    store = Store({"YES": {"best_ask": 0.50, "best_bid": 0.48, "ask_size": 100}})

    pos, reason = trader.enter(
        signal=_signal(event_type="ML_ARBITRAGE", fair_price=0.80, expected_move=0.20),
        token_id="YES", side="YES", book_store=store,
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )
    assert pos is not None, reason

    closed = trader.force_exit("YES", store, "stop_loss")
    assert closed is not None

    rebuy, reason = trader.enter(
        signal=_signal(event_type="ML_ARBITRAGE", fair_price=0.80, expected_move=0.20),
        token_id="YES", side="YES", book_store=store,
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )
    assert rebuy is None
    assert reason.startswith("reentry_cooldown")


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


def test_dynamic_model_fair_can_trigger_value_exit():
    trader = PaperTrader()
    pos, reason = trader.enter(
        signal=_signal(ask=0.50, fair_price=0.70, expected_move=0.20),
        token_id="YES", side="YES",
        book_store=Store({"YES": {"best_ask": 0.50, "best_bid": 0.48, "ask_size": 100}}),
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )
    assert pos is not None, reason

    trader.update_fair_value("YES", 0.49)
    closed = trader.check_exits(Store({"YES": {"best_bid": 0.49, "best_ask": 0.51}}), set())

    assert len(closed) == 1
    assert closed[0].exit_reason == "model_value_exit"


def test_latency_edge_timeout_exits_after_average_edge_window(monkeypatch):
    monkeypatch.setattr("paper_trader.EXIT_LATENCY_EDGE_SEC", 1)
    trader = PaperTrader()
    pos, reason = trader.enter(
        signal=_signal(ask=0.50, fair_price=0.70, expected_move=0.20),
        token_id="YES", side="YES",
        book_store=Store({"YES": {"best_ask": 0.50, "best_bid": 0.48, "ask_size": 100}}),
        match_id="M1", market_name="Test", opposing_token_id="NO",
    )
    assert pos is not None, reason
    pos.entry_time_ns = time.time_ns() - 2_000_000_000

    closed = trader.check_exits(Store({"YES": {"best_bid": 0.52, "best_ask": 0.54}}), set())

    assert len(closed) == 1
    assert closed[0].exit_reason == "latency_edge_timeout"


def test_load_open_positions_replays_trade_csv(tmp_path):
    path = tmp_path / "paper_trades.csv"
    headers = [
        "timestamp_utc", "action", "token_id", "match_id", "market_name", "side",
        "entry_price", "shares", "cost_usd", "event_type", "lag", "expected_move",
        "entry_game_time_sec", "exit_price", "proceeds_usd", "pnl_usd", "roi",
        "hold_sec", "exit_game_time_sec", "exit_reason",
    ]
    rows = [
        {
            "timestamp_utc": "2026-05-14T18:52:51.576+00:00",
            "action": "entry",
            "token_id": "OPEN",
            "match_id": "M1",
            "market_name": "Test",
            "side": "NO",
            "entry_price": "0.39",
            "shares": "142.05128205128204",
            "cost_usd": "55.4",
            "event_type": "POLL_COMEBACK_RECOVERY",
            "lag": "0.1258",
            "expected_move": "0.1308",
            "entry_game_time_sec": "1447",
        },
        {
            "timestamp_utc": "2026-05-14T18:53:01.000+00:00",
            "action": "entry",
            "token_id": "CLOSED",
            "match_id": "M2",
            "market_name": "Other",
            "side": "YES",
            "entry_price": "0.50",
            "shares": "20",
            "cost_usd": "10",
            "event_type": "POLL_FIGHT_SWING",
            "lag": "0.1",
            "expected_move": "0.2",
            "entry_game_time_sec": "1200",
        },
        {"timestamp_utc": "2026-05-14T18:53:05.000+00:00", "action": "exit", "token_id": "CLOSED"},
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    trader = PaperTrader()
    restored = trader.load_open_positions(str(path))

    assert restored == 1
    assert set(trader.positions) == {"OPEN"}
    pos = trader.positions["OPEN"]
    assert pos.match_id == "M1"
    assert pos.side == "NO"
    assert pos.entry_price == pytest.approx(0.39)
    assert pos.entry_game_time_sec == 1447
    assert trader._match_open_usd["M1"] == pytest.approx(55.4)
