from __future__ import annotations

import time

import pytest

from live_executor import LiveExecutor, round_down_to_tick
from poly_ws import BookStore


class FakeLiveClient:
    def __init__(self):
        self.calls = []

    async def buy_fak_market(self, **kwargs):
        self.calls.append(kwargs)
        return {"success": True, "status": "matched", "avgFillPrice": kwargs["price_cap"]}


def _signal(**overrides):
    base = {
        "event_type": "ULTRA_LATE_WIPE",
        "cluster_event_types": "ULTRA_LATE_WIPE",
        "event_direction": "radiant",
        "token_id": "TOKYES",
        "side": "YES",
        "fair_price": 0.72,
        "ask": 0.61,
        "executable_edge": 0.09,
        "lag": 0.09,
        "spread": 0.03,
        "book_age_ms": 100,
        "steam_age_ms": 100,
    }
    base.update(overrides)
    return base


def _game():
    return {
        "match_id": "M1",
        "received_at_ns": time.time_ns(),
        "game_over": False,
        "radiant_team": "Team A",
        "dire_team": "Team B",
    }


def _mapping(**overrides):
    base = {
        "name": "Team A vs Team B Game 1",
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "no_team": "Team B",
        "yes_token_id": "TOKYES",
        "no_token_id": "TOKNO",
        "dota_match_id": "M1",
        "confidence": 1.0,
        "tick_size": "0.01",
        "neg_risk": False,
    }
    base.update(overrides)
    return base


def _book_store(ask=0.61, bid=0.58):
    store = BookStore()
    store.update_direct("TOKYES", best_ask=ask, best_bid=bid, ask_size=100, bid_size=100)
    return store


def test_round_down_to_tick():
    assert round_down_to_tick(0.6789, "0.01") == 0.67
    assert round_down_to_tick(0.6789, "0.001") == 0.678


@pytest.mark.asyncio
async def test_live_executor_sends_capped_fak_buy():
    client = FakeLiveClient()
    executor = LiveExecutor(client=client)
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=_book_store()
    )
    assert attempt.order_status == "matched"
    assert attempt.submitted_size_usd == 1
    assert attempt.filled_size_usd == 1
    assert client.calls[0]["amount_usd"] == 1
    assert client.calls[0]["price_cap"] == 0.70  # fair 0.72 - safety 0.02, rounded
    assert client.calls[0]["tick_size"] == "0.01"


@pytest.mark.asyncio
async def test_live_executor_rejects_tower_trade_when_disabled(monkeypatch):
    monkeypatch.setattr("live_executor.DISABLE_STRUCTURE_TRADES", True)
    executor = LiveExecutor(client=FakeLiveClient())
    attempt = await executor.try_buy(
        signal=_signal(event_type="T3_TOWER_FALL", cluster_event_types="T3_TOWER_FALL"),
        mapping=_mapping(), game=_game(), book_store=_book_store(),
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "structure_trade_disabled"


@pytest.mark.asyncio
async def test_live_executor_rejects_if_ask_above_price_cap():
    executor = LiveExecutor(client=FakeLiveClient())
    # price cap is 0.70; ask 0.705 rounds outside acceptable cap
    attempt = await executor.try_buy(
        signal=_signal(fair_price=0.72, executable_edge=0.09, lag=0.09),
        mapping=_mapping(), game=_game(), book_store=_book_store(ask=0.71, bid=0.69),
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected in {"best_ask_above_price_cap", "fresh_edge_too_small"}


@pytest.mark.asyncio
async def test_live_executor_budget_caps_after_ten_attempts(monkeypatch):
    monkeypatch.setattr("live_executor.MAX_OPEN_POSITIONS", 999)
    client = FakeLiveClient()
    executor = LiveExecutor(client=client)
    for _ in range(10):
        attempt = await executor.try_buy(
            signal=_signal(), mapping=_mapping(), game=_game(), book_store=_book_store()
        )
        assert attempt.submitted_size_usd == 1
        # For this budget-specific test, treat fills as closed so the open-position
        # guard does not stop before the max-total-spend guard.
        executor.open_positions = 0
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=_book_store()
    )
    assert attempt.reason_if_rejected == "max_total_live_usd_reached"


@pytest.mark.asyncio
async def test_live_executor_uses_event_specific_max_fill_above_default():
    client = FakeLiveClient()
    executor = LiveExecutor(client=client)
    sig = _signal(
        event_type="OBJECTIVE_CONVERSION_T3",
        cluster_event_types="OBJECTIVE_CONVERSION_T3",
        fair_price=0.88,
        executable_edge=0.08,
        lag=0.08,
        max_fill_price=0.88,
    )
    attempt = await executor.try_buy(
        signal=sig, mapping=_mapping(), game=_game(), book_store=_book_store(ask=0.82, bid=0.80)
    )
    assert attempt.order_status == "matched"
    assert client.calls[0]["price_cap"] == 0.86


@pytest.mark.asyncio
async def test_live_executor_rejects_above_event_max_fill():
    executor = LiveExecutor(client=FakeLiveClient())
    sig = _signal(
        event_type="OBJECTIVE_CONVERSION_T3",
        cluster_event_types="OBJECTIVE_CONVERSION_T3",
        fair_price=0.95,
        executable_edge=0.10,
        lag=0.10,
        max_fill_price=0.88,
    )
    attempt = await executor.try_buy(
        signal=sig, mapping=_mapping(), game=_game(), book_store=_book_store(ask=0.89, bid=0.87)
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "ask_above_event_max_fill"


@pytest.mark.asyncio
async def test_live_executor_rejects_mapping_confidence_below_one():
    executor = LiveExecutor(client=FakeLiveClient())
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(confidence=0.99), game=_game(), book_store=_book_store()
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected.startswith("mapping_invalid:")


@pytest.mark.asyncio
async def test_live_executor_rejects_tier_c_by_default(monkeypatch):
    monkeypatch.setattr("live_executor.TRADE_EVENTS", {"T2_TOWER_FALL"})
    monkeypatch.setattr("live_executor.ALLOW_CONFIRMATION_ONLY_LIVE_TRADES", False)
    executor = LiveExecutor(client=FakeLiveClient())
    attempt = await executor.try_buy(
        signal=_signal(event_type="T2_TOWER_FALL", cluster_event_types="T2_TOWER_FALL"),
        mapping=_mapping(),
        game=_game(),
        book_store=_book_store(),
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "confirmation_only_event"


@pytest.mark.asyncio
async def test_live_executor_rejects_priced_objective_conversion_t3_without_large_edge(monkeypatch):
    monkeypatch.setattr("live_executor.TRADE_EVENTS", {"OBJECTIVE_CONVERSION_T3"})
    executor = LiveExecutor(client=FakeLiveClient())
    sig = _signal(
        event_type="OBJECTIVE_CONVERSION_T3",
        cluster_event_types="OBJECTIVE_CONVERSION_T3",
        ask=0.87,
        fair_price=0.93,
        executable_edge=0.06,
        lag=0.08,
        max_fill_price=0.90,
    )
    attempt = await executor.try_buy(
        signal=sig, mapping=_mapping(), game=_game(), book_store=_book_store(ask=0.87, bid=0.85)
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "objective_conversion_t3_requires_8c_edge_above_85c"


@pytest.mark.asyncio
async def test_live_executor_rejects_terminal_price_chasing_before_submit(monkeypatch):
    monkeypatch.setattr("live_executor.TRADE_EVENTS", {"OBJECTIVE_CONVERSION_T4"})
    executor = LiveExecutor(client=FakeLiveClient())
    sig = _signal(
        event_type="OBJECTIVE_CONVERSION_T4",
        cluster_event_types="OBJECTIVE_CONVERSION_T4",
        ask=0.96,
        fair_price=0.99,
        executable_edge=0.10,
        lag=0.08,
        max_fill_price=0.98,
    )
    attempt = await executor.try_buy(
        signal=sig, mapping=_mapping(), game=_game(), book_store=_book_store(ask=0.96, bid=0.94)
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "chasing_terminal_price"


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", [
    "LOW_PRICE_UNDERDOG_COUNTERPUNCH",
    "LATE_CHEAP_LEAD_SWING_REPRICE",
])
async def test_live_executor_rejects_research_events_even_if_allowlisted(monkeypatch, event_type):
    monkeypatch.setattr("live_executor.TRADE_EVENTS", {event_type})
    executor = LiveExecutor(client=FakeLiveClient())
    attempt = await executor.try_buy(
        signal=_signal(event_type=event_type, cluster_event_types=event_type),
        mapping=_mapping(),
        game=_game(),
        book_store=_book_store(),
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "research_event_not_live_tradable"
