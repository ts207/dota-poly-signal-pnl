from __future__ import annotations

import os
import json
import pytest
import time
from live_executor import LiveExecutor
from live_state import LIVE_STATE_PATH, load_live_state, save_live_state

class FakeLiveClient:
    async def buy_fak_market(self, **kwargs):
        return {"success": True, "status": "matched", "filledAmountUsd": kwargs.get("amount_usd", 1.0)}

def _signal():
    return {
        "event_type": "POLL_ULTRA_LATE_FIGHT_FLIP",
        "cluster_event_types": "POLL_ULTRA_LATE_FIGHT_FLIP",
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
        "event_schema_version": "cadence_v1",
        "source_cadence_quality": "normal",
        "event_quality": 0.75,
    }

def _game():
    return {
        "match_id": "M1",
        "received_at_ns": time.time_ns(),
        "game_over": False,
        "radiant_team": "Team A",
        "dire_team": "Team B",
    }

def _mapping():
    return {
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

class FakeBookStore:
    def get(self, token_id):
        return {
            "best_ask": 0.61,
            "best_bid": 0.58,
            "received_at_ns": time.time_ns()
        }

@pytest.mark.asyncio
async def test_live_executor_persistence(monkeypatch, tmp_path):
    # Setup tmp state file
    state_file = tmp_path / "live_state.json"
    monkeypatch.setattr("live_state.LIVE_STATE_PATH", str(state_file))
    
    # Ensure starting from clean state
    if state_file.exists():
        state_file.unlink()

    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    
    executor = LiveExecutor(client=FakeLiveClient())
    assert executor.total_submitted_usd == 0.0
    assert executor.total_filled_usd == 0.0
    assert executor.open_positions == 0

    # Execute a trade
    await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=FakeBookStore()
    )

    assert executor.total_submitted_usd == 1.0
    assert executor.total_filled_usd == 1.0
    assert executor.open_positions == 1

    # Verify file was written
    assert state_file.exists()
    with open(state_file, "r") as f:
        data = json.load(f)
        assert data["total_submitted_usd"] == 1.0
        assert data["total_filled_usd"] == 1.0
        assert data["open_positions"] == 1

    # Create a new instance and verify it loads the state
    executor2 = LiveExecutor(client=FakeLiveClient())
    assert executor2.total_submitted_usd == 1.0
    assert executor2.total_filled_usd == 1.0
    assert executor2.open_positions == 1

@pytest.mark.asyncio
async def test_live_executor_budget_cap_persists(monkeypatch, tmp_path):
    state_file = tmp_path / "live_state.json"
    monkeypatch.setattr("live_state.LIVE_STATE_PATH", str(state_file))
    monkeypatch.setattr("live_executor.MAX_TOTAL_LIVE_USD", 10.0)
    
    # Save a state that already reached the cap
    save_live_state(total_submitted_usd=10.0, total_filled_usd=5.0, open_positions=5)
    
    executor = LiveExecutor(client=FakeLiveClient())
    assert executor.remaining_budget() == 0.0
    
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=FakeBookStore()
    )
    assert attempt.order_status == "rejected_precheck"
    assert attempt.reason_if_rejected == "max_total_live_usd_reached"
