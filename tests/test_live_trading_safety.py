import pytest
import time
from live_executor import LiveExecutor

class FakeLiveClient:
    async def buy_fak_market(self, **kwargs):
        return {"success": True, "status": "matched"}

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
async def test_live_executor_respects_real_live_trading_flag(monkeypatch):
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", False)
    # Mock load_live_state to return clean state
    monkeypatch.setattr("live_executor.load_live_state", lambda: {"total_submitted_usd": 0.0, "total_filled_usd": 0.0, "open_positions": 0})
    
    executor = LiveExecutor(client=FakeLiveClient())
    
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=FakeBookStore()
    )
    
    assert attempt.order_status == "would_be_live_skipped", f"Rejected with: {attempt.reason_if_rejected}"
    assert attempt.reason_if_rejected == "ENABLE_REAL_LIVE_TRADING is false"

@pytest.mark.asyncio
async def test_live_executor_allows_real_live_trading_when_enabled(monkeypatch):
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    # Mock load_live_state to return clean state
    monkeypatch.setattr("live_executor.load_live_state", lambda: {"total_submitted_usd": 0.0, "total_filled_usd": 0.0, "open_positions": 0})
    
    executor = LiveExecutor(client=FakeLiveClient())
    
    attempt = await executor.try_buy(
        signal=_signal(), mapping=_mapping(), game=_game(), book_store=FakeBookStore()
    )
    
    assert attempt.order_status == "matched", f"Rejected with: {attempt.reason_if_rejected}"
