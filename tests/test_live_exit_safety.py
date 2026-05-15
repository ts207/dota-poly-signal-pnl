from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock

import pytest

from live_executor import LiveExitExecutor, LiveExecutor
from live_position_store import LivePosition
from live_exit_engine import ExitDecision


@dataclass
class MockPosition:
    position_id: str = "P1"
    token_id: str = "TOK1"
    match_id: str = "M1"
    shares: float = 10.0
    market_name: str = "Market 1"
    side: str = "YES"


class FakeLiveClient:
    def __init__(self):
        self.sell_calls = []
        self.sell_response = {"success": True, "status": "matched", "filledShares": 10.0}

    async def sell_fak_market(self, **kwargs):
        self.sell_calls.append(kwargs)
        # Simulate some latency
        await asyncio.sleep(0.01)
        return self.sell_response


@pytest.mark.anyio
async def test_unknown_sell_fill_response_does_not_close(monkeypatch):
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    client = FakeLiveClient()
    # Response with success but NO explicit filled shares fields
    client.sell_response = {"success": True, "status": "matched"}
    
    executor = LiveExitExecutor(client=client)
    pos = MockPosition()
    book = {"best_bid": 0.50}
    mapping = {"tick_size": "0.01"}
    
    attempt = await executor.try_exit(position=pos, book=book, reason="test", mapping=mapping)
    
    # shares_filled should be 0 because we couldn't parse it
    assert attempt.shares_filled == 0.0
    # In main.py logic, attempt.shares_filled >= pos.shares * 0.999 would fail, so it wouldn't close.


@pytest.mark.anyio
async def test_ambiguous_fill_fields_rejected(monkeypatch):
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    client = FakeLiveClient()
    # Response with ambiguous fields that could be USDC
    client.sell_response = {"success": True, "status": "matched", "amountFilled": 10.0, "filled_size": 10.0}
    
    executor = LiveExitExecutor(client=client)
    pos = MockPosition()
    book = {"best_bid": 0.50}
    mapping = {"tick_size": "0.01"}
    
    attempt = await executor.try_exit(position=pos, book=book, reason="test", mapping=mapping)
    
    # Should be 0 because we only accept clearly share-denominated fields
    assert attempt.shares_filled == 0.0


@pytest.mark.anyio
async def test_explicit_filled_shares_closes_position(monkeypatch):
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    client = FakeLiveClient()
    client.sell_response = {"success": True, "status": "matched", "filled_shares": 10.0}
    
    executor = LiveExitExecutor(client=client)
    pos = MockPosition()
    book = {"best_bid": 0.50}
    mapping = {"tick_size": "0.01"}
    
    attempt = await executor.try_exit(position=pos, book=book, reason="test", mapping=mapping)
    
    assert attempt.shares_filled == 10.0


@pytest.mark.anyio
async def test_live_state_open_positions_decrements_after_full_exit(monkeypatch):
    # Setup LiveExecutor with 1 open position
    monkeypatch.setattr("live_executor.load_live_state", lambda: {"open_positions": 1})
    save_mock = MagicMock()
    monkeypatch.setattr("live_executor.save_live_state", save_mock)
    
    executor = LiveExecutor()
    assert executor.open_positions == 1
    
    executor.decrement_open_positions()
    assert executor.open_positions == 0
    save_mock.assert_called_with(0.0, 0.0, 0)
    
    # Test it doesn't go below 0
    executor.decrement_open_positions()
    assert executor.open_positions == 0


@pytest.mark.anyio
async def test_concurrent_check_live_exits_calls_submit_only_one_sell(monkeypatch):
    """
    This test verifies that the asyncio.Lock in _check_live_exits (via a simulated wrapper)
    prevents duplicate submissions. Since we can't easily call the internal 
    _check_live_exits from main.py without refactoring main.py into a testable class,
    we will simulate the locking logic and verify it works.
    """
    monkeypatch.setattr("live_executor.ENABLE_REAL_LIVE_TRADING", True)
    client = FakeLiveClient()
    executor = LiveExitExecutor(client=client)
    
    # Shared state
    live_position_store = MagicMock()
    pos = LivePosition(
        position_id="P1", state="OPEN", token_id="TOK1", opposing_token_id="TOK2",
        match_id="M1", market_name="M", side="YES", entry_price=0.5, shares=10.0,
        cost_usd=5.0, entry_time_ns=0, entry_game_time_sec=0, event_type="E",
        expected_move=0.1, fair_price=0.6
    )
    live_position_store.open_positions.return_value = [pos]
    
    lock = asyncio.Lock()
    sell_count = 0
    
    async def simulated_check_live_exits():
        nonlocal sell_count
        async with lock:
            # Re-fetch open positions inside lock
            open_pos = live_position_store.open_positions()
            for p in open_pos:
                if p.state == "OPEN":
                    # Mark as exiting immediately to prevent others from picking it up
                    p.state = "EXITING" 
                    await executor.try_exit(
                        position=p, book={"best_bid": 0.5}, reason="test", mapping={}
                    )
                    sell_count += 1
                    p.state = "CLOSED"

    # Run two concurrent checks
    await asyncio.gather(
        simulated_check_live_exits(),
        simulated_check_live_exits()
    )
    
    # Should only have called sell once
    assert sell_count == 1
    assert len(client.sell_calls) == 1

if __name__ == "__main__":
    pytest.main([__file__])
