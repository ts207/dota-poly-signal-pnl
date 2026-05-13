import time

from steam_client import normalize_league_game


def test_live_league_stream_delay_does_not_age_received_timestamp():
    now_ns = time.time_ns()
    raw = {
        "match_id": "123",
        "lobby_id": "456",
        "stream_delay_s": 120,
        "_received_at_ns": now_ns,
        "scoreboard": {
            "duration": 1800,
            "radiant": {"score": 10, "players": [{"net_worth": 1000}]},
            "dire": {"score": 8, "players": [{"net_worth": 900}]},
        },
    }
    game = normalize_league_game(raw)
    assert game["received_at_ns"] == now_ns
    assert game["stream_delay_s"] == 120
    assert game["source_update_age_sec"] is None

import pytest
from steam_client import LeagueGameCache


@pytest.mark.asyncio
async def test_league_game_cache_avoids_refetch(monkeypatch):
    calls = []

    async def fake_fetch(session):
        calls.append(1)
        return [{"match_id": "1"}]

    monkeypatch.setattr("steam_client.fetch_live_league_games", fake_fetch)
    cache = LeagueGameCache(refresh_seconds=999)
    first = await cache.get(None)
    second = await cache.get(None)
    assert first == second == [{"match_id": "1"}]
    assert len(calls) == 1
