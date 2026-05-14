import time

from steam_client import decode_top_live_tower_state, normalize_league_game, normalize_top_live


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


def test_top_live_building_state_decodes_lane_towers_only():
    initial = 0x490049
    decoded = decode_top_live_tower_state(initial)

    assert decoded == (1 << 22) - 1

    top_t1_down = (initial & ~(1 << 16)) | (1 << 17)
    top_t2_down = (top_t1_down & ~(1 << 17)) | (1 << 18)
    prev = decode_top_live_tower_state(top_t1_down)
    cur = decode_top_live_tower_state(top_t2_down)

    assert prev & (1 << (11 + 1))
    assert not cur & (1 << (11 + 1))
    assert cur & (1 << (11 + 2))
    assert cur & (1 << (11 + 9))
    assert cur & (1 << (11 + 10))


def test_normalize_top_live_sets_decoded_tower_state():
    game = normalize_top_live(
        {
            "match_id": "123",
            "game_time": 100,
            "radiant_lead": 0,
            "building_state": 0x490049,
        },
        time.time_ns(),
    )

    assert game["building_state"] == 0x490049
    assert game["tower_state"] == (1 << 22) - 1

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
