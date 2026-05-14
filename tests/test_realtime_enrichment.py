import asyncio

import realtime_enrichment
from realtime_enrichment import clear_cache, maybe_enrich_realtime, parse_player_net_worth


def test_realtime_stats_parser_keeps_delayed_net_worth_separate():
    parsed = parse_player_net_worth({
        "result": {
            "teams": [
                {"players": [
                    {"net_worth": 1000, "hero_id": 1, "level": 10, "respawn_timer": 0},
                    {"net_worth": 2000, "hero_id": 2, "level": 12, "respawn_timer": 55},
                ]},
                {"players": [
                    {"net_worth": 1500, "hero_id": 3, "level": 11, "respawn_timer": 0},
                    {"net_worth": 500, "hero_id": 4, "level": 9, "respawn_timer": 0},
                ]},
            ],
            "game_time": 1234,
        },
    })

    assert parsed["realtime_game_time_sec"] == 1234
    assert parsed["realtime_lead_nw"] == 1000
    assert parsed["delayed_net_worth_diff"] == 1000
    assert parsed["delayed_radiant_net_worth"] == 3000
    assert parsed["delayed_dire_net_worth"] == 2000
    assert "radiant_net_worth" not in parsed
    assert "dire_net_worth" not in parsed
    assert parsed["radiant_p1_net_worth"] == 1000
    assert parsed["radiant_p1_hero_id"] == 1
    assert parsed["radiant_p1_level"] == 10
    assert parsed["radiant_level"] == 22
    assert parsed["dire_level"] == 20
    assert parsed["radiant_dead_count"] == 1
    assert parsed["radiant_core_dead_count"] == 1


def test_realtime_stats_parser_uses_explicit_team_side_before_array_order():
    parsed = parse_player_net_worth({
        "result": {
            "teams": [
                {"team_number": 3, "players": [{"net_worth": 4000, "hero_id": 4, "level": 12}]},
                {"team_number": 2, "players": [{"net_worth": 7000, "hero_id": 7, "level": 15}]},
            ],
            "duration": 900,
        },
    })

    assert parsed["realtime_game_time_sec"] == 900
    assert parsed["realtime_lead_nw"] == 3000
    assert parsed["radiant_p1_hero_id"] == 7
    assert parsed["dire_p1_hero_id"] == 4


def test_fresh_realtime_enrichment_age_is_not_negative(monkeypatch):
    async def fake_fetch(session, server_steam_id):
        return {
            "result": {
                "game_time": 120,
                "teams": [
                    {"players": [{"net_worth": 1000}]},
                    {"players": [{"net_worth": 900}]},
                ],
            },
        }, 9999999999.0

    clear_cache()
    monkeypatch.setattr(realtime_enrichment, "REALTIME_STATS_ENABLED", True)
    monkeypatch.setattr(realtime_enrichment, "_fetch_realtime_stats", fake_fetch)
    game = {"server_steam_id": "server1"}

    asyncio.run(maybe_enrich_realtime(game, session=object()))

    assert game["realtime_stats_age_sec"] == 0.0
    assert game["delayed_field_age_sec"] == 0.0
