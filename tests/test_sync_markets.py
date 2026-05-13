from sync_markets import choose_mapping_for_live_game, norm_team, sync_markets_to_games


def _market(name, yes="Team Falcons", no="Virtus.pro", match_id="STEAM_MATCH_OR_LOBBY_ID_HERE", confidence=0.0):
    return {
        "name": name,
        "yes_team": yes,
        "no_team": no,
        "dota_match_id": match_id,
        "confidence": confidence,
    }


def _game(match_id="M1", radiant="Team Falcons", dire="Virtus.pro"):
    return {
        "match_id": match_id,
        "lobby_id": "L1",
        "radiant_team": radiant,
        "dire_team": dire,
        "game_time_sec": 400,
        "data_source": "top_live",
    }


def test_norm_team_handles_common_aliases_and_suffixes():
    assert norm_team("NAVI") == norm_team("Natus Vincere")
    assert norm_team("Team Falcons") == norm_team("Falcons Esports")
    assert norm_team("Virtus.pro") == norm_team("Virtus Pro")


def test_choose_mapping_uses_lowest_inactive_game_number():
    markets = [
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 2 Winner"),
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 1 Winner"),
    ]
    market, reason = choose_mapping_for_live_game(markets, _game())
    assert reason == "matched"
    assert "Game 1" in market["name"]


def test_choose_mapping_skips_when_current_match_already_active():
    markets = [
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 1 Winner", match_id="M1", confidence=1.0),
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 2 Winner"),
    ]
    market, reason = choose_mapping_for_live_game(markets, _game())
    assert market is None
    assert reason == "already_mapped_current_match"


def test_sync_updates_one_market_for_live_match():
    markets = [
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 1 Winner"),
        _market("Dota 2: Team Falcons vs Virtus.pro - Game 2 Winner"),
    ]
    updates = sync_markets_to_games(markets, [_game(match_id="M9")])
    assert len(updates) == 1
    assert markets[0]["dota_match_id"] == "M9"
    assert markets[0]["confidence"] == 1.0
    assert markets[1]["dota_match_id"] == "STEAM_MATCH_OR_LOBBY_ID_HERE"
