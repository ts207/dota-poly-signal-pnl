from market_scope import is_game3_match_proxy, is_active_strategy_mapping


def test_map_winner_active():
    assert is_active_strategy_mapping(
        {"market_type": "MAP_WINNER"},
        enable_match_winner_game3_proxy=True,
    )


def test_match_winner_game3_proxy_active():
    m = {
        "market_type": "MATCH_WINNER",
        "series_type": 1,
        "game_number": 3,
        "series_score_yes": 1,
        "series_score_no": 1,
    }
    assert is_game3_match_proxy(m)
    assert is_active_strategy_mapping(m, enable_match_winner_game3_proxy=True)


def test_match_winner_game2_disabled():
    m = {
        "market_type": "MATCH_WINNER",
        "series_type": 1,
        "game_number": 2,
        "series_score_yes": 1,
        "series_score_no": 0,
    }
    assert not is_game3_match_proxy(m)
    assert not is_active_strategy_mapping(m, enable_match_winner_game3_proxy=True)
