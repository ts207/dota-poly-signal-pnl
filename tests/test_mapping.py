from mapping import validate_mapping


def test_valid_map_winner_mapping():
    mapping = {
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "yes_token_id": "123",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    ok, err = validate_mapping(mapping)
    assert ok
    assert err is None


def test_rejects_series_without_model():
    mapping = {
        "market_type": "SERIES_WINNER",
        "yes_team": "Team A",
        "yes_token_id": "123",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    ok, err = validate_mapping(mapping)
    assert not ok
    assert "unsupported" in err.reason


def test_rejects_placeholder_token():
    mapping = {
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "yes_token_id": "YES_TOKEN_ID_HERE",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    ok, err = validate_mapping(mapping)
    assert not ok
    assert "placeholder" in err.reason
