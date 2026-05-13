from mapping import load_valid_mappings, validate_mapping
from mapping_validator import validate_mapping_identity


def test_valid_map_winner_mapping():
    mapping = {
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "no_team": "Team B",
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
        "no_team": "Team B",
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
        "no_team": "Team B",
        "yes_token_id": "YES_TOKEN_ID_HERE",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    ok, err = validate_mapping(mapping)
    assert not ok
    assert "placeholder" in err.reason


def test_rejects_duplicate_match_id_across_game_markets(tmp_path):
    path = tmp_path / "markets.yaml"
    path.write_text(
        """
markets:
  - name: Team A vs Team B Game 1
    market_type: MAP_WINNER
    yes_team: Team A
    no_team: Team B
    yes_token_id: y1
    no_token_id: n1
    dota_match_id: same
    confidence: 1.0
  - name: Team A vs Team B Game 2
    market_type: MAP_WINNER
    yes_team: Team A
    no_team: Team B
    yes_token_id: y2
    no_token_id: n2
    dota_match_id: same
    confidence: 1.0
""",
        encoding="utf-8",
    )
    valid, errors = load_valid_mappings(str(path))
    assert valid == []
    assert any("duplicate dota_match_id" in err.reason for err in errors)


def test_identity_rejects_team_id_mismatch():
    mapping = {
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "no_team": "Team B",
        "yes_team_id": 10,
        "no_team_id": 20,
        "yes_token_id": "123",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    game = {
        "match_id": "789",
        "radiant_team": "Team A",
        "dire_team": "Team B",
        "radiant_team_id": 99,
        "dire_team_id": 20,
    }
    result = validate_mapping_identity(mapping, game)
    assert not result.ok
    assert "team_id_mismatch" in result.mapping_errors


def test_identity_rejects_league_and_series_mismatch():
    mapping = {
        "name": "Team A vs Team B Game 4",
        "market_type": "MAP_WINNER",
        "yes_team": "Team A",
        "no_team": "Team B",
        "yes_token_id": "123",
        "no_token_id": "456",
        "dota_match_id": "789",
        "league_id": "1",
        "series_id": "abc",
        "confidence": 1.0,
    }
    game = {
        "match_id": "789",
        "radiant_team": "Team A",
        "dire_team": "Team B",
        "league_id": "2",
        "liveleague_context": {"series_id": "def", "series_type": 1},
    }
    result = validate_mapping_identity(mapping, game)
    assert not result.ok
    assert any("league_id_mismatch" in err for err in result.mapping_errors)
    assert any("series_id_mismatch" in err for err in result.mapping_errors)
    assert any("game_number=4" in err for err in result.mapping_errors)


def test_identity_rejects_yes_no_direction_ambiguity():
    mapping = {
        "market_type": "MAP_WINNER",
        "yes_team": "Team X",
        "no_team": "Team Y",
        "yes_token_id": "123",
        "no_token_id": "456",
        "dota_match_id": "789",
        "confidence": 1.0,
    }
    game = {"match_id": "789", "radiant_team": "Team A", "dire_team": "Team B"}
    result = validate_mapping_identity(mapping, game)
    assert not result.ok
    assert any("team_name_mismatch" in err for err in result.mapping_errors)
