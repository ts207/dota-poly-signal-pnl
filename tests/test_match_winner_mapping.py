# dota-poly-signal-pnl/tests/test_match_winner_mapping.py
import pytest
from mapping_validator import validate_mapping_schema

def test_match_winner_mapping_missing_fields():
    m = {"market_type": "MATCH_WINNER", "confidence": 1.0, "name": "Test Match"}
    result = validate_mapping_schema(m)
    assert any("missing" in err.lower() for err in result.mapping_errors)

def test_match_winner_mapping_invalid_state():
    m = {
        "market_type": "MATCH_WINNER",
        "series_type": 3,
        "current_game_number": 2,
        "series_score_yes": 0,
        "series_score_no": 0,
        "p_next_yes": 0.5,
        "confidence": 1.0,
        "name": "Test Match",
        "yes_team": "Team A",
        "no_team": "Team B",
        "yes_token_id": "1",
        "no_token_id": "2",
        "dota_match_id": "123"
    }
    result = validate_mapping_schema(m)
    assert any("invalid bo3 state" in err.lower() for err in result.mapping_errors)
