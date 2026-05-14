# dota-poly-signal-pnl/tests/test_series_model.py
import pytest
from series_model import compute_bo3_match_p

def test_bo3_states():
    # p = 0.6, q = 0.5. Uses default series_type=1
    assert compute_bo3_match_p(0.6, 0.5, 0, 0, 1) == pytest.approx(0.55)
    assert compute_bo3_match_p(0.6, 0.5, 1, 0, 2, series_type=1) == pytest.approx(0.8)
    assert compute_bo3_match_p(0.6, 0.5, 0, 1, 2, series_type=1) == pytest.approx(0.3)
    assert compute_bo3_match_p(0.6, 0.5, 1, 1, 3, series_type=1) == pytest.approx(0.6)

def test_bo3_invalid_series_type():
    with pytest.raises(ValueError, match="Only series_type=1"):
        compute_bo3_match_p(0.6, 0.5, 0, 0, 1, series_type=3)

def test_bo3_invalid_states():
    with pytest.raises(ValueError, match="Invalid BO3 state"):
        compute_bo3_match_p(0.6, 0.5, 0, 0, 2) # Game 2 but 0-0
