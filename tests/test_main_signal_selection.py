import pytest

from main import _best_signal_candidate, _yes_fair_from_radiant


def test_best_signal_candidate_prefers_executable_edge():
    low = {"signal": {"executable_edge": 0.04, "expected_move": 0.30}, "direction": "radiant", "events": []}
    high = {"signal": {"executable_edge": 0.06, "expected_move": 0.10}, "direction": "dire", "events": []}
    assert _best_signal_candidate([low, high]) is high


def test_best_signal_candidate_uses_expected_move_tiebreaker():
    a = {"signal": {"executable_edge": 0.05, "expected_move": 0.10}, "direction": "radiant", "events": []}
    b = {"signal": {"executable_edge": 0.05, "expected_move": 0.20}, "direction": "dire", "events": []}
    assert _best_signal_candidate([a, b]) is b


def test_best_signal_candidate_empty():
    assert _best_signal_candidate([]) is None


def test_yes_fair_uses_reversed_steam_side_mapping():
    mapping = {"steam_side_mapping": "reversed", "yes_team": "Team YES"}
    game = {"radiant_team": "Other", "dire_team": "Team YES"}
    fair, direction = _yes_fair_from_radiant(mapping, game, 0.70)
    assert fair == pytest.approx(0.30)
    assert direction == "dire"


def test_yes_fair_falls_back_to_team_names():
    mapping = {"yes_team": "Radiant Club"}
    game = {"radiant_team": "Radiant Club", "dire_team": "Dire Club"}
    assert _yes_fair_from_radiant(mapping, game, 0.62) == (0.62, "radiant")
