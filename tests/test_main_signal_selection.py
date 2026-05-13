from main import _best_signal_candidate


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
