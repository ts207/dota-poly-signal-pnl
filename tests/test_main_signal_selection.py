import pytest
import time

from main import (
    _acquire_single_instance_lock, _best_signal_candidate, _exit_adverse_position_for_signal, _hybrid_context,
    _hybrid_delay_seconds, _yes_fair_from_radiant,
)
from paper_trader import PaperTrader, Position


class Store:
    def __init__(self, books):
        self.books = books

    def get(self, token_id):
        return self.books.get(token_id)


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


def test_single_instance_lock_rejects_second_holder(tmp_path):
    import main

    lock_path = tmp_path / "paper_bot.lock"
    assert _acquire_single_instance_lock(str(lock_path)) is True
    first_handle = main._LOCK_HANDLE
    assert _acquire_single_instance_lock(str(lock_path)) is False
    first_handle.close()
    main._LOCK_HANDLE = None


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


def test_hybrid_context_uses_realtime_not_liveleague():
    ctx = {"radiant_dead_count": 0, "aegis_team": "radiant"}
    game = {"radiant_dead_count": 2, "delayed_field_age_sec": 5}
    merged = _hybrid_context(game, ctx)
    assert merged["radiant_dead_count"] == 2
    assert "aegis_team" not in merged
    assert merged["delayed_field_age_sec"] == 5


def test_hybrid_delay_uses_realtime_game_time_when_available():
    game = {"game_time_sec": 1500, "realtime_game_time_sec": 1390, "game_time_lag_sec": 900}
    assert _hybrid_delay_seconds(game) == 110


def test_adverse_exit_runs_for_primary_opposing_signal_even_when_not_executable():
    trader = PaperTrader()
    trader.positions["NO"] = Position(
        token_id="NO", match_id="M1", market_name="Test", side="NO",
        entry_price=0.50, shares=50, cost_usd=25,
        entry_time_ns=time.time_ns(), entry_game_time_sec=1200,
        event_type="BASE_PRESSURE_T4", lag=0.1, expected_move=0.2,
    )
    trader._match_open_usd["M1"] = 25

    closed = _exit_adverse_position_for_signal(
        {
            "decision": "skip",
            "reason": "already_repriced",
            "event_is_primary": True,
            "token_id": "YES",
        },
        {"yes_token_id": "YES", "no_token_id": "NO"},
        trader,
        Store({"NO": {"best_bid": 0.44, "best_ask": 0.46}}),
    )

    assert closed is not None
    assert closed.exit_reason == "adverse_event"
    assert "NO" not in trader.positions
