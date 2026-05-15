from reaction_lag import analyze_reaction_lag


def test_reaction_lag_expected_ask_move():
    events = [{
        "timestamp_utc": "2026-01-01T00:00:10.000+00:00",
        "yes_token_id": "YES",
        "mapping_name": "m",
        "event_type": "POLL_FIGHT_SWING",
        "severity": "medium",
        "game_time_sec": "600",
        "direction": "radiant",
        "yes_team": "Team A",
        "radiant_team": "Team A",
        "dire_team": "Team B",
    }]
    books = [
        {"timestamp_utc": "2026-01-01T00:00:09.000+00:00", "asset_id": "YES", "best_bid": "0.50", "best_ask": "0.52", "spread": "0.02", "ask_size": "100"},
        {"timestamp_utc": "2026-01-01T00:00:12.000+00:00", "asset_id": "YES", "best_bid": "0.52", "best_ask": "0.54", "spread": "0.02", "ask_size": "100"},
    ]
    rows = analyze_reaction_lag(events, books)
    assert len(rows) == 1
    assert rows[0]["favors_yes"] is True
    assert rows[0]["time_to_expected_ask_move_s"] == 2.0
