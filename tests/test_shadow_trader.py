from shadow_trader import build_shadow_trade


def test_build_shadow_trade():
    s = build_shadow_trade(
        signal={
            "event_type": "POLL_FIGHT_SWING",
            "event_tier": "B",
            "event_family": "fight",
            "token_id": "tok",
            "side": "YES",
            "decision": "skip",
            "reason": "edge_too_small",
            "ask": 0.51,
            "bid": 0.49,
            "executable_price": 0.52,
            "fair_price": 0.6,
            "executable_edge": 0.08,
            "lag": 0.1,
        },
        mapping={"market_type": "MAP_WINNER", "name": "M"},
        game={"match_id": "123", "game_time_sec": 1800},
        token_id="tok",
        side="YES",
    )
    assert s.entry_price == 0.52
    assert abs(s.spread_at_entry - 0.02) < 1e-9
