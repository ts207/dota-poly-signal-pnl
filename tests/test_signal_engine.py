import time

from signal_engine import EventSignalEngine, MIN_LAG, MIN_FILL_PRICE, DEFAULT_MAX_FILL_PRICE


TOKEN_YES = "YES_TOKEN"
TOKEN_NO  = "NO_TOKEN"


def _engine_with_price(token_id: str, price: float) -> EventSignalEngine:
    engine = EventSignalEngine()
    engine.record_price(token_id, price)
    return engine


def _game(now_ns: int, game_time_sec: int = 600) -> dict:
    return {
        "match_id": "M1",
        "received_at_ns": now_ns,
        "game_time_sec": game_time_sec,
        "radiant_team": "team a",
        "dire_team": "team b",
        "data_source": "top_live",
    }


def _mapping() -> dict:
    return {
        "market_type": "MAP_WINNER",
        "yes_team": "team a",
        "yes_token_id": TOKEN_YES,
        "no_token_id": TOKEN_NO,
        "confidence": 1.0,
    }


def _book(now_ns: int, ask: float = 0.46, bid: float = 0.44, size: float = 100) -> dict:
    return {
        "best_ask": ask,
        "best_bid": bid,
        "ask_size": size,
        "received_at_ns": now_ns,
    }


def test_fires_on_sufficient_lag():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["lag"] >= MIN_LAG


def test_skip_team_side_unknown():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    mapping = _mapping()
    mapping["yes_team"] = "team c"
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns), mapping, _book(now_ns), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "team_side_unknown"


def test_skip_fill_price_too_low():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.10)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns, ask=MIN_FILL_PRICE - 0.01, bid=0.08), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "fill_price_too_low"


def test_default_fill_cap_blocks_lower_priority_events():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.79)
    result = engine.evaluate(
        "T2_TOWER_FALL", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns, ask=DEFAULT_MAX_FILL_PRICE + 0.01, bid=0.79), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "fill_price_too_high"


def test_first_t4_has_event_specific_fill_cap_above_default():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.85)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns, game_time_sec=3300), _mapping(), _book(now_ns, ask=0.90, bid=0.88), None,
    )
    assert result["decision"] == "paper_buy_yes"


def test_first_t4_still_respects_event_specific_fill_cap():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.90)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns, ask=0.94, bid=0.92), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "fill_price_too_high"


def test_barracks_fall_is_inactive():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.89)
    result = engine.evaluate(
        "BARRACKS_FALL", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns, ask=0.91, bid=0.89), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "event_type_inactive"


def test_lead_swing_60s_fires_at_high_severity():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "LEAD_SWING_60S", "radiant", 6001,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="high",
    )
    assert result["decision"] == "paper_buy_yes"


def test_lead_swing_60s_skips_at_medium_severity():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "LEAD_SWING_60S", "radiant", 3001,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="medium",
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "severity_too_low"


def test_multitower_does_not_hit_adverse_move_filter():
    """Two towers falling (delta=2, expected_move=0.28) with no market move should fire."""
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 2,
        _game(now_ns), _mapping(), _book(now_ns), None,
    )
    # market_move=0, expected_move=0.28 → lag=0.28 > MIN_LAG — should fire, not skip
    assert result["decision"] == "paper_buy_yes"


def test_skip_game_too_early():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        _game(now_ns, game_time_sec=60), _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "game_too_early"


def test_skip_inactive_event_type():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "KILL_SWING", "radiant", 1,
        _game(now_ns), _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "event_type_inactive"


def test_skip_lead_swing_low_severity():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "LEAD_SWING_30S", "radiant", 1600,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="medium",
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "severity_too_low"


def test_lead_swing_high_severity_fires_when_lag_is_large_enough():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "LEAD_SWING_30S", "radiant", 6000,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="high",
    )
    assert result["decision"] == "paper_buy_yes"


def test_kill_confirmed_lead_swing_can_fire_at_medium_severity_when_lag_is_large_enough():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "KILL_CONFIRMED_LEAD_SWING", "radiant", 5000,
        _game(now_ns, game_time_sec=2400), _mapping(), _book(now_ns), None,
        severity="medium",
    )
    assert result["decision"] == "paper_buy_yes"


def test_teamfight_swing_can_fire_when_lag_is_large_enough():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "TEAMFIGHT_SWING_30S", "radiant", 2,
        _game(now_ns, game_time_sec=1200), _mapping(), _book(now_ns), None,
        severity="medium",
    )
    assert result["decision"] == "paper_buy_yes"


def test_kill_burst_medium_severity_is_too_small_standalone():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "KILL_BURST_30S", "radiant", 3,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="medium",
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "edge_too_small"


def test_high_severity_kill_burst_can_fire_when_lag_is_large_enough():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "KILL_BURST_30S", "radiant", 10,
        _game(now_ns, game_time_sec=1200), _mapping(), _book(now_ns), None,
        severity="high",
    )
    assert result["decision"] == "paper_buy_yes"


def test_confirmed_short_window_events_are_primary_for_clusters():
    from event_taxonomy import event_is_primary, event_tier

    assert event_tier("KILL_CONFIRMED_LEAD_SWING") == "B"
    assert event_is_primary("KILL_CONFIRMED_LEAD_SWING") is True
    assert event_tier("TEAMFIGHT_SWING_30S") == "B"
    assert event_is_primary("TEAMFIGHT_SWING_30S") is True
    assert event_tier("KILL_BURST_30S") == "B"
    assert event_is_primary("KILL_BURST_30S") is True
    assert event_tier("LEAD_SWING_30S") == "B"
    assert event_is_primary("LEAD_SWING_30S") is True
    assert event_tier("LEAD_SWING_60S") == "C"
    assert event_is_primary("LEAD_SWING_60S") is False


def test_cooldown_blocks_second_signal():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    kwargs = dict(
        event_type="FIRST_T4_TOWER_FALL", event_direction="radiant", event_delta=1,
        game=_game(now_ns), mapping=_mapping(),
        yes_book=_book(now_ns), no_book=None,
    )
    first = engine.evaluate(**kwargs)
    assert first["decision"] == "paper_buy_yes"
    engine.commit_signal(first)
    second = engine.evaluate(**kwargs)
    assert second["decision"] == "skip"
    assert second["reason"] == "cooldown"


def test_skip_non_top_live_source():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    g = _game(now_ns)
    g["data_source"] = "live_league"
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        g, _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "non_top_live_source"


def test_skip_stale_source_update_age():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    g = _game(now_ns)
    g["source_update_age_sec"] = 120
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        g, _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "source_update_stale"


def test_stream_delay_is_logged_not_skip_guard():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    g = _game(now_ns)
    g["stream_delay_s"] = 999
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        g, _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["stream_delay_s"] == 999


def test_team_side_matching_uses_shared_team_normalization():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    g = _game(now_ns)
    g["radiant_team"] = "Virtus.pro"
    m = _mapping()
    m["yes_team"] = "Virtus Pro"
    result = engine.evaluate(
        "FIRST_T4_TOWER_FALL", "radiant", 1,
        g, m, _book(now_ns), None,
    )
    assert result["decision"] == "paper_buy_yes"


def test_raw_t3_tower_only_is_damped_vs_objective_conversion():
    now_ns = time.time_ns()
    raw_engine = _engine_with_price(TOKEN_YES, 0.45)
    raw = raw_engine.evaluate(
        "T3_TOWER_FALL", "radiant", 1,
        _game(now_ns, game_time_sec=2100), _mapping(), _book(now_ns), None,
        severity="medium",
    )

    conversion_engine = _engine_with_price(TOKEN_YES, 0.45)
    converted = conversion_engine.evaluate(
        "OBJECTIVE_CONVERSION_T3", "radiant", 1,
        _game(now_ns, game_time_sec=2100), _mapping(), _book(now_ns), None,
        severity="high",
    )

    assert converted["decision"] == "paper_buy_yes"
    assert converted["expected_move"] > raw.get("expected_move", 0)


def test_small_standalone_event_fails_explicit_lag_gate():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    # COMEBACK at minute 10 with small delta produces a tiny expected_move.
    # Use a low enough ask price that edge passes but remaining_move < MIN_LAG.
    ask = 0.38
    bid = 0.36
    result = engine.evaluate(
        "COMEBACK", "radiant", 3001,
        _game(now_ns, game_time_sec=600), _mapping(), _book(now_ns, ask=ask, bid=bid), None,
        severity="medium",
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "lag_too_small"


def test_all_t2_context_suppresses_plain_t2_component():
    from signal_engine import apply_suppressions
    events = [
        {"event_type": "T2_TOWER_FALL", "direction": "radiant"},
        {"event_type": "ALL_T2_TOWERS_DOWN", "direction": "radiant"},
    ]
    kept = apply_suppressions(events)
    assert [e["event_type"] for e in kept] == ["ALL_T2_TOWERS_DOWN"]


def test_signal_includes_event_specific_max_fill_price():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.82)
    result = engine.evaluate(
        "OBJECTIVE_CONVERSION_T3", "radiant", 1,
        _game(now_ns, game_time_sec=2400), _mapping(), _book(now_ns, ask=0.84, bid=0.82), None,
        severity="high",
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["max_fill_price"] == 0.88


def test_throne_exposed_is_active_event():
    from signal_engine import ACTIVE_EVENTS
    assert "THRONE_EXPOSED" in ACTIVE_EVENTS


def test_throne_exposed_suppresses_second_t4():
    from signal_engine import apply_suppressions, SUPPRESSIONS
    assert "SECOND_T4_TOWER_FALL" in SUPPRESSIONS.get("THRONE_EXPOSED", set())


def test_throne_exposed_has_higher_state_cap():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "THRONE_EXPOSED", "radiant", 2,
        _game(now_ns, game_time_sec=2400), _mapping(), _book(now_ns), None,
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["max_fill_price"] >= 0.96


def test_all_t3_towers_down_is_active():
    from signal_engine import ACTIVE_EVENTS
    assert "ALL_T3_TOWERS_DOWN" in ACTIVE_EVENTS


def test_all_t3_suppresses_t3_tower():
    from signal_engine import apply_suppressions
    events = [
        {"event_type": "T3_TOWER_FALL", "direction": "radiant"},
        {"event_type": "ALL_T3_TOWERS_DOWN", "direction": "radiant"},
    ]
    kept = apply_suppressions(events)
    kept_types = [e["event_type"] for e in kept]
    assert "T3_TOWER_FALL" not in kept_types
    assert "ALL_T3_TOWERS_DOWN" in kept_types


def test_t3_plus_t4_chain_is_active():
    from signal_engine import ACTIVE_EVENTS
    assert "T3_PLUS_T4_CHAIN" in ACTIVE_EVENTS


def test_multi_structure_collapse_is_active():
    from signal_engine import ACTIVE_EVENTS
    assert "MULTI_STRUCTURE_COLLAPSE" in ACTIVE_EVENTS
