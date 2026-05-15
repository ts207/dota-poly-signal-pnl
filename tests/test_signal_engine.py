import time

from event_taxonomy import event_is_primary, event_tier
from signal_engine import EventSignalEngine, MIN_FILL_PRICE, MIN_LAG, apply_suppressions


TOKEN_YES = "YES_TOKEN"
TOKEN_NO = "NO_TOKEN"


def _engine_with_price(token_id: str, price: float) -> EventSignalEngine:
    engine = EventSignalEngine()
    engine.record_price(token_id, price)
    return engine


def _game(now_ns: int, game_time_sec: int = 1200) -> dict:
    return {
        "match_id": "M1",
        "received_at_ns": now_ns,
        "game_time_sec": game_time_sec,
        "radiant_team": "team a",
        "dire_team": "team b",
        "radiant_lead": 1500,
        "radiant_score": 12,
        "dire_score": 10,
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


def _event(event_type="POLL_FIGHT_SWING", direction="radiant", delta=1500):
    return {
        "event_type": event_type,
        "direction": direction,
        "delta": delta,
        "severity": "high",
        "game_time_sec": 1200,
        "event_schema_version": "cadence_v1",
        "snapshot_gap_sec": 31,
        "actual_window_sec": 31,
        "networth_delta": delta,
        "kill_diff_delta": 2,
        "total_kills_delta": 2,
        "networth_delta_per_30s": round(delta * 30 / 31, 3),
        "kill_diff_delta_per_30s": round(2 * 30 / 31, 3),
        "source_cadence_quality": "normal",
    }


def test_fires_on_sufficient_lag_and_logs_cadence_metadata():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate_cluster(
        [_event("POLL_FIGHT_SWING", delta=1800)],
        _game(now_ns),
        _mapping(),
        _book(now_ns),
        None,
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["lag"] >= MIN_LAG
    assert result["event_schema_version"] == "cadence_v1"
    assert result["snapshot_gap_sec"] == 31
    assert result["source_cadence_quality"] == "normal"


def test_retired_fixed_events_are_inactive():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate(
        "LEAD_SWING_30S", "radiant", 6000,
        _game(now_ns), _mapping(), _book(now_ns), None,
        severity="high",
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "event_type_inactive"


def test_tactical_events_are_primary_for_clusters():
    assert event_tier("POLL_ULTRA_LATE_FIGHT_FLIP") == "A"
    assert event_is_primary("POLL_ULTRA_LATE_FIGHT_FLIP") is True
    assert event_tier("POLL_FIGHT_SWING") == "B"
    assert event_is_primary("POLL_FIGHT_SWING") is True
    assert event_tier("OBJECTIVE_CONVERSION_T2") == "research"
    assert event_is_primary("OBJECTIVE_CONVERSION_T2") is False
    assert event_tier("LEAD_SWING_30S") == "retired"
    assert event_is_primary("LEAD_SWING_30S") is False


def test_skip_team_side_unknown():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    mapping = _mapping()
    mapping["yes_team"] = "team c"
    result = engine.evaluate_cluster([_event()], _game(now_ns), mapping, _book(now_ns), None)
    assert result["decision"] == "skip"
    assert result["reason"] == "team_side_unknown"


def test_skip_fill_price_too_low():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.10)
    result = engine.evaluate_cluster(
        [_event()],
        _game(now_ns),
        _mapping(),
        _book(now_ns, ask=MIN_FILL_PRICE - 0.01, bid=0.08),
        None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "fill_price_too_low"


def test_event_specific_fill_cap_blocks_and_allows():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.83)
    allowed_event = _event("OBJECTIVE_CONVERSION_T3", delta=1)
    allowed_event["game_time_sec"] = 2400
    allowed = engine.evaluate_cluster(
        [allowed_event],
        _game(now_ns, game_time_sec=2400),
        _mapping(),
        _book(now_ns, ask=0.84, bid=0.82),
        None,
    )
    assert allowed["decision"] == "paper_buy_yes"
    assert allowed["max_fill_price"] == 0.88

    engine = _engine_with_price(TOKEN_YES, 0.83)
    blocked_event = _event("OBJECTIVE_CONVERSION_T3", delta=1)
    blocked_event["game_time_sec"] = 2400
    blocked = engine.evaluate_cluster(
        [blocked_event],
        _game(now_ns, game_time_sec=2400),
        _mapping(),
        _book(now_ns, ask=0.89, bid=0.87),
        None,
    )
    assert blocked["decision"] == "skip"
    assert blocked["reason"] == "fill_price_too_high"


def test_objective_conversion_t2_skips_without_primary_event():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate_cluster(
        [_event("OBJECTIVE_CONVERSION_T2", delta=1)],
        _game(now_ns),
        _mapping(),
        _book(now_ns),
        None,
    )
    assert result["decision"] == "skip"
    assert result["reason"] == "no_primary_event"


def test_suppressions_keep_strongest_cadence_event():
    events = [
        {"event_type": "POLL_FIGHT_SWING", "direction": "radiant"},
        {"event_type": "POLL_KILL_BURST_CONFIRMED", "direction": "radiant"},
    ]
    kept = apply_suppressions(events)
    assert [e["event_type"] for e in kept] == ["POLL_KILL_BURST_CONFIRMED"]


def test_cooldown_blocks_second_signal():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    kwargs = dict(
        events=[_event()],
        game=_game(now_ns),
        mapping=_mapping(),
        yes_book=_book(now_ns),
        no_book=None,
    )
    first = engine.evaluate_cluster(**kwargs)
    assert first["decision"] == "paper_buy_yes"
    engine.commit_signal(first)
    second = engine.evaluate_cluster(**kwargs)
    assert second["decision"] == "skip"
    assert second["reason"] == "cooldown"


def test_source_and_book_freshness_guards():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    game = _game(now_ns)
    game["data_source"] = "live_league"
    result = engine.evaluate_cluster([_event()], game, _mapping(), _book(now_ns), None)
    assert result["decision"] == "skip"
    assert result["reason"] == "non_top_live_source"

    stale_ns = now_ns - 10_000_000_000
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate_cluster([_event()], _game(now_ns), _mapping(), _book(stale_ns), None)
    assert result["decision"] == "skip"
    assert result["reason"] == "book_stale"
    assert result["event_tier"] == "B"
    assert result["token_id"] == TOKEN_YES


def test_already_repriced_skip_keeps_side_metadata():
    now_ns = time.time_ns()
    engine = EventSignalEngine()
    engine.record_price(TOKEN_YES, 0.45)
    # Move the anchor back to 6s ago (instead of 31s) to match the new 5s repricing check.
    engine._price_history[TOKEN_YES][0] = (int(time.time() * 1000) - 6_000, 0.45)
    engine.record_price(TOKEN_YES, 0.58)  # Move > 0.12 (1.5 * 0.08)

    result = engine.evaluate_cluster(
        [_event()],
        _game(now_ns),
        _mapping(),
        _book(now_ns, ask=0.59, bid=0.57),
        None,
    )

    assert result["decision"] == "skip"
    assert result["reason"] == "already_repriced"
    assert result["token_id"] == TOKEN_YES
    assert result["side"] == "YES"


def test_hybrid_fair_override_drives_edge_gate():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    result = engine.evaluate_cluster(
        events=[_event()],
        game=_game(now_ns),
        mapping=_mapping(),
        yes_book=_book(now_ns, ask=0.46, bid=0.44),
        no_book=None,
        fair_price_override=0.62,
        fair_source="hybrid",
    )
    assert result["decision"] == "paper_buy_yes"
    assert result["fair_price"] == 0.62
    assert result["fair_source"] == "hybrid"

def test_structure_confidence_lowers_impact():
    now_ns = time.time_ns()
    engine = _engine_with_price(TOKEN_YES, 0.45)
    
    event_high = _event("OBJECTIVE_CONVERSION_T3", delta=1)
    event_high["structure_confidence"] = 1.0
    
    res_high = engine.evaluate_cluster(
        events=[event_high],
        game=_game(now_ns),
        mapping=_mapping(),
        yes_book=_book(now_ns),
        no_book=None,
        require_primary=False
    )
    
    event_low = _event("OBJECTIVE_CONVERSION_T3", delta=1)
    event_low["structure_confidence"] = 0.5
    
    res_low = engine.evaluate_cluster(
        events=[event_low],
        game=_game(now_ns),
        mapping=_mapping(),
        yes_book=_book(now_ns),
        no_book=None,
        require_primary=False
    )
    
    assert res_low["expected_move"] < res_high["expected_move"]
    # Penalty should be logged
    assert res_low["structure_uncertainty_penalty"] > res_high["structure_uncertainty_penalty"]
    # Required edge should be higher
    assert res_low["required_edge"] > res_high["required_edge"]
