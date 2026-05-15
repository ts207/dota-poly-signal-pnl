from event_detector import EventDetector
from event_taxonomy import RETIRED_FIXED_WINDOW_EVENTS


ALL_ALIVE = (1 << 22) - 1


def game(t, lead, r_score=0, d_score=0, building_state=ALL_ALIVE, data_source="top_live"):
    return {
        "match_id": "M1",
        "lobby_id": "L1",
        "league_id": "LEAGUE",
        "game_time_sec": t,
        "radiant_team": "Team A",
        "dire_team": "Team B",
        "radiant_lead": lead,
        "radiant_score": r_score,
        "dire_score": d_score,
        "building_state": building_state,
        "tower_state": building_state,
        "data_source": data_source,
    }


def top_live_game(t, lead, r_score=0, d_score=0, building_state=ALL_ALIVE, tower_state=None):
    g = game(t, lead, r_score, d_score, building_state, data_source="top_live")
    g["tower_state"] = tower_state
    return g


def mapping():
    return {"name": "Team A Game 1", "yes_team": "Team A", "yes_token_id": "YES"}


def event_types(events):
    return {event.event_type for event in events}


def test_poll_fight_swing_emits_for_irregular_valid_gaps():
    for gap in (10, 31, 60, 75):
        detector = EventDetector()
        detector.observe(game(100, 0, r_score=5, d_score=5), mapping())
        events = detector.observe(game(100 + gap, 1500, r_score=7, d_score=5), mapping())
        evt = next(e for e in events if e.event_type == "POLL_FIGHT_SWING")
        assert evt.snapshot_gap_sec == gap
        assert evt.actual_window_sec == gap
        assert evt.networth_delta == 1500
        assert evt.kill_diff_delta == 2
        assert evt.event_schema_version == "cadence_v1"
        assert evt.source_cadence_quality in {"direct", "normal"}


def test_stale_and_invalid_gaps_block_tactical_fights():
    for gap, quality in ((120, "stale_gap"), (300, "invalid_gap")):
        detector = EventDetector()
        detector.observe(game(100, 0, r_score=5, d_score=5), mapping())
        events = detector.observe(game(100 + gap, 5000, r_score=9, d_score=5), mapping())
        assert not (event_types(events) & {"POLL_FIGHT_SWING", "POLL_KILL_BURST_CONFIRMED", "POLL_LATE_FIGHT_FLIP"})
        assert all(e.source_cadence_quality == quality for e in events)


def test_kill_burst_requires_networth_direction_confirmation():
    detector = EventDetector()
    detector.observe(game(0, 1000, r_score=5, d_score=5), mapping())
    contradicted = detector.observe(game(31, 500, r_score=9, d_score=5), mapping())
    assert "POLL_KILL_BURST_CONFIRMED" not in event_types(contradicted)

    detector = EventDetector()
    detector.observe(game(0, 1000, r_score=5, d_score=5), mapping())
    confirmed = detector.observe(game(31, 1500, r_score=8, d_score=5), mapping())
    evt = next(e for e in confirmed if e.event_type == "POLL_KILL_BURST_CONFIRMED")
    assert evt.direction == "radiant"
    assert evt.delta == 3
    assert "NETWORTH_DELTA" in (evt.component_event_types or "")


def test_lead_flip_with_kills_and_comeback_recovery():
    detector = EventDetector()
    detector.observe(game(1200, -2500, r_score=10, d_score=13), mapping())
    events = detector.observe(game(1260, 1200, r_score=13, d_score=13), mapping())
    evt = next(e for e in events if e.event_type == "POLL_LEAD_FLIP_WITH_KILLS")
    assert evt.direction == "radiant"
    assert evt.previous_value == -2500
    assert evt.current_value == 1200

    detector = EventDetector()
    detector.observe(game(1200, -5000, r_score=10, d_score=16), mapping())
    events = detector.observe(game(1260, -2500, r_score=11, d_score=16), mapping())
    evt = next(e for e in events if e.event_type == "POLL_COMEBACK_RECOVERY")
    assert evt.direction == "radiant"
    assert evt.delta == 2500


def test_major_comeback_and_stomp_throw_confirmed():
    detector = EventDetector()
    detector.observe(game(2400, -9000, r_score=20, d_score=28), mapping())
    events = detector.observe(game(2460, -4500, r_score=20, d_score=28), mapping())
    assert "POLL_MAJOR_COMEBACK_RECOVERY" in event_types(events)

    detector = EventDetector()
    detector.observe(game(1800, 13000, r_score=30, d_score=20), mapping())
    events = detector.observe(game(1860, 10000, r_score=30, d_score=23), mapping())
    evt = next(e for e in events if e.event_type == "POLL_STOMP_THROW_CONFIRMED")
    assert evt.direction == "dire"
    assert evt.delta == 3000


def test_late_and_ultra_late_fight_flip_ranking():
    detector = EventDetector()
    detector.observe(game(2400, 0, r_score=30, d_score=30), mapping())
    late = detector.observe(game(2460, 2600, r_score=33, d_score=30), mapping())
    assert "POLL_LATE_FIGHT_FLIP" in event_types(late)

    detector = EventDetector()
    detector.observe(game(3000, 2000, r_score=35, d_score=35), mapping())
    ultra = detector.observe(game(3060, -1200, r_score=35, d_score=38), mapping())
    evt = next(e for e in ultra if e.event_type == "POLL_ULTRA_LATE_FIGHT_FLIP")
    assert evt.direction == "dire"
    assert "POLL_LEAD_FLIP_WITH_KILLS" in (evt.component_event_types or "")


def test_structure_only_is_component_not_trade_event():
    detector = EventDetector()
    detector.observe(game(0, 0, building_state=ALL_ALIVE, data_source="live_league"), mapping())
    events = detector.observe(game(20, 0, building_state=ALL_ALIVE & ~(1 << 4), data_source="live_league"), mapping())
    assert events == []


def test_objective_conversion_requires_same_direction_tactical_event():
    detector = EventDetector()
    # T3 bottom is bit 2
    detector.observe(game(0, 0, r_score=10, d_score=10, building_state=ALL_ALIVE, data_source="live_league"), mapping())
    events = detector.observe(
        game(31, -3500, r_score=10, d_score=14, building_state=ALL_ALIVE & ~(1 << 2), data_source="live_league"),
        mapping(),
    )
    evt = next(e for e in events if e.event_type == "OBJECTIVE_CONVERSION_T3")
    assert evt.direction == "dire"
    assert "T3_TOWER_FALL" in (evt.component_event_types or "")
    assert "POLL_KILL_BURST_CONFIRMED" in (evt.component_event_types or "")

    detector = EventDetector()
    detector.observe(game(0, 0, r_score=10, d_score=10, building_state=ALL_ALIVE, data_source="live_league"), mapping())
    no_support = detector.observe(
        game(31, -100, r_score=10, d_score=10, building_state=ALL_ALIVE & ~(1 << 2), data_source="live_league"),
        mapping(),
    )
    assert "OBJECTIVE_CONVERSION_T3" not in event_types(no_support)


def test_t2_conversion_is_research_support():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=10, d_score=10, building_state=ALL_ALIVE, data_source="live_league"), mapping())
    events = detector.observe(
        # T2 middle is bit 4
        game(31, -1800, r_score=10, d_score=12, building_state=ALL_ALIVE & ~(1 << 4), data_source="live_league"),
        mapping(),
    )
    evt = next(e for e in events if e.event_type == "POLL_FIGHT_SWING")
    assert "OBJECTIVE_CONVERSION_T2" in (evt.component_event_types or "")


def test_top_live_building_state_without_decoded_tower_state_does_not_emit_structure_events():
    detector = EventDetector()
    detector.observe(top_live_game(723, -1100, r_score=3, d_score=3, building_state=4849801), mapping())
    events = detector.observe(top_live_game(783, 934, r_score=4, d_score=3, building_state=5374089), mapping())
    assert not (event_types(events) & {"OBJECTIVE_CONVERSION_T3", "BASE_PRESSURE_T3_COLLAPSE", "BASE_PRESSURE_T4"})


def test_no_retired_fixed_window_events_are_primary():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=5, d_score=5), mapping())
    events = detector.observe(game(31, 5000, r_score=10, d_score=5), mapping())
    assert event_types(events).isdisjoint(RETIRED_FIXED_WINDOW_EVENTS)
    for event in events:
        assert not event.event_type.endswith("_30S")
        assert not event.event_type.endswith("_60S")

def test_throne_exposed_semantics():
    detector = EventDetector()
    # Dire T3 dead, T4 alive (Bits 17,18,19 dead, 20,21 alive)
    mask = ALL_ALIVE & ~(0x1C0 << 11)
    detector.observe(game(1000, 0, building_state=mask), mapping())
    # Both Dire T4 fall (bits 20, 21)
    # Support (kills) in same direction (Radiant)
    events = detector.observe(game(1031, 1000, r_score=3, d_score=0, building_state=mask & ~(0x600 << 11)), mapping())
    # OBJECTIVE_CONVERSION_T4 (120) should be primary, THRONE_EXPOSED (110) should be component
    assert "OBJECTIVE_CONVERSION_T4" in event_types(events)
    primary = next(e for e in events if e.event_type == "OBJECTIVE_CONVERSION_T4")
    assert "THRONE_EXPOSED" in (primary.component_event_types or "")

def test_base_pressure_requires_pressure():
    detector = EventDetector()
    # Dire T4 already dead
    mask = ALL_ALIVE & ~(0x7FF << 11)
    detector.observe(game(1000, 0, building_state=mask), mapping())
    # Just bit change (no kills/NW)
    events = detector.observe(game(1031, 0, building_state=mask), mapping())
    assert "BASE_PRESSURE_T4" not in event_types(events)
    
    # Pressure added (Radiant support)
    events = detector.observe(game(1062, 500, r_score=1, d_score=0, building_state=mask), mapping())
    assert "BASE_PRESSURE_T4" in event_types(events)
