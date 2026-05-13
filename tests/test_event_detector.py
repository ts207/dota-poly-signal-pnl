from event_detector import EventDetector

ALL_ALIVE = (1 << 22) - 1

def game(t, lead, r_score=0, d_score=0, building_state=ALL_ALIVE):
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
    }


def mapping():
    return {"name": "Team A Game 1", "yes_team": "Team A", "yes_token_id": "YES"}


def test_detects_30s_lead_swing():
    detector = EventDetector()
    assert detector.observe(game(0, 0), mapping()) == []
    detector.observe(game(15, 500), mapping())
    events = detector.observe(game(31, 1800), mapping())
    assert any(e.event_type == "LEAD_SWING_30S" for e in events)


def test_detects_t2_tower_fall():
    detector = EventDetector()
    # Radiant Mid T2 (bit 4) falls favors Dire
    detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    events = detector.observe(game(20, 0, building_state=ALL_ALIVE & ~(1 << 4)), mapping())
    assert any(e.event_type == "T2_TOWER_FALL" and e.direction == "dire" for e in events)


def test_detects_kill_confirmed_lead_swing():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=5, d_score=5), mapping())
    events = detector.observe(game(31, 2600, r_score=8, d_score=5), mapping())

    evt = next(e for e in events if e.event_type == "KILL_CONFIRMED_LEAD_SWING")
    assert evt.direction == "radiant"
    assert evt.delta == 2600
    assert evt.severity == "medium"


def test_detects_kill_swing_only_when_networth_confirms():
    detector = EventDetector()
    detector.observe(game(0, 1000, r_score=5, d_score=5), mapping())
    contradicted = detector.observe(game(31, 500, r_score=9, d_score=5), mapping())
    assert not any(e.event_type == "KILL_BURST_30S" for e in contradicted)

    detector = EventDetector()
    detector.observe(game(0, 1000, r_score=5, d_score=5), mapping())
    confirmed = detector.observe(game(31, 1800, r_score=10, d_score=5), mapping())
    evt = next(e for e in confirmed if e.event_type == "KILL_BURST_30S")
    assert evt.direction == "radiant"
    assert evt.severity == "high"


def test_lead_swing_requires_reasonable_window_tolerance():
    detector = EventDetector()
    detector.observe(game(0, 0), mapping())
    # This would previously compare t=100 against t=0 and label a 100s move as a 30s swing.
    events = detector.observe(game(100, 5000), mapping())
    assert not any(e.event_type in {"LEAD_SWING_30S", "LEAD_SWING_60S"} for e in events)


def test_detects_objective_conversion_t3_when_tower_and_fight_align():
    detector = EventDetector()
    # Radiant Top T3 (bit 2) falls favors Dire
    detector.observe(game(0, 0, r_score=10, d_score=10, building_state=ALL_ALIVE), mapping())
    events = detector.observe(
        game(31, -3500, r_score=10, d_score=14, building_state=ALL_ALIVE & ~(1 << 2)),
        mapping(),
    )
    event_types = {e.event_type for e in events}
    assert "T3_TOWER_FALL" in event_types
    assert "KILL_CONFIRMED_LEAD_SWING" in event_types
    evt = next(e for e in events if e.event_type == "OBJECTIVE_CONVERSION_T3")
    assert evt.direction == "dire"
    assert evt.severity == "high"


def test_no_objective_conversion_without_same_direction_support():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=10, d_score=10, building_state=ALL_ALIVE), mapping())
    events = detector.observe(
        game(31, -100, r_score=10, d_score=10, building_state=ALL_ALIVE & ~(1 << 2)),
        mapping(),
    )
    assert any(e.event_type == "T3_TOWER_FALL" for e in events)
    assert not any(e.event_type == "OBJECTIVE_CONVERSION_T3" for e in events)


def test_detects_all_t2_towers_down_context_event():
    detector = EventDetector()
    # Radiant loses its last T2 (1, 4, 7) favors Dire
    two_t2s_already_dead = ALL_ALIVE & ~(1 << 1) & ~(1 << 4)
    detector.observe(game(0, 0, building_state=two_t2s_already_dead), mapping())
    events = detector.observe(game(20, -500, building_state=two_t2s_already_dead & ~(1 << 7)), mapping())
    event_types = {e.event_type for e in events}
    assert "T2_TOWER_FALL" in event_types
    evt = next(e for e in events if e.event_type == "ALL_T2_TOWERS_DOWN")
    assert evt.direction == "dire"


def test_dire_tower_fall_favors_radiant():
    detector = EventDetector()
    # Dire Mid T2 (bit 11 + 4 = 15) falls favors Radiant
    detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    events = detector.observe(game(20, 500, building_state=ALL_ALIVE & ~(1 << 15)), mapping())
    assert any(e.event_type == "T2_TOWER_FALL" and e.direction == "radiant" for e in events)


def test_building_state_does_not_treat_existing_destroyed_t3s_as_new_falls():
    detector = EventDetector()
    # Bits 0-10: Radiant, 11-21: Dire
    # If bit 2 (Radiant Top T3) is ALREADY 0 in the first update, it shouldn't fire in the second.
    initial = ALL_ALIVE & ~(1 << 2)
    detector.observe(game(0, 0, building_state=initial), mapping())
    # Another tower falls (Radiant Mid T2, bit 4)
    events = detector.observe(game(20, 0, building_state=initial & ~(1 << 4)), mapping())
    
    event_types = {e.event_type for e in events}
    assert "T2_TOWER_FALL" in event_types
    assert "T3_TOWER_FALL" not in event_types
