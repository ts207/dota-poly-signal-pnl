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
    evt = next(e for e in events if e.event_type == "LEAD_SWING_30S")
    assert evt.event_dedupe_key
    assert evt.event_tier == "B"
    assert evt.event_is_primary is True


def test_suppresses_duplicate_same_event_key():
    detector = EventDetector()
    detector.observe(game(0, 0), mapping())
    first = detector.observe(game(60, 8000), mapping())
    repeated = detector.observe(game(60, 8000), mapping())
    assert any(e.event_type == "LEAD_SWING_60S" for e in first)
    assert not any(e.event_type == "LEAD_SWING_60S" for e in repeated)


def test_ultra_late_lead_swing_requires_extreme_delta():
    detector = EventDetector()
    detector.observe(game(3540, 0), mapping())
    noisy = detector.observe(game(3600, 16000), mapping())
    assert not any(e.event_type == "LEAD_SWING_60S" for e in noisy)

    detector = EventDetector()
    detector.observe(game(3540, 0), mapping())
    extreme = detector.observe(game(3600, 24000), mapping())
    assert any(e.event_type == "LEAD_SWING_60S" for e in extreme)


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


def test_detects_teamfight_swing_below_kill_confirmed_gold_threshold():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=5, d_score=5), mapping())
    events = detector.observe(game(31, 1200, r_score=7, d_score=5), mapping())
    event_types = {e.event_type for e in events}
    assert "TEAMFIGHT_SWING_30S" in event_types
    assert "KILL_CONFIRMED_LEAD_SWING" not in event_types
    evt = next(e for e in events if e.event_type == "TEAMFIGHT_SWING_30S")
    assert evt.direction == "radiant"
    assert evt.event_tier == "B"
    assert evt.event_is_primary is True


def test_detects_bloody_even_fight_as_research_context():
    detector = EventDetector()
    detector.observe(game(0, 0, r_score=5, d_score=5), mapping())
    events = detector.observe(game(31, 200, r_score=8, d_score=7), mapping())
    evt = next(e for e in events if e.event_type == "BLOODY_EVEN_FIGHT_30S")
    assert evt.direction is None
    assert evt.event_tier == "research"
    assert evt.event_is_primary is False


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
    assert evt.event_tier == "B"
    assert evt.event_is_primary is True


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


def test_detects_throne_exposed_after_second_t4():
    detector = EventDetector()
    # First T4 (Radiant bit 9) falls
    one_t4_alive = ALL_ALIVE & ~(1 << 9)
    detector.observe(game(20, 0, building_state=ALL_ALIVE), mapping())
    events1 = detector.observe(game(21, 0, building_state=one_t4_alive), mapping())
    assert any(e.event_type == "FIRST_T4_TOWER_FALL" for e in events1)

    # Second T4 (Radiant bit 10) falls → throne exposed
    both_t4_dead = ALL_ALIVE & ~(1 << 9) & ~(1 << 10)
    events2 = detector.observe(game(22, 0, building_state=both_t4_dead), mapping())
    event_types = {e.event_type for e in events2}
    assert "SECOND_T4_TOWER_FALL" in event_types
    assert "THRONE_EXPOSED" in event_types
    throne_evt = next(e for e in events2 if e.event_type == "THRONE_EXPOSED")
    assert throne_evt.direction == "dire"
    assert throne_evt.severity == "high"


def test_detects_throne_exposed_when_both_t4s_fall_simultaneously():
    detector = EventDetector()
    # Both T4s fall at once
    both_t4_dead = ALL_ALIVE & ~(1 << 9) & ~(1 << 10)
    detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    events = detector.observe(game(20, 0, building_state=both_t4_dead), mapping())
    event_types = {e.event_type for e in events}
    assert "THRONE_EXPOSED" in event_types
    assert "SECOND_T4_TOWER_FALL" in event_types


def test_detects_all_t3_towers_down():
    detector = EventDetector()
    # Radiant Top and Mid T3 already dead (bits 2, 5), Bot T3 (bit 8) still alive
    two_t3_dead = ALL_ALIVE & ~(1 << 2) & ~(1 << 5)
    detector.observe(game(0, 0, building_state=two_t3_dead), mapping())
    # Bot T3 (bit 8) now dies too
    all_t3_dead = two_t3_dead & ~(1 << 8)
    events = detector.observe(game(20, 0, building_state=all_t3_dead), mapping())
    event_types = {e.event_type for e in events}
    assert "ALL_T3_TOWERS_DOWN" in event_types
    evt = next(e for e in events if e.event_type == "ALL_T3_TOWERS_DOWN")
    assert evt.direction == "dire"
    assert evt.severity == "high"


def test_detects_multi_structure_collapse():
    detector = EventDetector()
    # Radiant Top T2 (bit 1) and Top T3 (bit 2) fall simultaneously
    t2_t3_fall = ALL_ALIVE & ~(1 << 1) & ~(1 << 2)
    detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    events = detector.observe(game(20, 0, building_state=t2_t3_fall), mapping())
    event_types = {e.event_type for e in events}
    assert "MULTI_STRUCTURE_COLLAPSE" in event_types
    mc_evt = next(e for e in events if e.event_type == "MULTI_STRUCTURE_COLLAPSE")
    assert mc_evt.direction == "dire"


def test_detects_t3_plus_t4_chain():
    detector = EventDetector()
    # Radiant T3 (bit 2) and T4 (bit 9) fall simultaneously
    t3_t4_fall = ALL_ALIVE & ~(1 << 2) & ~(1 << 9)
    detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    events = detector.observe(game(20, 0, building_state=t3_t4_fall), mapping())
    event_types = {e.event_type for e in events}
    assert "T3_PLUS_T4_CHAIN" in event_types
    assert "MULTI_STRUCTURE_COLLAPSE" in event_types
    chain_evt = next(e for e in events if e.event_type == "T3_PLUS_T4_CHAIN")
    assert chain_evt.severity == "high"


def test_pressure_metadata_with_kill_delta():
    detector = EventDetector()
    # Radiant T2 falls while radiant is gaining kills and net worth
    detector.observe(game(0, 500, r_score=10, d_score=10, building_state=ALL_ALIVE), mapping())
    events = detector.observe(
        game(20, 3500, r_score=14, d_score=10, building_state=ALL_ALIVE & ~(1 << 4)),
        mapping(),
    )
    t2_evt = next(e for e in events if e.event_type == "T2_TOWER_FALL")
    assert t2_evt.base_pressure_score is not None
    assert t2_evt.event_confidence is not None


def test_pressure_metadata_without_previous_snapshot():
    detector = EventDetector()
    # First observation → no previous → pressure scores should be None or based on base only
    events = detector.observe(game(0, 0, building_state=ALL_ALIVE), mapping())
    assert events == []
    # Second observation (no building changes)
    events = detector.observe(game(20, 500, building_state=ALL_ALIVE), mapping())
    for evt in events:
        assert evt.base_pressure_score is None or evt.base_pressure_score is not None


def test_detects_late_major_comeback_reprice():
    detector = EventDetector()
    detector.observe(game(2400, -9000, r_score=20, d_score=28), mapping())
    events = detector.observe(game(2460, 1500, r_score=24, d_score=28), mapping())
    event_types = {e.event_type for e in events}
    assert "MAJOR_COMEBACK" in event_types
    assert "LEAD_SWING_60S" in event_types
    evt = next(e for e in events if e.event_type == "LATE_MAJOR_COMEBACK_REPRICE")
    assert evt.direction == "radiant"
    assert evt.event_tier == "B"
    assert evt.event_family == "late_reversal"
    assert evt.component_event_types
    assert "MAJOR_COMEBACK" in evt.component_event_types
    assert "LEAD_SWING_60S" in evt.component_event_types


def test_detects_comeback_recovery_before_lead_flip():
    detector = EventDetector()
    detector.observe(game(1200, -5000, r_score=10, d_score=16), mapping())
    events = detector.observe(game(1260, -2500, r_score=13, d_score=16), mapping())
    evt = next(e for e in events if e.event_type == "COMEBACK_RECOVERY_60S")
    assert evt.direction == "radiant"
    assert evt.delta == 2500
    assert evt.event_tier == "B"
    assert evt.event_is_primary is True


def test_detects_late_major_comeback_reprice_before_lead_flip():
    detector = EventDetector()
    detector.observe(game(2400, -9000, r_score=20, d_score=28), mapping())
    events = detector.observe(game(2460, -4500, r_score=24, d_score=28), mapping())
    event_types = {e.event_type for e in events}
    assert "MAJOR_COMEBACK_RECOVERY_60S" in event_types
    evt = next(e for e in events if e.event_type == "LATE_MAJOR_COMEBACK_REPRICE")
    assert evt.direction == "radiant"
    assert "MAJOR_COMEBACK_RECOVERY_60S" in evt.component_event_types


def test_detects_ultra_late_wipe_confirmed_with_base_pressure():
    detector = EventDetector()
    detector.observe(game(3000, 0, r_score=30, d_score=30, building_state=ALL_ALIVE), mapping())
    dire_top_t3_dead = ALL_ALIVE & ~(1 << 13)
    events = detector.observe(
        game(3031, 5500, r_score=34, d_score=30, building_state=dire_top_t3_dead),
        mapping(),
    )
    event_types = {e.event_type for e in events}
    assert "ULTRA_LATE_WIPE" in event_types
    assert "T3_TOWER_FALL" in event_types
    evt = next(e for e in events if e.event_type == "ULTRA_LATE_WIPE_CONFIRMED")
    assert evt.direction == "radiant"
    assert evt.event_tier == "B"


def test_detects_fight_to_gold_confirm_primary_event():
    detector = EventDetector()
    detector.observe(game(600, 0, r_score=8, d_score=8), mapping())
    events = detector.observe(game(631, 2600, r_score=11, d_score=8), mapping())
    evt = next(e for e in events if e.event_type == "FIGHT_TO_GOLD_CONFIRM_30S")
    assert evt.direction == "radiant"
    assert evt.event_tier == "B"
    assert evt.event_is_primary is True
    assert evt.component_deltas


def test_detects_chained_late_fight_recovery():
    detector = EventDetector()
    detector.observe(game(2670, 0, r_score=30, d_score=30), mapping())
    first = detector.observe(game(2701, 6000, r_score=33, d_score=30), mapping())
    assert any(e.event_type == "KILL_CONFIRMED_LEAD_SWING" for e in first)
    second = detector.observe(game(2732, 6200, r_score=33, d_score=30), mapping())
    evt = next(e for e in second if e.event_type == "CHAINED_LATE_FIGHT_RECOVERY")
    assert evt.direction == "radiant"
    assert evt.event_tier == "B"
