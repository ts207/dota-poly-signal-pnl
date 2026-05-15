from structure_state import decode_structure_state, diff_structure_state


def test_building_state_only_not_decoded():
    s = decode_structure_state({"match_id": "1", "game_time_sec": 10, "building_state": 123})
    assert s.confidence == 0.0
    assert s.schema == "building_unknown"


def test_tower_count_increase_invalid():
    prev = decode_structure_state({"match_id": "1", "game_time_sec": 10, "tower_state": 0})
    cur = decode_structure_state({"match_id": "1", "game_time_sec": 20, "tower_state": 1})
    d = diff_structure_state(prev, cur)
    assert not d.valid
    assert d.reason == "structure_count_increased"
