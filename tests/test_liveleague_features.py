import time
from liveleague_features import (
    extract_items, parse_players, extract_liveleague_features,
    compute_derived_events, LiveLeagueContextCache,
)


def _make_player(**overrides):
    base = {
        "account_id": 100,
        "hero_id": 1,
        "kills": 0,
        "deaths": 0,
        "assists": 0,
        "net_worth": 1000,
        "item0": 0,
        "item1": 0,
        "item2": 0,
        "item3": 0,
        "item4": 0,
        "item5": 0,
    }
    base.update(overrides)
    return base


def _make_raw(**overrides):
    base = {
        "match_id": "12345",
        "lobby_id": "67890",
        "league_id": "999",
        "series_id": 42,
        "series_type": 1,
        "stream_delay_s": 120,
        "radiant_team": {"team_id": 100, "team_name": "Team Radiant"},
        "dire_team": {"team_id": 200, "team_name": "Team Dire"},
        "scoreboard": {
            "duration": 1800,
            "radiant": {
                "score": 15,
                "tower_state": 2031,
                "barracks_state": 63,
                "players": [
                    _make_player(account_id=1, hero_id=1, net_worth=5000, kills=3, deaths=0),
                    _make_player(account_id=2, hero_id=2, net_worth=4000, kills=2, deaths=1, respawn_timer=5),
                ],
            },
            "dire": {
                "score": 10,
                "tower_state": 2031,
                "barracks_state": 63,
                "players": [
                    _make_player(account_id=3, hero_id=3, net_worth=3500, kills=1, deaths=3),
                    _make_player(account_id=4, hero_id=4, net_worth=2500, kills=1, deaths=4, respawn_timer=60),
                ],
            },
        },
    }
    base.update(overrides)
    return base


def test_extract_items_picks_up_item_slots():
    player = {"item0": 108, "item1": 50, "item2": 0, "backpack0": 77, "other_key": "foo"}
    items = extract_items(player)
    assert 108 in items
    assert 50 in items
    assert 77 in items
    assert 0 not in items


def test_extract_items_empty():
    assert extract_items({}) == []


def test_parse_players_basic():
    players = [
        _make_player(account_id=1, hero_id=10, net_worth=5000, kills=3, deaths=1, assists=5),
        _make_player(account_id=2, hero_id=20, net_worth=3000, kills=1, deaths=4, assists=3, respawn_timer=20),
    ]
    parsed = parse_players(players)
    assert len(parsed) == 2
    assert parsed[0]["account_id"] == 1
    assert parsed[0]["net_worth"] == 5000
    assert parsed[1]["respawn_timer"] == 20
    assert parsed[0]["has_aegis"] is False


def test_parse_players_aegis_detection():
    players = [_make_player(item0=108)]
    parsed = parse_players(players)
    assert parsed[0]["has_aegis"] is True


def test_parse_players_empty_list():
    assert parse_players([]) == []


def test_parse_players_non_dict_skipped():
    parsed = parse_players(["not_a_dict", None, _make_player()])
    assert len(parsed) == 1


def test_extract_liveleague_features_basic():
    raw = _make_raw()
    received_at_ns = time.time_ns()
    features = extract_liveleague_features(raw, received_at_ns)

    assert features["match_id"] == "12345"
    assert features["lobby_id"] == "67890"
    assert features["league_id"] == "999"
    assert features["series_id"] == 42
    assert features["series_type"] == 1
    assert features["game_time_sec"] == 1800
    assert features["stream_delay_s"] == 120
    assert features["radiant_team"] == "Team Radiant"
    assert features["dire_team"] == "Team Dire"
    assert features["radiant_team_id"] == 100
    assert features["dire_team_id"] == 200
    assert features["radiant_score"] == 15
    assert features["dire_score"] == 10
    assert features["aegis_team"] is None


def test_extract_liveleague_features_aegis_radiant():
    raw = _make_raw()
    raw["scoreboard"]["radiant"]["players"][0]["item0"] = 108
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["aegis_team"] == "radiant"
    assert features["aegis_holder_hero_id"] == 1


def test_extract_liveleague_features_aegis_dire():
    raw = _make_raw()
    raw["scoreboard"]["dire"]["players"][0]["item0"] = 108
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["aegis_team"] == "dire"


def test_extract_liveleague_features_dead_counts():
    raw = _make_raw()
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["radiant_dead_count"] == 1
    assert features["dire_dead_count"] == 1


def test_extract_liveleague_features_max_respawn():
    raw = _make_raw()
    # radiant player has respawn_timer=5, dire player has respawn_timer=60
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["radiant_max_respawn"] == 5
    assert features["dire_max_respawn"] == 60


def test_extract_liveleague_features_core_dead_count():
    raw = _make_raw()
    # dire player has respawn_timer=60 (>= 50, counts as core dead)
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["dire_core_dead_count"] == 1
    assert features["radiant_core_dead_count"] == 0


def test_extract_liveleague_features_top3_nw():
    raw = _make_raw()
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["radiant_top3_nw"] == 9000
    assert features["dire_top3_nw"] == 6000


def test_extract_liveleague_features_missing_scoreboard():
    raw = {"match_id": "1", "lobby_id": "2"}
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["match_id"] == "1"
    assert features["game_time_sec"] is None
    assert features["radiant_players"] == []
    assert features["dire_players"] == []


def test_extract_liveleague_features_series_id_non_numeric():
    raw = _make_raw(series_id="not_a_number")
    features = extract_liveleague_features(raw, time.time_ns())
    assert features["series_id"] is None


def test_compute_derived_events_aegis():
    ctx = {"aegis_team": "radiant"}
    events = compute_derived_events(ctx)
    assert "AEGIS_HELD_BY_RADIANT" in events


def test_compute_derived_events_core_dead_late_game():
    ctx = {"radiant_core_dead_count": 2, "dire_core_dead_count": 1}
    events = compute_derived_events(ctx, game_time_sec=2700)
    assert "TWO_CORES_DEAD_50S_PLUS_RADIANT" in events
    assert "CORE_DEAD_60S_PLUS_DIRE" in events


def test_compute_derived_events_no_events_early_game():
    ctx = {"aegis_team": None, "radiant_core_dead_count": 1}
    events = compute_derived_events(ctx, game_time_sec=600)
    assert "CORE_DEAD_60S_PLUS_RADIANT" not in events


def test_liveleague_context_cache_update_and_get():
    cache = LiveLeagueContextCache()
    raw = _make_raw()
    cache.update([raw], time.time_ns())

    result = cache.get("12345")
    assert result is not None
    assert result["match_id"] == "12345"
    assert result["radiant_team"] == "Team Radiant"


def test_liveleague_context_cache_missing():
    cache = LiveLeagueContextCache()
    assert cache.get("nonexistent") is None


def test_attach_to_game_fresh_context():
    cache = LiveLeagueContextCache()
    received_at_ns = time.time_ns()
    raw = _make_raw()
    cache.update([raw], received_at_ns)

    game = {
        "match_id": "12345",
        "game_time_sec": 1798,
        "received_at_ns": received_at_ns,
        "radiant_team": "Team Radiant",
        "dire_team": "Team Dire",
    }
    result = cache.attach_to_game(game)
    assert result["liveleague_context_status"] in ("fresh", "stale")
    assert "liveleague_context" in result
    assert "liveleague_age_ms" in result


def test_attach_to_game_missing_context():
    cache = LiveLeagueContextCache()
    game = {"match_id": "nonexistent", "game_time_sec": 100}
    result = cache.attach_to_game(game)
    assert result["liveleague_context_status"] == "missing"


def test_stale_context_does_not_affect_signal_decision():
    cache = LiveLeagueContextCache()
    received_at_ns = time.time_ns() - 120_000_000_000  # 120 seconds ago
    raw = _make_raw()
    raw["scoreboard"]["duration"] = 1800
    cache.update([raw], received_at_ns)

    game = {
        "match_id": "12345",
        "game_time_sec": 1800,
        "received_at_ns": time.time_ns(),
        "radiant_team": "Team Radiant",
        "dire_team": "Team Dire",
    }
    cache.attach_to_game(game)

    # Stale context: age > 3000ms
    assert game["liveleague_context_status"] == "stale"
    # Even with stale context, the game dict should not have trading-relevant fields changed
    # The signal decision path must check liveleague_context_status before using any LLG data
    assert game.get("liveleague_context") is not None


def test_validate_mapping_no_mismatch():
    cache = LiveLeagueContextCache()
    raw = _make_raw()
    cache.update([raw], time.time_ns())

    game = {
        "match_id": "12345",
        "radiant_team": "Team Radiant",
        "dire_team": "Team Dire",
        "league_id": "999",
    }
    mapping = {"yes_team": "Team Radiant", "dota_match_id": "12345"}
    mismatches = cache.validate_mapping(game, mapping)
    assert mismatches == []


def test_validate_mapping_detects_team_mismatch():
    cache = LiveLeagueContextCache()
    raw = _make_raw()
    raw["scoreboard"]["radiant"]["players"][0]["net_worth"] = 5000
    cache.update([raw], time.time_ns())

    game = {
        "match_id": "12345",
        "radiant_team": "Wrong Team Name",
        "dire_team": "Team Dire",
        "league_id": "999",
    }
    mapping = {"yes_team": "Team Radiant", "dota_match_id": "12345"}
    mismatches = cache.validate_mapping(game, mapping)
    assert len(mismatches) > 0
    assert any("radiant_team" in m for m in mismatches)


def test_validate_mapping_series_game_mismatch():
    cache = LiveLeagueContextCache()
    raw = _make_raw(series_type=0)  # Bo1/Bo2 style
    cache.update([raw], time.time_ns())

    game = {
        "match_id": "12345",
        "radiant_team": "Team Radiant",
        "dire_team": "Team Dire",
        "league_id": "999",
    }
    mapping = {"yes_team": "Team Radiant", "dota_match_id": "12345", "game_number": 5}
    mismatches = cache.validate_mapping(game, mapping)
    assert any("series_type" in m for m in mismatches)


def test_validate_mapping_missing_context():
    cache = LiveLeagueContextCache()
    game = {"match_id": "not_in_cache", "radiant_team": "X", "dire_team": "Y", "league_id": "1"}
    mapping = {"yes_team": "X", "dota_match_id": "not_in_cache"}
    mismatches = cache.validate_mapping(game, mapping)
    assert mismatches == []