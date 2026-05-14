from hybrid_nowcast import compute_hybrid_nowcast


def test_drift_adjustment_is_oriented_to_radiant_event_probability():
    nowcast = compute_hybrid_nowcast(
        latest_liveleague_features={},
        latest_toplive_snapshot={"radiant_lead": 5000, "realtime_lead_nw": 1000},
        toplive_event_cluster=[],
        source_delay_metrics={"game_time_lag_sec": 0},
        slow_model_fair=0.50,
        game_time_sec=1800,
        event_direction="radiant",
    )
    assert nowcast.fast_event_adjustment > 0
    assert nowcast.hybrid_fair > 0.50


def test_drift_adjustment_is_flipped_for_dire_event_probability():
    nowcast = compute_hybrid_nowcast(
        latest_liveleague_features={},
        latest_toplive_snapshot={"radiant_lead": 5000, "realtime_lead_nw": 1000},
        toplive_event_cluster=[],
        source_delay_metrics={"game_time_lag_sec": 0},
        slow_model_fair=0.50,
        game_time_sec=1800,
        event_direction="dire",
    )
    assert nowcast.fast_event_adjustment < 0
    assert nowcast.hybrid_fair < 0.50
