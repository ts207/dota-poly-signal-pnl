from dota_fair_model.features import DEFAULT_FEATURE_COLUMNS, build_feature_row, row_to_features
from dota_fair_model.inference import FairModelBundle, predict_yes
from dota_fair_model.schemas import FEATURE_SCHEMA_VERSION, phase_for_duration
from dota_fair_model.train import MIN_MATCH_GROUPS_PER_PHASE, MIN_SNAPSHOTS_PER_PHASE, assert_trainable_artifact


class FakeModel:
    classes_ = [0, 1]

    def predict_proba(self, X):
        assert len(X[0]) == len(DEFAULT_FEATURE_COLUMNS)
        return [[0.3, 0.7]]


class BadProbabilityModel:
    classes_ = [0, 1]

    def predict_proba(self, X):
        return [[-0.2, 1.2]]


def test_phase_for_duration_has_unknown_and_finer_buckets():
    assert phase_for_duration(None) == "unknown"
    assert phase_for_duration("") == "unknown"
    assert phase_for_duration("bad") == "unknown"
    assert phase_for_duration(9 * 60) == "early"
    assert phase_for_duration(12 * 60) == "laning"
    assert phase_for_duration(25 * 60) == "mid"
    assert phase_for_duration(35 * 60) == "late"
    assert phase_for_duration(50 * 60) == "ultra_late"


def test_build_feature_row_computes_derived_diffs_and_missingness():
    row = {
        "match_id": "m1",
        "game_time_sec": 1200,
        "radiant_score": 12,
        "dire_score": 9,
        "radiant_net_worth": 33000,
        "dire_net_worth": 30000,
        "radiant_p1_net_worth": 12000,
        "radiant_p2_net_worth": 9000,
        "radiant_p3_net_worth": 6000,
        "dire_p1_net_worth": 11000,
        "dire_p2_net_worth": 8000,
        "dire_p3_net_worth": 7000,
        "radiant_level": 70,
        "dire_level": 65,
        "radiant_has_aegis": False,
    }
    features = build_feature_row(row)
    assert features["score_diff"] == 3
    assert features["net_worth_diff"] == 3000
    assert features["top1_net_worth_diff"] == 1000
    assert features["top2_net_worth_diff"] == 2000
    assert features["top3_net_worth_diff"] == 1000
    assert features["level_diff"] == 5
    assert features["radiant_has_aegis"] == 0.0
    assert features["radiant_has_aegis_missing"] == 0.0
    assert features["dire_has_aegis_missing"] == 1.0
    assert features["feature_schema_version"] == FEATURE_SCHEMA_VERSION


def test_build_feature_row_treats_nan_as_missing():
    row = {
        "match_id": "m1",
        "game_time_sec": 1200,
        "radiant_score": 12,
        "dire_score": 9,
        "score_diff": float("nan"),
        "radiant_net_worth": 33000,
        "dire_net_worth": 30000,
        "net_worth_diff": float("nan"),
    }
    features = build_feature_row(row)
    assert features["score_diff"] == 3
    assert features["net_worth_diff"] == 3000
    assert features["net_worth_diff_missing"] == 0.0


def test_build_feature_row_uses_fast_radiant_lead_for_current_net_worth_diff():
    row = {
        "match_id": "m1",
        "game_time_sec": 1200,
        "radiant_score": 12,
        "dire_score": 9,
        "radiant_lead": 5000,
        "delayed_radiant_net_worth": 33000,
        "delayed_dire_net_worth": 30000,
    }
    features = build_feature_row(row)
    assert features["net_worth_diff"] == 5000
    assert features["net_worth_diff_missing"] == 0.0


def test_row_to_features_includes_missingness_columns():
    vector = row_to_features({"game_time_sec": 1200, "radiant_has_aegis": False})
    assert len(vector) == len(DEFAULT_FEATURE_COLUMNS)
    assert "dire_has_aegis_missing" in DEFAULT_FEATURE_COLUMNS
    assert vector[DEFAULT_FEATURE_COLUMNS.index("dire_has_aegis_missing")] == 1.0


def test_predict_radiant_returns_no_prediction_for_unknown_phase():
    bundle = FairModelBundle(models={"mid": FakeModel()}, metadata={"feature_names": DEFAULT_FEATURE_COLUMNS})
    pred = bundle.predict_radiant({"match_id": "m1"})
    assert pred["radiant_fair_probability"] is None
    assert pred["model_phase"] == "unknown"
    assert pred["model_reason"] == "unknown_phase"


def test_predict_radiant_rejects_out_of_range_probability():
    bundle = FairModelBundle(models={"mid": BadProbabilityModel()}, metadata={"feature_names": DEFAULT_FEATURE_COLUMNS})
    pred = bundle.predict_radiant({"match_id": "m1", "game_time_sec": 25 * 60})
    assert pred["radiant_fair_probability"] is None
    assert pred["model_reason"] == "invalid_probability"


def test_predict_yes_maps_radiant_to_market_side():
    bundle = FairModelBundle(
        models={"mid": FakeModel()},
        metadata={"feature_names": DEFAULT_FEATURE_COLUMNS, "schema_version": FEATURE_SCHEMA_VERSION},
    )
    row = {"game_time_sec": 25 * 60}
    normal = predict_yes(bundle, row, {"steam_side_mapping": "normal"})
    assert normal["yes_fair_probability"] == 0.7
    assert normal["no_fair_probability"] == 0.3

    reversed_side = bundle.predict_yes(row, {"steam_side_mapping": "reversed"})
    assert reversed_side["yes_fair_probability"] == 0.3
    assert reversed_side["no_fair_probability"] == 0.7


def test_predict_yes_requires_team_side_mapping():
    bundle = FairModelBundle(models={"mid": FakeModel()}, metadata={"feature_names": DEFAULT_FEATURE_COLUMNS})
    pred = predict_yes(bundle, {"game_time_sec": 25 * 60}, {})
    assert pred["yes_fair_probability"] is None
    assert pred["model_reason"] == "team_side_unknown"


def test_training_defaults_require_meaningful_sample_size():
    assert MIN_MATCH_GROUPS_PER_PHASE >= 50
    assert MIN_SNAPSHOTS_PER_PHASE >= 500


def test_trainable_artifact_guard_allows_trained_phase():
    assert_trainable_artifact({"models": {"mid": object()}, "metadata": {"metrics": {}}})


def test_trainable_artifact_guard_rejects_empty_model_set():
    try:
        assert_trainable_artifact(
            {
                "models": {},
                "metadata": {
                    "metrics": {
                        "early": {"skipped": "not_enough_rows"},
                        "late": {"skipped": "not_enough_match_groups"},
                    }
                },
            }
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("empty model artifact should be rejected")

    assert "no phase models trained" in message
    assert "early=not_enough_rows" in message
    assert "late=not_enough_match_groups" in message
