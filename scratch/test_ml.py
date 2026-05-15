import json
import joblib
import pandas as pd
from dota_fair_model.inference import load_bundle
from dota_fair_model.features import row_to_features

bundle = load_bundle('dota_fair_model/models/dota_fair.joblib')
row = {
    "game_time_sec": 1297,
    "radiant_lead": 1479,
    "radiant_score": 24,
    "dire_score": 19,
    "radiant_team": "Team Liquid",
    "dire_team": "Virtus.pro",
    "networth_delta": 1479,
    "kill_diff_delta": 5,
    "total_kills_delta": 43, # Wait, total_kills is 24+19 = 43
    "networth_delta_per_30s": 1056,
    "kill_diff_delta_per_30s": 1.4,
}

feat = row_to_features(row, bundle.feature_names)
print("Features:", list(zip(bundle.feature_names, feat)))
pred = bundle.predict_radiant(row)
print("Prediction:", pred)
