from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .features import row_to_features
from .schemas import FEATURE_SCHEMA_VERSION, phase_for_duration


class FairModelBundle:
    def __init__(self, models: dict[str, Any], metadata: dict[str, Any]):
        self.models = models
        self.metadata = metadata

    @property
    def feature_names(self) -> list[str]:
        return list(self.metadata.get("feature_names") or [])

    def predict_radiant(self, row: dict[str, Any]) -> dict[str, Any]:
        phase = phase_for_duration(row.get("game_time_sec"))
        if phase == "unknown":
            return _missing_prediction(phase, "unknown_phase")
        model = self.models.get(phase)
        if model is None:
            return _missing_prediction(phase, "no_phase_model")
        X = [row_to_features(row, self.feature_names)]
        proba = model.predict_proba(X)[0]
        classes = list(getattr(model, "classes_", [0, 1]))
        radiant_idx = classes.index(1) if 1 in classes else len(proba) - 1
        radiant = float(proba[radiant_idx])
        if radiant < -1e-9 or radiant > 1.0 + 1e-9:
            return _missing_prediction(phase, "invalid_probability")
        radiant = min(max(radiant, 0.0), 1.0)
        return {
            "radiant_fair_probability": round(radiant, 4),
            "dire_fair_probability": round(1.0 - radiant, 4),
            "model_phase": phase,
            "model_confidence": round(abs(radiant - 0.5) * 2.0, 4),
            "model_schema_version": self.metadata.get("schema_version") or FEATURE_SCHEMA_VERSION,
            "model_available": True,
            "model_reason": "ok",
            "top_features": self.metadata.get("top_features", {}).get(phase, []),
        }

    def predict_yes(self, row: dict[str, Any], mapping: dict[str, Any]) -> dict[str, Any]:
        return predict_yes(self, row, mapping)


def predict_yes(bundle: FairModelBundle, row: dict[str, Any], mapping: dict[str, Any]) -> dict[str, Any]:
    pred = bundle.predict_radiant(row)
    radiant = pred["radiant_fair_probability"]

    if radiant is None:
        return {
            **pred,
            "yes_fair_probability": None,
            "no_fair_probability": None,
        }

    yes_side = mapping.get("steam_side_mapping")
    if yes_side == "normal":
        yes = radiant
    elif yes_side == "reversed":
        yes = 1.0 - radiant
    else:
        return {
            **pred,
            "yes_fair_probability": None,
            "no_fair_probability": None,
            "model_available": False,
            "model_reason": "team_side_unknown",
        }

    return {
        **pred,
        "yes_fair_probability": round(yes, 4),
        "no_fair_probability": round(1.0 - yes, 4),
        "model_reason": "ok",
    }


def _missing_prediction(phase: str, reason: str) -> dict[str, Any]:
    return {
        "radiant_fair_probability": None,
        "dire_fair_probability": None,
        "model_phase": phase,
        "model_confidence": 0.0,
        "model_schema_version": FEATURE_SCHEMA_VERSION,
        "model_available": False,
        "model_reason": reason,
        "top_features": [],
    }


def load_bundle(path: str | Path) -> FairModelBundle:
    import joblib

    path = Path(path)
    data = joblib.load(path)
    return FairModelBundle(models=data["models"], metadata=data["metadata"])


def load_metadata(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
