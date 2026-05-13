from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .features import row_to_features
from .schemas import phase_for_duration


class FairModelBundle:
    def __init__(self, models: dict[str, Any], metadata: dict[str, Any]):
        self.models = models
        self.metadata = metadata

    @property
    def feature_names(self) -> list[str]:
        return list(self.metadata.get("feature_names") or [])

    def predict_radiant(self, row: dict[str, Any]) -> dict[str, Any]:
        phase = phase_for_duration(row.get("game_time_sec"))
        model = self.models.get(phase)
        if model is None:
            return {
                "radiant_fair_probability": None,
                "dire_fair_probability": None,
                "model_phase": phase,
                "model_confidence": 0.0,
                "top_features": [],
            }
        X = [row_to_features(row, self.feature_names)]
        proba = model.predict_proba(X)[0]
        classes = list(getattr(model, "classes_", [0, 1]))
        radiant_idx = classes.index(1) if 1 in classes else len(proba) - 1
        radiant = float(proba[radiant_idx])
        return {
            "radiant_fair_probability": round(radiant, 4),
            "dire_fair_probability": round(1.0 - radiant, 4),
            "model_phase": phase,
            "model_confidence": round(abs(radiant - 0.5) * 2.0, 4),
            "top_features": self.metadata.get("top_features", {}).get(phase, []),
        }


def load_bundle(path: str | Path) -> FairModelBundle:
    import joblib

    path = Path(path)
    data = joblib.load(path)
    return FairModelBundle(models=data["models"], metadata=data["metadata"])


def load_metadata(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))

