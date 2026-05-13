from __future__ import annotations

from typing import Any


def calibration_metrics(y_true: list[int], y_prob: list[float]) -> dict[str, Any]:
    from sklearn.metrics import brier_score_loss, log_loss

    labels = [0, 1]
    return {
        "brier_score": float(brier_score_loss(y_true, y_prob)),
        "log_loss": float(log_loss(y_true, y_prob, labels=labels)),
    }


def calibrated_classifier(base_model: Any, *, method: str = "sigmoid", cv: int = 3) -> Any:
    from sklearn.calibration import CalibratedClassifierCV

    return CalibratedClassifierCV(base_model, method=method, cv=cv)
