from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from .calibrate import calibration_metrics
from .features import DEFAULT_FEATURE_COLUMNS, row_to_features
from .schemas import FEATURE_SCHEMA_VERSION, ModelMetadata, PHASES, phase_for_duration


def load_rows(path: str | Path) -> list[dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def train_phase_models(rows: list[dict[str, Any]], target_name: str = "radiant_win") -> dict[str, Any]:
    from sklearn.ensemble import ExtraTreesClassifier
    from sklearn.model_selection import GroupShuffleSplit

    artifacts: dict[str, Any] = {"models": {}, "metadata": {}}
    metrics: dict[str, Any] = {}
    top_features: dict[str, list[str]] = {}

    for phase in PHASES:
        phase_rows = [row for row in rows if phase_for_duration(row.get("game_time_sec")) == phase]
        phase_rows = [row for row in phase_rows if row.get(target_name) not in (None, "")]
        if len(phase_rows) < 4:
            metrics[phase] = {"skipped": "not_enough_rows"}
            continue

        X = [row_to_features(row, DEFAULT_FEATURE_COLUMNS) for row in phase_rows]
        y = [int(float(row[target_name])) for row in phase_rows]
        groups = [str(row.get("match_id") or "") for row in phase_rows]
        _assert_group_split_possible(groups)

        splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=1)
        train_idx, test_idx = next(splitter.split(X, y, groups))
        train_groups = {groups[i] for i in train_idx}
        test_groups = {groups[i] for i in test_idx}
        overlap = train_groups & test_groups
        if overlap:
            raise RuntimeError(f"match_id leakage in {phase}: {sorted(overlap)[:5]}")

        model = ExtraTreesClassifier(
            n_estimators=300,
            criterion="entropy",
            random_state=1,
            class_weight="balanced_subsample",
        )
        model.fit([X[i] for i in train_idx], [y[i] for i in train_idx])
        probs = [float(p[1]) for p in model.predict_proba([X[i] for i in test_idx])]
        metrics[phase] = calibration_metrics([y[i] for i in test_idx], probs)
        artifacts["models"][phase] = model
        importances = getattr(model, "feature_importances_", [])
        ranked = sorted(zip(DEFAULT_FEATURE_COLUMNS, importances), key=lambda x: x[1], reverse=True)
        top_features[phase] = [name for name, _ in ranked[:10]]

    artifacts["metadata"] = ModelMetadata(
        schema_version=FEATURE_SCHEMA_VERSION,
        phase="all",
        feature_names=DEFAULT_FEATURE_COLUMNS,
        target_name=target_name,
        estimator="ExtraTreesClassifier",
        metrics=metrics,
    ).to_dict()
    artifacts["metadata"]["top_features"] = top_features
    return artifacts


def save_artifacts(artifacts: dict[str, Any], output: str | Path) -> None:
    import joblib

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifacts, output)
    output.with_suffix(".metadata.json").write_text(
        json.dumps(artifacts["metadata"], indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _assert_group_split_possible(groups: list[str]) -> None:
    unique = {g for g in groups if g}
    if len(unique) < 2:
        raise RuntimeError("need at least two match_id groups; row-level random splits are forbidden")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv")
    parser.add_argument("--target", default="radiant_win")
    parser.add_argument("--output", default="dota_fair_model/models/dota_fair.joblib")
    args = parser.parse_args()

    artifacts = train_phase_models(load_rows(args.input_csv), args.target)
    save_artifacts(artifacts, args.output)


if __name__ == "__main__":
    main()
