from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

from .calibrate import calibrated_classifier, calibration_metrics
from .features import DEFAULT_FEATURE_COLUMNS, row_to_features
from .schemas import FEATURE_SCHEMA_VERSION, ModelMetadata, PHASES, phase_for_duration

MIN_MATCH_GROUPS_PER_PHASE = 50
MIN_SNAPSHOTS_PER_PHASE = 500


def load_rows(path: str | Path) -> list[dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def train_phase_models(
    rows: list[dict[str, Any]],
    target_name: str = "radiant_win",
    *,
    min_match_groups: int = MIN_MATCH_GROUPS_PER_PHASE,
    min_snapshots: int = MIN_SNAPSHOTS_PER_PHASE,
    calibration_method: str | None = "sigmoid",
) -> dict[str, Any]:
    from sklearn.ensemble import ExtraTreesClassifier
    from sklearn.model_selection import GroupShuffleSplit

    artifacts: dict[str, Any] = {"models": {}, "metadata": {}}
    metrics: dict[str, Any] = {}
    top_features: dict[str, list[str]] = {}
    phase_counts: dict[str, dict[str, int]] = {}

    for phase in PHASES:
        phase_rows = [row for row in rows if phase_for_duration(row.get("game_time_sec")) == phase]
        phase_rows = [row for row in phase_rows if row.get(target_name) not in (None, "")]
        groups = [str(row.get("match_id") or "") for row in phase_rows]
        unique_groups = {g for g in groups if g}
        phase_counts[phase] = {"rows": len(phase_rows), "matches": len(unique_groups)}

        if len(phase_rows) < min_snapshots:
            metrics[phase] = {"skipped": "not_enough_rows"}
            continue
        if len(unique_groups) < min_match_groups:
            metrics[phase] = {"skipped": "not_enough_match_groups"}
            continue

        X = [row_to_features(row, DEFAULT_FEATURE_COLUMNS) for row in phase_rows]
        y = [int(float(row[target_name])) for row in phase_rows]
        if len(set(y)) < 2:
            metrics[phase] = {"skipped": "one_target_class"}
            continue
        if not _group_labels_have_two_classes(groups, y):
            metrics[phase] = {"skipped": "insufficient_class_coverage_by_group"}
            continue

        splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=1)
        train_idx, test_idx = next(splitter.split(X, y, groups))
        train_groups = {groups[i] for i in train_idx}
        test_groups = {groups[i] for i in test_idx}
        overlap = train_groups & test_groups
        if overlap:
            raise RuntimeError(f"match_id leakage in {phase}: {sorted(overlap)[:5]}")
        y_train = [y[i] for i in train_idx]
        y_test = [y[i] for i in test_idx]
        if len(set(y_train)) < 2:
            metrics[phase] = {"skipped": "train_split_one_class"}
            continue
        if len(set(y_test)) < 2:
            metrics[phase] = {"skipped": "test_split_one_class"}
            continue
        if calibration_method and min(y_train.count(0), y_train.count(1)) < 3:
            metrics[phase] = {"skipped": "not_enough_train_class_rows_for_calibration"}
            continue

        base_model = ExtraTreesClassifier(
            n_estimators=300,
            criterion="entropy",
            random_state=1,
            class_weight="balanced_subsample",
        )
        if calibration_method:
            model = calibrated_classifier(base_model, method=calibration_method, cv=3)
        else:
            model = base_model

        model.fit([X[i] for i in train_idx], y_train)
        probs = [float(p[1]) for p in model.predict_proba([X[i] for i in test_idx])]
        metrics[phase] = {
            **calibration_metrics(y_test, probs),
            "rows": len(phase_rows),
            "matches": len(unique_groups),
            "test_rows": len(test_idx),
            "calibration_method": calibration_method or "none",
        }
        artifacts["models"][phase] = model

        # Extract feature importances from the fitted model
        import numpy as np
        if hasattr(model, "calibrated_classifiers_"):
            # Average importances across all CV folds
            all_importances = []
            for clf in model.calibrated_classifiers_:
                if hasattr(clf, "estimator") and hasattr(clf.estimator, "feature_importances_"):
                    all_importances.append(clf.estimator.feature_importances_)
                elif hasattr(clf, "base_estimator") and hasattr(clf.base_estimator, "feature_importances_"):
                    all_importances.append(clf.base_estimator.feature_importances_)
            
            if all_importances:
                importances = np.mean(all_importances, axis=0)
            else:
                importances = []
        else:
            importances = getattr(model, "feature_importances_", [])

        if len(importances) > 0:
            ranked = sorted(zip(DEFAULT_FEATURE_COLUMNS, importances), key=lambda x: x[1], reverse=True)
            top_features[phase] = [name for name, _ in ranked[:10]]
        else:
            top_features[phase] = []

    artifacts["metadata"] = ModelMetadata(
        schema_version=FEATURE_SCHEMA_VERSION,
        phase="all",
        feature_names=DEFAULT_FEATURE_COLUMNS,
        target_name=target_name,
        estimator="ExtraTreesClassifier",
        metrics=metrics,
    ).to_dict()
    artifacts["metadata"]["top_features"] = top_features
    artifacts["metadata"]["phase_counts"] = phase_counts
    artifacts["metadata"]["training_rows"] = len(rows)
    artifacts["metadata"]["training_match_count"] = len({str(row.get("match_id") or "") for row in rows if row.get("match_id")})
    artifacts["metadata"]["calibration_method"] = calibration_method or "none"
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


def assert_trainable_artifact(artifacts: dict[str, Any]) -> None:
    if artifacts.get("models"):
        return

    metrics = artifacts.get("metadata", {}).get("metrics", {})
    reasons = ", ".join(
        f"{phase}={detail.get('skipped', 'unknown')}"
        for phase, detail in sorted(metrics.items())
        if isinstance(detail, dict)
    )
    raise RuntimeError(f"no phase models trained; refusing to save empty artifact ({reasons})")


def _assert_group_split_possible(groups: list[str]) -> None:
    unique = {g for g in groups if g}
    if len(unique) < 2:
        raise RuntimeError("need at least two match_id groups; row-level random splits are forbidden")


def _group_labels_have_two_classes(groups: list[str], y: list[int]) -> bool:
    labels_by_group: dict[str, set[int]] = {}
    for group, label in zip(groups, y):
        labels_by_group.setdefault(group, set()).add(label)
    group_level_labels = {next(iter(labels)) for labels in labels_by_group.values() if len(labels) == 1}
    return len(group_level_labels) >= 2 or len({label for labels in labels_by_group.values() for label in labels}) >= 2


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv")
    parser.add_argument("--target", default="radiant_win")
    parser.add_argument("--output", default="dota_fair_model/models/dota_fair.joblib")
    parser.add_argument("--min-match-groups", type=int, default=MIN_MATCH_GROUPS_PER_PHASE)
    parser.add_argument("--min-snapshots", type=int, default=MIN_SNAPSHOTS_PER_PHASE)
    parser.add_argument("--calibration-method", choices=["sigmoid", "isotonic", "none"], default="sigmoid")
    parser.add_argument("--allow-empty-artifact", action="store_true")
    args = parser.parse_args()

    calibration_method = None if args.calibration_method == "none" else args.calibration_method
    artifacts = train_phase_models(
        load_rows(args.input_csv),
        args.target,
        min_match_groups=args.min_match_groups,
        min_snapshots=args.min_snapshots,
        calibration_method=calibration_method,
    )
    if not args.allow_empty_artifact:
        try:
            assert_trainable_artifact(artifacts)
        except RuntimeError as exc:
            print(f"CRITICAL: {exc}", file=sys.stderr)
            sys.exit(1)
    save_artifacts(artifacts, args.output)


if __name__ == "__main__":
    main()
