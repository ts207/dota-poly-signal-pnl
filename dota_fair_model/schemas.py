from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

FEATURE_SCHEMA_VERSION = "dota_fair_v1"
PHASES = ("early", "mid", "late")


@dataclass(frozen=True)
class ModelMetadata:
    schema_version: str
    phase: str
    feature_names: list[str]
    target_name: str
    estimator: str
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def phase_for_duration(duration_sec: int | float | None) -> str:
    if duration_sec is None:
        return "late"
    minutes = float(duration_sec) / 60.0
    if minutes < 10:
        return "early"
    if minutes < 25:
        return "mid"
    return "late"

