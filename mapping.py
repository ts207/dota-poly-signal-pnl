from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml

SUPPORTED_MARKET_TYPES = {"MAP_WINNER"}
PLACEHOLDER_MARKERS = {
    "TOKEN_ID_HERE",
    "MATCH_OR_LOBBY_ID_HERE",
    "STEAM_MATCH_OR_LOBBY_ID_HERE",
    "POLY_MARKET_ID_HERE",
}


@dataclass(frozen=True)
class MappingError:
    index: int
    name: str | None
    reason: str


def _has_placeholder(value: Any) -> bool:
    text = str(value or "")
    return any(marker in text for marker in PLACEHOLDER_MARKERS)


def load_mappings(filename: str = "markets.yaml") -> list[dict]:
    if not os.path.exists(filename):
        return []
    with open(filename, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("markets", []) or []


def validate_mapping(mapping: dict, index: int = 0) -> tuple[bool, MappingError | None]:
    required = ["market_type", "yes_team", "yes_token_id", "no_token_id", "dota_match_id"]
    missing = [field for field in required if not mapping.get(field)]
    if missing:
        return False, MappingError(index=index, name=mapping.get("name"), reason=f"missing: {', '.join(missing)}")

    for field in ["yes_token_id", "no_token_id", "dota_match_id", "market_id", "condition_id"]:
        if _has_placeholder(mapping.get(field)):
            return False, MappingError(index=index, name=mapping.get("name"), reason=f"placeholder value in {field}")

    market_type = str(mapping.get("market_type", "")).upper()
    if market_type not in SUPPORTED_MARKET_TYPES:
        return False, MappingError(index=index, name=mapping.get("name"), reason=f"unsupported market_type={market_type}")

    try:
        confidence = float(mapping.get("confidence", 0))
    except (TypeError, ValueError):
        return False, MappingError(index=index, name=mapping.get("name"), reason="confidence is not numeric")

    if confidence < 0.98:
        return False, MappingError(index=index, name=mapping.get("name"), reason="confidence below 0.98")

    mapping["market_type"] = market_type
    mapping["confidence"] = confidence
    mapping["yes_token_id"] = str(mapping["yes_token_id"])
    mapping["no_token_id"] = str(mapping["no_token_id"])
    mapping["dota_match_id"] = str(mapping["dota_match_id"])
    return True, None


def load_valid_mappings(filename: str = "markets.yaml") -> tuple[list[dict], list[MappingError]]:
    raw = load_mappings(filename)
    valid: list[dict] = []
    errors: list[MappingError] = []

    for i, mapping in enumerate(raw):
        mapping = dict(mapping)  # copy so validate_mapping's normalisation doesn't mutate the original
        ok, err = validate_mapping(mapping, i)
        if ok:
            valid.append(mapping)
        elif err:
            errors.append(err)

    return valid, errors
