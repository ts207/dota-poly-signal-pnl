from __future__ import annotations

import os

import yaml

from mapping_validator import MappingError, has_placeholder, validate_active_mappings, validate_mapping_schema

DEFAULT_MARKETS_PATH = os.path.join(os.path.dirname(__file__), "markets.yaml")


def load_mappings(filename: str = DEFAULT_MARKETS_PATH) -> list[dict]:
    if not os.path.exists(filename):
        return []
    with open(filename, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("markets", []) or []


def validate_mapping(mapping: dict, index: int = 0) -> tuple[bool, MappingError | None]:
    result = validate_mapping_schema(mapping, index)
    if result.mapping_errors:
        return False, MappingError(index=index, name=mapping.get("name"), reason="; ".join(result.mapping_errors))

    market_type = str(mapping.get("market_type", "")).upper()
    confidence = float(mapping.get("confidence", 0))
    mapping["market_type"] = market_type
    mapping["confidence"] = confidence
    mapping["yes_token_id"] = str(mapping["yes_token_id"])
    mapping["no_token_id"] = str(mapping["no_token_id"])
    mapping["dota_match_id"] = str(mapping["dota_match_id"])
    return True, None


def load_valid_mappings(filename: str = DEFAULT_MARKETS_PATH) -> tuple[list[dict], list[MappingError]]:
    raw = load_mappings(filename)
    valid: list[dict] = []
    errors: list[MappingError] = []
    results = validate_active_mappings([dict(m) for m in raw])

    for i, (mapping, result) in enumerate(zip(raw, results)):
        mapping = dict(mapping)  # copy so validate_mapping's normalisation doesn't mutate the original
        if result.mapping_errors:
            errors.append(MappingError(index=i, name=mapping.get("name"), reason="; ".join(result.mapping_errors)))
            continue

        ok, err = validate_mapping(mapping, i)
        if not ok and err:
            errors.append(err)
            continue

        mapping["mapping_confidence"] = result.mapping_confidence
        mapping["mapping_errors"] = ""
        mapping["series_id"] = mapping.get("series_id") or result.series_id
        mapping["series_type"] = mapping.get("series_type") if mapping.get("series_type") is not None else result.series_type
        mapping["game_number"] = mapping.get("game_number") or result.game_number
        mapping["team_id_match"] = result.team_id_match
        mapping["market_game_number_match"] = result.market_game_number_match
        mapping["duplicate_match_id_error"] = result.duplicate_match_id_error
        valid.append(mapping)

    return valid, errors
