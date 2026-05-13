from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from team_utils import norm_team

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


@dataclass
class MappingValidationResult:
    mapping_confidence: float = 0.0
    mapping_errors: list[str] = field(default_factory=list)
    series_id: str | None = None
    series_type: int | None = None
    game_number: int | None = None
    team_id_match: bool | None = None
    market_game_number_match: bool | None = None
    duplicate_match_id_error: bool = False

    @property
    def ok(self) -> bool:
        return not self.mapping_errors and self.mapping_confidence == 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mapping_confidence": self.mapping_confidence,
            "mapping_errors": ";".join(self.mapping_errors),
            "series_id": self.series_id,
            "series_type": self.series_type,
            "game_number": self.game_number,
            "team_id_match": self.team_id_match,
            "market_game_number_match": self.market_game_number_match,
            "duplicate_match_id_error": self.duplicate_match_id_error,
        }


def has_placeholder(value: Any) -> bool:
    text = str(value or "")
    return any(marker in text for marker in PLACEHOLDER_MARKERS)


def infer_game_number(mapping: dict) -> int | None:
    raw = mapping.get("game_number")
    if raw not in (None, ""):
        try:
            value = int(raw)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    text = " ".join(str(mapping.get(k) or "") for k in ("name", "question", "market_title", "title"))
    match = re.search(r"\bgame\s*([1-5])\b", text, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _confidence(mapping: dict) -> float:
    try:
        return float(mapping.get("confidence", 0.0))
    except (TypeError, ValueError):
        return 0.0


def validate_mapping_schema(mapping: dict, index: int = 0) -> MappingValidationResult:
    result = MappingValidationResult(
        mapping_confidence=1.0 if _confidence(mapping) == 1.0 else 0.0,
        series_id=_to_str(mapping.get("series_id")),
        series_type=_to_int(mapping.get("series_type")),
        game_number=infer_game_number(mapping),
    )

    required = ["market_type", "yes_team", "yes_token_id", "no_team", "no_token_id", "dota_match_id"]
    missing = [field for field in required if not mapping.get(field)]
    if missing:
        result.mapping_errors.append(f"missing: {', '.join(missing)}")

    for field_name in ["yes_token_id", "no_token_id", "dota_match_id", "market_id", "condition_id"]:
        if has_placeholder(mapping.get(field_name)):
            result.mapping_errors.append(f"placeholder value in {field_name}")

    market_type = str(mapping.get("market_type", "")).upper()
    if market_type not in SUPPORTED_MARKET_TYPES:
        result.mapping_errors.append(f"unsupported market_type={market_type}")

    if mapping.get("confidence") in (None, ""):
        result.mapping_errors.append("missing confidence")
    elif _confidence(mapping) != 1.0:
        result.mapping_errors.append("confidence below required 1.0")

    yes_token = str(mapping.get("yes_token_id") or "")
    no_token = str(mapping.get("no_token_id") or "")
    if yes_token and no_token and yes_token == no_token:
        result.mapping_errors.append("yes_token_id equals no_token_id")

    yes_team = norm_team(mapping.get("yes_team") or "")
    no_team = norm_team(mapping.get("no_team") or "")
    if yes_team and no_team and yes_team == no_team:
        result.mapping_errors.append("yes_team equals no_team")

    if result.mapping_errors:
        result.mapping_confidence = 0.0
    return result


def validate_active_mappings(mappings: list[dict]) -> list[MappingValidationResult]:
    results = [validate_mapping_schema(mapping, i) for i, mapping in enumerate(mappings)]
    by_match: dict[str, list[int]] = {}
    for i, mapping in enumerate(mappings):
        mid = str(mapping.get("dota_match_id") or "")
        if mid:
            by_match.setdefault(mid, []).append(i)

    for mid, indexes in by_match.items():
        if len(indexes) <= 1:
            continue
        game_numbers = {results[i].game_number for i in indexes}
        names = ", ".join(str(mappings[i].get("name") or f"#{i}") for i in indexes)
        if len(game_numbers) > 1 or any(gn is not None for gn in game_numbers):
            for i in indexes:
                results[i].duplicate_match_id_error = True
                results[i].mapping_confidence = 0.0
                results[i].mapping_errors.append(f"duplicate dota_match_id={mid} across Game N markets: {names}")

    return results


def validate_mapping_identity(mapping: dict, game: dict, liveleague_context: dict | None = None) -> MappingValidationResult:
    result = validate_mapping_schema(mapping)
    ctx = liveleague_context or game.get("liveleague_context") or {}

    result.series_id = _to_str(mapping.get("series_id") or ctx.get("series_id") or game.get("series_id"))
    result.series_type = _to_int(mapping.get("series_type") if mapping.get("series_type") is not None else ctx.get("series_type"))
    result.game_number = infer_game_number(mapping)

    yes_team = norm_team(mapping.get("yes_team") or "")
    no_team = norm_team(mapping.get("no_team") or "")
    radiant_team = norm_team(game.get("radiant_team") or ctx.get("radiant_team") or ctx.get("radiant_team_name") or "")
    dire_team = norm_team(game.get("dire_team") or ctx.get("dire_team") or ctx.get("dire_team_name") or "")

    if radiant_team and dire_team and yes_team and no_team:
        normal = yes_team == radiant_team and no_team == dire_team
        reversed_side = yes_team == dire_team and no_team == radiant_team
        if not (normal or reversed_side):
            result.mapping_errors.append(
                f"team_name_mismatch yes={yes_team} no={no_team} radiant={radiant_team} dire={dire_team}"
            )

    mapped_league = _to_str(mapping.get("league_id"))
    game_league = _to_str(game.get("league_id") or ctx.get("league_id"))
    if mapped_league and game_league and mapped_league != game_league and mapped_league != "0" and game_league != "0":
        result.mapping_errors.append(f"league_id_mismatch mapping={mapped_league} game={game_league}")

    mapped_series = _to_str(mapping.get("series_id"))
    game_series = _to_str(ctx.get("series_id") or game.get("series_id"))
    if mapped_series and game_series and mapped_series != game_series:
        result.mapping_errors.append(f"series_id_mismatch mapping={mapped_series} game={game_series}")

    if result.game_number is not None and result.series_type is not None:
        max_games = {0: 1, 1: 3, 2: 3, 3: 5}.get(result.series_type)
        if max_games is not None:
            result.market_game_number_match = result.game_number <= max_games
            if not result.market_game_number_match:
                result.mapping_errors.append(
                    f"game_number={result.game_number} incompatible with series_type={result.series_type}"
                )

    result.team_id_match = _team_ids_match(mapping, game, ctx)
    if result.team_id_match is False:
        result.mapping_errors.append("team_id_mismatch")

    if result.mapping_errors:
        result.mapping_confidence = 0.0
    return result


def _team_ids_match(mapping: dict, game: dict, ctx: dict) -> bool | None:
    yes_id = _to_str(mapping.get("yes_team_id"))
    no_id = _to_str(mapping.get("no_team_id"))
    radiant_id = _to_str(game.get("radiant_team_id") or ctx.get("radiant_team_id"))
    dire_id = _to_str(game.get("dire_team_id") or ctx.get("dire_team_id"))

    if not any((yes_id, no_id, radiant_id, dire_id)):
        return None
    if not (yes_id and no_id and radiant_id and dire_id):
        return None

    yes_team = norm_team(mapping.get("yes_team") or "")
    no_team = norm_team(mapping.get("no_team") or "")
    radiant_team = norm_team(game.get("radiant_team") or ctx.get("radiant_team") or "")
    dire_team = norm_team(game.get("dire_team") or ctx.get("dire_team") or "")

    if yes_team == radiant_team and no_team == dire_team:
        return yes_id == radiant_id and no_id == dire_id
    if yes_team == dire_team and no_team == radiant_team:
        return yes_id == dire_id and no_id == radiant_id
    return {yes_id, no_id} == {radiant_id, dire_id}


def result_to_error(index: int, mapping: dict, result: MappingValidationResult) -> MappingError:
    return MappingError(index=index, name=mapping.get("name"), reason="; ".join(result.mapping_errors))
