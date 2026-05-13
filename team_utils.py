from __future__ import annotations

import re

_DROP_TOKENS = {
    "team", "esports", "esport", "gaming", "e-sports", "e-sport",
    "club", "dota", "dota2",
}

_ALIASES = {
    "navi": "natus vincere",
    "natus vincere": "natus vincere",
    "ngx": "nigma galaxy",
    "nigma": "nigma galaxy",
    "flc": "falcons",
    "team falcons": "falcons",
    "falcons": "falcons",
    "bb": "betboom",
    "bb4": "betboom",
    "betboom team": "betboom",
    "ts": "spirit",
    "ts8": "spirit",
    "team spirit": "spirit",
    "spirit": "spirit",
    "liquid": "liquid",
    "team liquid": "liquid",
    "vp": "virtus pro",
    "virtus pro": "virtus pro",
    "virtuspro": "virtus pro",
    "pari": "parivision",
    "pari vision": "parivision",
    "parivision": "parivision",
}


def norm_team(value: str | None) -> str:
    """Normalize team names across Steam and Polymarket labels.

    This is deliberately conservative: it removes generic esports/team words,
    folds punctuation, and applies a small alias table for common Dota names.
    """
    text = (value or "").casefold().replace("&", " and ")
    text = text.replace(".", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _ALIASES.get(text, text)
    tokens = [tok for tok in text.split() if tok not in _DROP_TOKENS]
    normalized = " ".join(tokens) or text
    return _ALIASES.get(normalized, normalized)


def teams_match(a: str | None, b: str | None) -> bool:
    na = norm_team(a)
    nb = norm_team(b)
    if not na or not nb:
        return False
    return na == nb
