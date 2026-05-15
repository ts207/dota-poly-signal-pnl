#!/usr/bin/env python3
"""Backfill radiant_win labels for logged match_ids.

Reads match IDs from live feature logs, skips IDs already present in the labels
CSV, fetches final outcomes from OpenDota or Steam, and appends verified labels.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


OPENDOTA_MATCH_URL = "https://api.opendota.com/api/matches/{match_id}"
STEAM_MATCH_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/"


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill labels/match_results.csv from logged match IDs.")
    parser.add_argument("--features", default="logs/rich_context.csv")
    parser.add_argument("--labels", default="labels/match_results.csv")
    parser.add_argument("--source", choices=["opendota", "steam", "both"], default="both")
    parser.add_argument("--sleep-sec", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    feature_ids = read_match_ids(Path(args.features))
    labels_path = Path(args.labels)
    existing = read_labels(labels_path)
    missing = [match_id for match_id in feature_ids if match_id not in existing]

    print(f"feature_match_ids={len(feature_ids)} existing_labels={len(existing)} missing={len(missing)}")
    if not missing:
        return

    steam_key = os.getenv("STEAM_API_KEY")
    additions: list[dict[str, str]] = []
    for idx, match_id in enumerate(missing, start=1):
        result = fetch_radiant_win(match_id, source=args.source, steam_key=steam_key)
        if result is None:
            print(f"{idx}/{len(missing)} {match_id}: unresolved")
        else:
            additions.append({"match_id": match_id, "radiant_win": "1" if result else "0"})
            print(f"{idx}/{len(missing)} {match_id}: radiant_win={int(result)}")
        if idx < len(missing) and args.sleep_sec > 0:
            time.sleep(args.sleep_sec)

    if args.dry_run:
        print(f"dry_run=true additions={len(additions)}")
        return

    append_labels(labels_path, additions)
    print(f"appended={len(additions)} labels_path={labels_path}")


def read_match_ids(path: Path) -> list[str]:
    ids: set[str] = set()
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "match_id" not in (reader.fieldnames or []):
            raise RuntimeError(f"{path} must contain match_id")
        for row in reader:
            match_id = str(row.get("match_id") or "").strip()
            if match_id:
                ids.add(match_id)
    return sorted(ids, key=int)


def read_labels(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "match_id" not in (reader.fieldnames or []) or "radiant_win" not in (reader.fieldnames or []):
            raise RuntimeError(f"{path} must contain match_id and radiant_win")
        return {
            str(row.get("match_id") or "").strip(): str(row.get("radiant_win") or "").strip()
            for row in reader
            if str(row.get("match_id") or "").strip()
        }


def fetch_radiant_win(match_id: str, *, source: str, steam_key: str | None) -> bool | None:
    if source in ("opendota", "both"):
        result = fetch_opendota(match_id)
        if result is not None:
            return result
    if source in ("steam", "both") and steam_key and steam_key != "replace_me":
        return fetch_steam(match_id, steam_key)
    return None


def fetch_opendota(match_id: str) -> bool | None:
    data = get_json(OPENDOTA_MATCH_URL.format(match_id=urllib.parse.quote(match_id)))
    value = data.get("radiant_win")
    return value if isinstance(value, bool) else None


def fetch_steam(match_id: str, steam_key: str) -> bool | None:
    url = STEAM_MATCH_URL + "?" + urllib.parse.urlencode({"key": steam_key, "match_id": match_id})
    data = get_json(url)
    result = data.get("result") if isinstance(data.get("result"), dict) else data
    value = result.get("radiant_win") if isinstance(result, dict) else None
    return value if isinstance(value, bool) else None


def get_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "dota-poly-signal-pnl/label-backfill"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"fetch_error url={redact_key(url)} error={type(exc).__name__}: {exc}")
        return {}


def append_labels(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["match_id", "radiant_win"])
        if needs_header:
            writer.writeheader()
        writer.writerows(rows)


def redact_key(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted = [(key, "***" if key == "key" else value) for key, value in params]
    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(redacted)))


if __name__ == "__main__":
    main()
