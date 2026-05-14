from __future__ import annotations

import csv
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


INPUT = Path("logs/event_qualified_price_response.csv")
OUTPUT = Path("logs/event_qualified_price_response_dedup.csv")


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_ts(value: str | None) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def fnum(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def stat(values: list[float | None]) -> str:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return "n/a"
    vals = sorted(vals)
    return (
        f"n={len(vals)} min={vals[0]:.3f} "
        f"p25={vals[len(vals) // 4]:.3f} med={statistics.median(vals):.3f} "
        f"p75={vals[(3 * len(vals)) // 4]:.3f} max={vals[-1]:.3f}"
    )


def dedupe_rows(rows: list[dict]) -> list[dict]:
    keys = [
        "match_id", "market_type", "market_name", "event_type",
        "game_time_sec", "direction", "expected_direction",
    ]
    out = []
    seen = set()
    for row in sorted(rows, key=lambda r: parse_ts(r.get("event_timestamp_utc"))):
        key = tuple(row.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def has_value(row: dict, key: str) -> bool:
    return row.get(key) not in (None, "")


def print_group(label: str, rows: list[dict]) -> None:
    n = len(rows)
    expected = sum(has_value(r, "expected_lag_s") for r in rows)
    any_move = sum(has_value(r, "any_lag_s") for r in rows)
    pre = sum(has_value(r, "pre_expected_move") for r in rows)
    clean = sum(has_value(r, "expected_lag_s") and not has_value(r, "pre_expected_move") for r in rows)
    lags = [fnum(r.get("expected_lag_s")) for r in rows]
    print(
        f"{label}: n={n} expected={expected}/{n} any={any_move}/{n} "
        f"pre={pre}/{n} clean={clean}/{n} lag={stat(lags)}"
    )


def main() -> None:
    rows = read_csv(INPUT)
    if not rows:
        print(f"missing or empty input: {INPUT}")
        return

    deduped = dedupe_rows(rows)
    write_csv(OUTPUT, deduped)
    print(f"wrote {OUTPUT}: {len(deduped)} rows from {len(rows)}")

    print_group("overall", deduped)

    print("\nby event type")
    by_event: dict[str, list[dict]] = defaultdict(list)
    for row in deduped:
        by_event[row.get("event_type") or ""].append(row)
    for event_type, event_rows in sorted(by_event.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print_group(event_type, event_rows)

    print("\nby severity")
    by_severity: dict[str, list[dict]] = defaultdict(list)
    for row in deduped:
        by_severity[row.get("severity") or ""].append(row)
    for severity, severity_rows in sorted(by_severity.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print_group(severity, severity_rows)

    print("\nclean event mix")
    clean_events = [
        r.get("event_type") for r in deduped
        if has_value(r, "expected_lag_s") and not has_value(r, "pre_expected_move")
    ]
    for event_type, count in Counter(clean_events).most_common():
        print(f"  {event_type}: {count}")


if __name__ == "__main__":
    main()
