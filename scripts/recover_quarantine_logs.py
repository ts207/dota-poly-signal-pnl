from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_EXCLUDE_MARKERS = (
    "dead_liveleague_context",
    "negative_liveleague_age",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def csv_base_name(path: Path) -> str | None:
    name = path.name
    if name.endswith(".csv"):
        return name
    if ".csv." in name:
        return name.split(".csv.", 1)[0] + ".csv"
    return None


def target_name_for_source(base: str, headers: dict[str, list[str]]) -> str | None:
    if base in headers:
        return base
    if base == "liveleague_features.csv" and "rich_context.csv" in headers:
        return "rich_context.csv"
    return None


def row_hash(row: Iterable[str]) -> str:
    return hashlib.sha256(json.dumps(list(row), separators=(",", ":")).encode("utf-8")).hexdigest()


def read_header(path: Path) -> list[str] | None:
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return next(csv.reader(f), None)
    except (OSError, UnicodeDecodeError, csv.Error):
        return None


def iter_csv_rows(path: Path) -> tuple[list[str] | None, list[list[str]], str | None]:
    try:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return None, [], None
            return header, list(reader), None
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        return None, [], str(exc)


def is_excluded(path: Path, markers: tuple[str, ...]) -> str | None:
    text = str(path)
    for marker in markers:
        if marker and marker in text:
            return marker
    return None


def discover_current_headers(logs_dir: Path) -> dict[str, list[str]]:
    headers: dict[str, list[str]] = {}
    for path in sorted(logs_dir.glob("*.csv")):
        header = read_header(path)
        if header:
            headers[path.name] = header
    return headers


def recover(
    logs_dir: Path,
    quarantine_dir: Path,
    output_dir: Path,
    exclude_markers: tuple[str, ...],
    include_current: bool,
    source_dirs: tuple[Path, ...] = (),
) -> dict:
    headers = discover_current_headers(logs_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for stale in output_dir.glob("*.csv"):
        stale.unlink()

    recovered_rows: dict[str, list[list[str]]] = {name: [] for name in headers}
    seen: dict[str, set[str]] = {name: set() for name in headers}
    manifest = {
        "created_at_utc": utc_now_iso(),
        "logs_dir": str(logs_dir),
        "quarantine_dir": str(quarantine_dir),
        "output_dir": str(output_dir),
        "include_current": include_current,
        "source_dirs": [str(path) for path in source_dirs],
        "exclude_markers": list(exclude_markers),
        "files": {},
        "skipped": [],
    }

    stats = defaultdict(lambda: {
        "current_rows_read": 0,
        "current_rows_written": 0,
        "source_rows_read": 0,
        "source_rows_written": 0,
        "quarantine_rows_read": 0,
        "quarantine_rows_written": 0,
        "duplicates_skipped": 0,
        "bad_width_rows_skipped": 0,
        "sources": defaultdict(int),
    })

    if include_current:
        for name in sorted(headers):
            path = logs_dir / name
            header, rows, error = iter_csv_rows(path)
            if error or header != headers[name]:
                manifest["skipped"].append({
                    "path": str(path),
                    "reason": f"could not read current CSV: {error}" if error else "current header changed while reading",
                })
                continue
            for row in rows:
                stats[name]["current_rows_read"] += 1
                if len(row) != len(headers[name]):
                    stats[name]["bad_width_rows_skipped"] += 1
                    continue
                digest = row_hash(row)
                if digest in seen[name]:
                    stats[name]["duplicates_skipped"] += 1
                    continue
                seen[name].add(digest)
                recovered_rows[name].append(row)
                stats[name]["current_rows_written"] += 1

    def ingest_source_path(path: Path, read_key: str, write_key: str) -> None:
        base = csv_base_name(path)
        if not base:
            return
        target = target_name_for_source(base, headers)
        if target is None:
            manifest["skipped"].append({
                "path": str(path),
                "reason": "no current target CSV with matching base name",
            })
            return
        header, rows, error = iter_csv_rows(path)
        if error:
            manifest["skipped"].append({"path": str(path), "reason": f"read error: {error}"})
            return
        if header == headers[target]:
            mode = "exact"
        elif header and all(column in headers[target] for column in header):
            mode = "project"
        else:
            manifest["skipped"].append({
                "path": str(path),
                "reason": "header mismatch",
                "source_columns": len(header or []),
                "target_columns": len(headers[target]),
                "target": target,
            })
            return
        for row in rows:
            stats[target][read_key] += 1
            if len(row) != len(header):
                stats[target]["bad_width_rows_skipped"] += 1
                continue
            if mode == "project":
                source_row = dict(zip(header, row))
                row = [source_row.get(column, "") for column in headers[target]]
            digest = row_hash(row)
            if digest in seen[target]:
                stats[target]["duplicates_skipped"] += 1
                continue
            seen[target].add(digest)
            recovered_rows[target].append(row)
            stats[target][write_key] += 1
            stats[target]["sources"][str(path)] += 1

    for source_dir in source_dirs:
        for path in sorted(source_dir.glob("*.csv")):
            ingest_source_path(path, "source_rows_read", "source_rows_written")

    for path in sorted(quarantine_dir.rglob("*")):
        if not path.is_file():
            continue
        marker = is_excluded(path, exclude_markers)
        if marker:
            manifest["skipped"].append({
                "path": str(path),
                "reason": f"excluded by marker: {marker}",
            })
            continue
        ingest_source_path(path, "quarantine_rows_read", "quarantine_rows_written")

    for name in sorted(headers):
        rows = recovered_rows[name]
        if not rows and not include_current:
            continue
        out = output_dir / name
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers[name])
            writer.writerows(rows)
        file_stats = dict(stats[name])
        file_stats["sources"] = dict(file_stats["sources"])
        file_stats["total_rows_written"] = len(rows)
        file_stats["output_path"] = str(out)
        manifest["files"][name] = file_stats

    manifest_path = output_dir / "recovery_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover compatible quarantined log rows into a separate merged log directory.")
    parser.add_argument("--logs-dir", default="logs", type=Path)
    parser.add_argument("--quarantine-dir", default="logs/quarantine_bad_data", type=Path)
    parser.add_argument("--output-dir", default="logs/recovered", type=Path)
    parser.add_argument("--source-dir", action="append", default=[], type=Path, help="Additional flat CSV directory to merge before quarantine rows.")
    parser.add_argument(
        "--exclude-marker",
        action="append",
        default=list(DEFAULT_EXCLUDE_MARKERS),
        help="Substring in a quarantine path to exclude. Can be passed multiple times.",
    )
    parser.add_argument("--quarantine-only", action="store_true", help="Do not seed outputs with current logs/*.csv rows.")
    args = parser.parse_args()

    manifest = recover(
        logs_dir=args.logs_dir,
        quarantine_dir=args.quarantine_dir,
        output_dir=args.output_dir,
        exclude_markers=tuple(args.exclude_marker),
        include_current=not args.quarantine_only,
        source_dirs=tuple(args.source_dir),
    )

    print(f"wrote {manifest['output_dir']}")
    for name, stats in sorted(manifest["files"].items()):
        print(
            f"{name}: total={stats['total_rows_written']} "
            f"current={stats['current_rows_written']} "
            f"source={stats['source_rows_written']} "
            f"recovered={stats['quarantine_rows_written']} "
            f"dupes={stats['duplicates_skipped']} "
            f"bad_width={stats['bad_width_rows_skipped']}"
        )
    print(f"skipped_sources={len(manifest['skipped'])}")


if __name__ == "__main__":
    main()
