from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Iterable


SUMMARY_HEADERS = [
    "delay_ms",
    "attempts",
    "filled",
    "fill_rate",
    "notional_usd",
    "marked_positions",
    "bid_marked_pnl_usd",
    "bid_marked_pnl_pct",
    "markout_count",
    "markout_3s_avg",
    "markout_3s_median",
    "markout_3s_positive_rate",
    "markout_10s_avg",
    "markout_10s_median",
    "markout_10s_positive_rate",
    "markout_30s_avg",
    "markout_30s_median",
    "markout_30s_positive_rate",
    "stale_survival_count",
    "stale_survival_avg_ms",
    "stale_survival_median_ms",
    "stale_survived_delay_rate",
    "source_delay_count",
    "game_time_lag_sec_avg",
    "game_time_lag_sec_median",
    "game_time_lag_sec_p95",
    "stream_delay_s_avg",
    "stream_delay_s_median",
    "stream_delay_s_p95",
    "wall_clock_receive_gap_sec_avg",
    "wall_clock_receive_gap_sec_median",
    "wall_clock_receive_gap_sec_p95",
    "verdict",
]


@dataclass(frozen=True)
class Scenario:
    delay_ms: int
    path: Path


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fnum(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt(value, digits: int = 4) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def fmt_pct(value) -> str:
    if value is None:
        return ""
    return f"{value:.1%}"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def stats(values: Iterable[float | None]) -> dict[str, float | int | None]:
    clean = [v for v in values if v is not None]
    return {
        "count": len(clean),
        "avg": mean(clean) if clean else None,
        "median": median(clean) if clean else None,
        "p95": percentile(clean, 0.95),
    }


def positive_rate(values: Iterable[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return len([v for v in clean if v > 0]) / len(clean)


def discover_scenarios(sweep_dir: Path) -> list[Scenario]:
    scenarios = []
    pattern = re.compile(r"^delay_(\d+)ms$")
    for child in sweep_dir.iterdir() if sweep_dir.exists() else []:
        if not child.is_dir():
            continue
        match = pattern.match(child.name)
        if match:
            scenarios.append(Scenario(delay_ms=int(match.group(1)), path=child))
    return sorted(scenarios, key=lambda s: s.delay_ms)


def pnl_overall(logs_dir: Path) -> dict:
    rows = read_csv(logs_dir / "pnl_summary.csv")
    for row in rows:
        if (row.get("scope") or "").strip().lower() == "overall":
            return row
    return rows[0] if rows else {}


def summarize_scenario(scenario: Scenario) -> dict:
    logs_dir = scenario.path / "logs"

    latency_rows = read_csv(logs_dir / "latency.csv")
    result_rows = [r for r in latency_rows if r.get("decision") == "paper_entry_result"]
    attempts = len(result_rows)
    filled = len([r for r in result_rows if r.get("paper_entry_result") == "filled"])
    fill_rate = filled / attempts if attempts else None

    pnl = pnl_overall(logs_dir)

    markout_rows = read_csv(logs_dir / "markouts.csv")
    markout_3 = [fnum(r.get("markout_3s")) for r in markout_rows]
    markout_10 = [fnum(r.get("markout_10s")) for r in markout_rows]
    markout_30 = [fnum(r.get("markout_30s")) for r in markout_rows]
    s3 = stats(markout_3)
    s10 = stats(markout_10)
    s30 = stats(markout_30)

    survival_rows = read_csv(logs_dir / "stale_ask_survival.csv")
    survival_ms = [fnum(r.get("stale_ask_survival_ms")) for r in survival_rows]
    survival_clean = [v for v in survival_ms if v is not None]
    survival_stats = stats(survival_clean)
    survived_delay = (
        len([v for v in survival_clean if v >= scenario.delay_ms]) / len(survival_clean)
        if survival_clean
        else None
    )

    source_rows = read_csv(logs_dir / "source_delay.csv")
    game_lag = stats(fnum(r.get("game_time_lag_sec")) for r in source_rows)
    stream_delay = stats(fnum(r.get("stream_delay_s")) for r in source_rows)
    receive_gap = stats(fnum(r.get("wall_clock_receive_gap_sec")) for r in source_rows)

    bid_pnl = fnum(pnl.get("unrealized_pnl_usd"))
    verdict = build_verdict(scenario.delay_ms, attempts, filled, fill_rate, bid_pnl, survived_delay)

    return {
        "delay_ms": scenario.delay_ms,
        "attempts": attempts,
        "filled": filled,
        "fill_rate": fill_rate,
        "notional_usd": fnum(pnl.get("notional_usd")),
        "marked_positions": fnum(pnl.get("marked_positions")),
        "bid_marked_pnl_usd": bid_pnl,
        "bid_marked_pnl_pct": fnum(pnl.get("unrealized_pnl_pct")),
        "markout_count": len(markout_rows),
        "markout_3s_avg": s3["avg"],
        "markout_3s_median": s3["median"],
        "markout_3s_positive_rate": positive_rate(markout_3),
        "markout_10s_avg": s10["avg"],
        "markout_10s_median": s10["median"],
        "markout_10s_positive_rate": positive_rate(markout_10),
        "markout_30s_avg": s30["avg"],
        "markout_30s_median": s30["median"],
        "markout_30s_positive_rate": positive_rate(markout_30),
        "stale_survival_count": survival_stats["count"],
        "stale_survival_avg_ms": survival_stats["avg"],
        "stale_survival_median_ms": survival_stats["median"],
        "stale_survived_delay_rate": survived_delay,
        "source_delay_count": len(source_rows),
        "game_time_lag_sec_avg": game_lag["avg"],
        "game_time_lag_sec_median": game_lag["median"],
        "game_time_lag_sec_p95": game_lag["p95"],
        "stream_delay_s_avg": stream_delay["avg"],
        "stream_delay_s_median": stream_delay["median"],
        "stream_delay_s_p95": stream_delay["p95"],
        "wall_clock_receive_gap_sec_avg": receive_gap["avg"],
        "wall_clock_receive_gap_sec_median": receive_gap["median"],
        "wall_clock_receive_gap_sec_p95": receive_gap["p95"],
        "verdict": verdict,
    }


def build_verdict(
    delay_ms: int,
    attempts: int,
    filled: int,
    fill_rate: float | None,
    bid_pnl: float | None,
    survived_delay_rate: float | None,
) -> str:
    flags = []
    if delay_ms > 0 and filled > 0 and bid_pnl is not None and bid_pnl > 0:
        flags.append("passes PnL/fill sanity")
    if attempts == 0:
        flags.append("no paper attempts")
    elif filled == 0:
        flags.append("no fills")
    if bid_pnl is not None and bid_pnl <= 0:
        flags.append("non-positive bid-marked PnL")
    if fill_rate is not None and fill_rate < 0.25:
        flags.append("low fill rate")
    if survived_delay_rate is not None and survived_delay_rate < 0.5:
        flags.append("survival-at-delay collapsed")
    return "; ".join(flags) if flags else "neutral"


def write_summary_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h) for h in SUMMARY_HEADERS})


def markdown_table(rows: list[dict]) -> str:
    headers = [
        "delay",
        "attempts",
        "filled",
        "fill rate",
        "bid PnL",
        "PnL %",
        "m3 avg",
        "m10 avg",
        "m30 avg",
        "survive delay",
        "src lag p95",
        "verdict",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = [
            f"{row.get('delay_ms')}ms",
            fmt(row.get("attempts"), 0),
            fmt(row.get("filled"), 0),
            fmt_pct(row.get("fill_rate")),
            fmt(row.get("bid_marked_pnl_usd"), 2),
            fmt_pct(row.get("bid_marked_pnl_pct")),
            fmt(row.get("markout_3s_avg"), 4),
            fmt(row.get("markout_10s_avg"), 4),
            fmt(row.get("markout_30s_avg"), 4),
            fmt_pct(row.get("stale_survived_delay_rate")),
            fmt(row.get("game_time_lag_sec_p95"), 2),
            str(row.get("verdict") or ""),
        ]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_report(path: Path, rows: list[dict], sweep_dir: Path) -> None:
    pass_rows = [r for r in rows if "passes PnL/fill sanity" in str(r.get("verdict"))]
    flagged_rows = [
        r for r in rows
        if any(flag in str(r.get("verdict")) for flag in ("no fills", "low fill rate", "survival-at-delay collapsed", "non-positive"))
    ]

    lines = [
        "# Latency Validation Report",
        "",
        f"Sweep root: `{sweep_dir}`",
        "",
        "## Scenario Comparison",
        "",
        markdown_table(rows) if rows else "No delay scenario directories found.",
        "",
        "## Verdict",
        "",
    ]
    if pass_rows:
        delays = ", ".join(f"{r['delay_ms']}ms" for r in pass_rows)
        lines.append(f"PnL/fill sanity passed at: {delays}.")
    else:
        lines.append("No realistic-delay scenario passed PnL/fill sanity.")
    if flagged_rows:
        delays = ", ".join(f"{r['delay_ms']}ms" for r in flagged_rows)
        lines.append(f"Flagged scenarios: {delays}.")
    else:
        lines.append("No mechanical collapse flags were triggered.")

    lines.extend([
        "",
        "## Metric Definitions",
        "",
        "- Fill rate uses `latency.csv` rows where `decision == paper_entry_result`.",
        "- Bid-marked PnL uses the `overall` row from archived `pnl_summary.csv`.",
        "- Markouts use archived `markouts.csv` 3s/10s/30s columns.",
        "- Stale survival reports the share of survival rows where `stale_ask_survival_ms >= delay_ms`.",
        "- Source-delay stats use archived `source_delay.csv` only.",
        "",
    ])

    path.write_text("\n".join(lines), encoding="utf-8")


def build_report(sweep_dir: Path) -> list[dict]:
    scenarios = discover_scenarios(sweep_dir)
    return [summarize_scenario(scenario) for scenario in scenarios]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge latency validation sweep artifacts.")
    parser.add_argument("sweep_dir", type=Path)
    args = parser.parse_args(argv)

    sweep_dir = args.sweep_dir
    rows = build_report(sweep_dir)
    summary_path = sweep_dir / "latency_validation_summary.csv"
    report_path = sweep_dir / "latency_validation_report.md"

    write_summary_csv(summary_path, rows)
    write_report(report_path, rows, sweep_dir)

    print(f"wrote summary: {summary_path}")
    print(f"wrote report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
