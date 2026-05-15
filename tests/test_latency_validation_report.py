import csv

import pytest

from scripts.merge_latency_validation import build_report, main


def write_csv(path, rows, headers):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_latency_validation_report_merges_core_metrics(tmp_path):
    logs = tmp_path / "delay_0250ms" / "logs"

    write_csv(
        logs / "latency.csv",
        [
            {"decision": "paper_entry_result", "paper_entry_result": "filled"},
            {"decision": "paper_entry_result", "paper_entry_result": "skipped"},
            {"decision": "paper_buy_yes", "paper_entry_result": ""},
        ],
        ["decision", "paper_entry_result"],
    )
    write_csv(
        logs / "pnl_summary.csv",
        [
            {
                "scope": "overall",
                "scenario_ms": "all",
                "notional_usd": "100",
                "marked_positions": "2",
                "unrealized_pnl_usd": "12.5",
                "unrealized_pnl_pct": "0.125",
            }
        ],
        ["scope", "scenario_ms", "notional_usd", "marked_positions", "unrealized_pnl_usd", "unrealized_pnl_pct"],
    )
    write_csv(
        logs / "markouts.csv",
        [
            {"markout_3s": "0.01", "markout_10s": "0.03", "markout_30s": "-0.01"},
            {"markout_3s": "-0.02", "markout_10s": "0.05", "markout_30s": "0.02"},
        ],
        ["markout_3s", "markout_10s", "markout_30s"],
    )
    write_csv(
        logs / "stale_ask_survival.csv",
        [
            {"stale_ask_survival_ms": "100"},
            {"stale_ask_survival_ms": "300"},
            {"stale_ask_survival_ms": ""},
        ],
        ["stale_ask_survival_ms"],
    )
    write_csv(
        logs / "source_delay.csv",
        [
            {"game_time_lag_sec": "10", "stream_delay_s": "120", "wall_clock_receive_gap_sec": "1"},
            {"game_time_lag_sec": "20", "stream_delay_s": "130", "wall_clock_receive_gap_sec": "3"},
        ],
        ["game_time_lag_sec", "stream_delay_s", "wall_clock_receive_gap_sec"],
    )

    rows = build_report(tmp_path)

    assert len(rows) == 1
    row = rows[0]
    assert row["delay_ms"] == 250
    assert row["attempts"] == 2
    assert row["filled"] == 1
    assert row["fill_rate"] == 0.5
    assert row["notional_usd"] == 100
    assert row["marked_positions"] == 2
    assert row["bid_marked_pnl_usd"] == 12.5
    assert row["bid_marked_pnl_pct"] == 0.125
    assert row["markout_count"] == 2
    assert row["markout_3s_avg"] == pytest.approx(-0.005)
    assert row["markout_3s_median"] == pytest.approx(-0.005)
    assert row["markout_3s_positive_rate"] == 0.5
    assert row["markout_10s_avg"] == pytest.approx(0.04)
    assert row["markout_10s_positive_rate"] == 1.0
    assert row["markout_30s_positive_rate"] == 0.5
    assert row["stale_survival_count"] == 2
    assert row["stale_survival_avg_ms"] == 200
    assert row["stale_survival_median_ms"] == 200
    assert row["stale_survived_delay_rate"] == 0.5
    assert row["source_delay_count"] == 2
    assert row["game_time_lag_sec_avg"] == 15
    assert row["game_time_lag_sec_median"] == 15
    assert row["game_time_lag_sec_p95"] == pytest.approx(19.5)
    assert "passes PnL/fill sanity" in row["verdict"]


def test_latency_validation_report_handles_missing_csvs(tmp_path):
    (tmp_path / "delay_1000ms" / "logs").mkdir(parents=True)

    rows = build_report(tmp_path)

    assert len(rows) == 1
    row = rows[0]
    assert row["delay_ms"] == 1000
    assert row["attempts"] == 0
    assert row["filled"] == 0
    assert row["fill_rate"] is None
    assert row["bid_marked_pnl_usd"] is None
    assert row["stale_survived_delay_rate"] is None
    assert row["source_delay_count"] == 0
    assert row["verdict"] == "no paper attempts"


def test_latency_validation_report_writes_outputs(tmp_path):
    (tmp_path / "delay_0000ms" / "logs").mkdir(parents=True)

    assert main([str(tmp_path)]) == 0

    summary = tmp_path / "latency_validation_summary.csv"
    report = tmp_path / "latency_validation_report.md"
    assert summary.exists()
    assert report.exists()
    assert "delay_ms" in summary.read_text(encoding="utf-8")
    assert "Scenario Comparison" in report.read_text(encoding="utf-8")
