import pytest

from positions import build_positions, summarize_positions


def test_build_positions_marks_long_yes_at_latest_bid():
    trades = [
        {
            "timestamp_utc": "2026-05-12T00:00:00+00:00",
            "scenario_ms": "750",
            "market_name": "Team A Game 1",
            "token_id": "yes-token",
            "side": "BUY_YES",
            "filled_usd": "25",
            "price": "0.50",
            "shares": "50",
            "p_yes": "0.62",
            "model_edge_at_fill": "0.12",
        }
    ]
    books = [
        {
            "timestamp_utc": "2026-05-12T00:00:01+00:00",
            "asset_id": "yes-token",
            "best_bid": "0.55",
            "best_ask": "0.57",
        },
        {
            "timestamp_utc": "2026-05-12T00:00:03+00:00",
            "asset_id": "yes-token",
            "best_bid": "0.60",
            "best_ask": "0.62",
        },
    ]

    positions = build_positions(trades, books)

    assert len(positions) == 1
    pos = positions[0]
    assert pos.latest_bid == 0.60
    assert pos.market_value_bid_usd == 30.0
    assert pos.unrealized_pnl_usd == 5.0
    assert pos.max_drawdown_usd == pytest.approx(2.5)
    assert pos.max_runup_usd == pytest.approx(5.0)


def test_summarize_positions_counts_attempts_and_fills():
    trades = [
        {"scenario_ms": "250", "filled_usd": "25", "price": "0.50", "shares": "50", "timestamp_utc": "2026-05-12T00:00:00+00:00", "token_id": "a"},
        {"scenario_ms": "250", "filled_usd": "0", "price": "0", "shares": "0", "timestamp_utc": "2026-05-12T00:00:00+00:00", "token_id": "a"},
        {"scenario_ms": "1500", "filled_usd": "0", "price": "0", "shares": "0", "timestamp_utc": "2026-05-12T00:00:00+00:00", "token_id": "a"},
    ]
    books = [
        {"timestamp_utc": "2026-05-12T00:00:01+00:00", "asset_id": "a", "best_bid": "0.52", "best_ask": "0.54"}
    ]
    positions = build_positions(trades, books)
    rows = summarize_positions(trades, positions)

    overall = rows[0]
    assert overall["attempts"] == 3
    assert overall["filled"] == 1
    assert overall["notional_usd"] == 25.0
    assert round(overall["unrealized_pnl_usd"], 6) == 1.0

    by_scenario = {row["scenario_ms"]: row for row in rows[1:]}
    assert by_scenario[250]["attempts"] == 2
    assert by_scenario[250]["filled"] == 1
    assert by_scenario[1500]["attempts"] == 1
    assert by_scenario[1500]["filled"] == 0


def test_build_positions_supports_live_open_entry_schema():
    trades = [
        {
            "timestamp_utc": "2026-05-12T00:00:00+00:00",
            "action": "entry",
            "token_id": "yes-token",
            "market_name": "Team A Game 1",
            "side": "YES",
            "entry_price": "0.50",
            "shares": "50",
            "cost_usd": "25",
        }
    ]
    books = [
        {
            "timestamp_utc": "2026-05-12T00:00:02+00:00",
            "asset_id": "yes-token",
            "best_bid": "0.56",
            "best_ask": "0.58",
        }
    ]

    positions = build_positions(trades, books)
    rows = summarize_positions(trades, positions)

    assert len(positions) == 1
    pos = positions[0]
    assert pos.status == "open"
    assert pos.entry_price == 0.50
    assert pos.notional_usd == 25.0
    assert pos.latest_bid == 0.56
    assert pos.unrealized_pnl_usd == pytest.approx(3.0)
    assert rows[0]["attempts"] == 1
    assert rows[0]["filled"] == 1


def test_build_positions_pairs_live_exit_schema():
    trades = [
        {
            "timestamp_utc": "2026-05-12T00:00:00+00:00",
            "action": "entry",
            "token_id": "yes-token",
            "market_name": "Team A Game 1",
            "side": "YES",
            "entry_price": "0.50",
            "shares": "50",
            "cost_usd": "25",
        },
        {
            "timestamp_utc": "2026-05-12T00:00:10+00:00",
            "action": "exit",
            "token_id": "yes-token",
            "market_name": "Team A Game 1",
            "side": "YES",
            "entry_price": "0.50",
            "exit_price": "0.70",
            "shares": "50",
            "cost_usd": "25",
            "proceeds_usd": "35",
            "pnl_usd": "10",
            "hold_sec": "10",
        },
    ]

    positions = build_positions(trades, [])
    rows = summarize_positions(trades, positions)

    assert len(positions) == 1
    pos = positions[0]
    assert pos.status == "closed"
    assert pos.latest_bid == 0.70
    assert pos.market_value_bid_usd == 35.0
    assert pos.unrealized_pnl_usd == 10.0
    assert pos.holding_seconds == 10.0
    assert rows[0]["attempts"] == 1
    assert rows[0]["filled"] == 1
    assert rows[0]["unrealized_pnl_usd"] == 10.0
