from __future__ import annotations

from config import PAPER_TRADES_CSV_PATH, BOOK_EVENTS_CSV_PATH, POSITIONS_CSV_PATH, PNL_SUMMARY_CSV_PATH
from positions import (
    POSITION_HEADERS,
    SUMMARY_HEADERS,
    build_positions,
    read_csv,
    summarize_positions,
    write_csv,
)


def fmt_money(value):
    if value is None or value == "":
        return "n/a"
    return f"${float(value):.2f}"


def main() -> None:
    trades = read_csv(PAPER_TRADES_CSV_PATH)
    book_events = read_csv(BOOK_EVENTS_CSV_PATH)

    positions = build_positions(trades, book_events)
    summary = summarize_positions(trades, positions)

    write_csv(POSITIONS_CSV_PATH, [p.to_dict() for p in positions], POSITION_HEADERS)
    write_csv(PNL_SUMMARY_CSV_PATH, summary, SUMMARY_HEADERS)

    overall = summary[0] if summary else {}
    print(f"positions written: {POSITIONS_CSV_PATH} ({len(positions)} positions)")
    print(f"summary written: {PNL_SUMMARY_CSV_PATH}")
    print(
        "overall: "
        f"attempts={overall.get('attempts', 0)}, "
        f"filled={overall.get('filled', 0)}, "
        f"notional={fmt_money(overall.get('notional_usd'))}, "
        f"bid-mark PnL={fmt_money(overall.get('unrealized_pnl_usd'))}"
    )


if __name__ == "__main__":
    main()
