"""Event-driven latency-arb backtest against real Polymarket order book data.

Thesis: the market is slow to reprice discrete in-game events (teamfights,
tower falls, NW swings). We don't bet on team quality (market knows that
pre-game). We bet that the market hasn't caught up to the LAST EVENT yet.

Signal logic:
  1. EventDetector fires on a discrete game event (KILL_SWING, TOWER_STATE_CHANGE,
     LEAD_SWING_*).
  2. Direction = which team benefited.
  3. Compute expected market move for that event type × magnitude.
  4. If actual market move since event < expected - min_lag → market is lagging → buy.
  5. Exit at +30s (or first profitable tick, configurable).

Usage:
    python3 backtest.py [--lag 0.05] [--size 25] [--exit 30]
"""
from __future__ import annotations

import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import sqlite3
from dataclasses import dataclass

from event_detector import EventDetector
from signal_engine import ACTIVE_EVENTS
from config import EVENT_LEAD_SWING_30S, EVENT_LEAD_SWING_60S

DATA_DIR = "/home/irene/dota_poly_bot_final/data"
PAPER_SIZE_USD = 25.0

SEGMENTS = [
    {
        "label": "Carstensz vs TEAM GRIND",
        "db": f"{DATA_DIR}/dota_poly_collection.sqlite",
        "match_key": "90285607589477394",
        "radiant_token": "90268231449155282246853972144583742931465600097997027484803301961579288855144",
        "dire_token":    "63987300715693577866871042327158392402412511432457971527449850967203600386804",
        "radiant_win": 0,
    },
    {
        "label": "PlayTime vs 1w Team",
        "db": f"{DATA_DIR}/dota_poly_collection.sqlite",
        "match_key": "90285599503423511_m1",
        "radiant_token": "13478386926402301406532136263977204904714000287949507563856704721767290839044",
        "dire_token":    "63310461820786146813035795297817607012343337186334700939384155278400607390107",
        "radiant_win": 0,
    },
    {
        "label": "Two Move vs Team Lynx",
        "db": f"{DATA_DIR}/lynx_tm6_collection.sqlite",
        "match_key": "90285619707346954_m1",
        "radiant_token": "34976881449444734178409311723175251004867357634324791603190926689290262342977",
        "dire_token":    "4452564105200725346521605963781468915677428239106200674543104246242028910211",
        "radiant_win": 0,
    },
    {
        "label": "1w Team vs PlayTime (G2)",
        "db": f"{DATA_DIR}/1win_ptime_g2.sqlite",
        "match_key": "90285618797931526_m1",
        "radiant_token": "44042712276170069650224504201935395716816628269726746518447030840697274440699",
        "dire_token":    "33812820765680339713713753007847781463087366023932766960858881005889078629256",
        "radiant_win": 1,
    },
    {
        "label": "PARIVISION vs 1w Team (G1)",
        "db": f"{DATA_DIR}/1win_pari_g1.sqlite",
        "match_key": "90285623272207384_m1",
        "radiant_token": "70347395524393779469493680391299369304316720284512794724445180423011761114165",
        "dire_token":    "74998310881290739392918170902879306286233744638879268738919090905932120366324",
        "radiant_win": 1,
    },
    {
        "label": "1w vs PARIVISION (G2)",
        "db": f"{DATA_DIR}/1win_pari_g2.sqlite",
        "match_key": "90285627567738905_m1",
        "radiant_token": "47625441297314461057077645727754264216244555280948560109804310553137263770263",
        "dire_token":    "57026843702394568915654625917402236159662865883159051302086816685884289545931",
        "radiant_win": 0,
    },
    {
        "label": "PARIVISION vs 1w Team (G3)",
        "db": f"{DATA_DIR}/1win_pari_g3.sqlite",
        "match_key": "90285630522125338_m1",
        "radiant_token": "39003960489463622267960758033797773112117778420142043276598674855608515962197",
        "dire_token":    "14082266884467670274043702622681498600675941864859148613356376458056888253905",
        "radiant_win": 1,
    },
]

# Expected market move per event type (in probability units).
# Must match ACTIVE_EVENTS in signal_engine.py.
EVENT_EXPECTED_MOVE = {name: spec.base for name, spec in ACTIVE_EVENTS.items()}

# Events that only fire on high severity
_HIGH_SEVERITY_ONLY = frozenset({"LEAD_SWING_30S", "LEAD_SWING_60S"})


@dataclass
class Trade:
    label: str
    event_type: str
    direction: str
    severity: str
    game_time_sec: int
    wall_ts_ms: int
    side: str
    fill: float
    pre_game_price: float
    price_at_event: float
    expected_move: float
    actual_move: float
    lag: float
    radiant_win: int
    pnl_15s: float | None = None
    pnl_30s: float | None = None
    pnl_60s: float | None = None
    pnl_term: float | None = None


def _load_dota(db: sqlite3.Connection, match_key: str, start_ms: int, end_ms: int) -> list[dict]:
    rows = db.execute(
        """SELECT ts_ms, game_time, radiant_score, dire_score, nw_diff,
                  radiant_team, dire_team
           FROM dota_ticks
           WHERE match_key=? AND ts_ms BETWEEN ? AND ?
           ORDER BY ts_ms""",
        (match_key, start_ms, end_ms),
    ).fetchall()
    seen: set[int] = set()
    out = []
    for ts_ms, game_time, r_score, d_score, nw_diff, r_team, d_team in rows:
        gt = int(game_time or 0)
        if gt in seen:
            continue
        seen.add(gt)
        out.append({
            "ts_ms": ts_ms,
            "game_time_sec": gt,
            "radiant_score": int(r_score or 0),
            "dire_score": int(d_score or 0),
            "radiant_lead": int(nw_diff or 0),
            "radiant_team": r_team,
            "dire_team": d_team,
            "match_id": match_key,
        })
    return out


def _load_market(db: sqlite3.Connection, token_id: str, start_ms: int, end_ms: int) -> list[dict]:
    rows = db.execute(
        """SELECT ts_ms, best_bid, best_ask, mid
           FROM market_ticks
           WHERE token_id=? AND ts_ms BETWEEN ? AND ?
           ORDER BY ts_ms""",
        (token_id, start_ms, end_ms),
    ).fetchall()
    return [{"ts_ms": r[0], "best_bid": r[1], "best_ask": r[2], "mid": r[3]} for r in rows]


def _nearest_before(ticks: list[dict], ts_ms: int) -> dict | None:
    lo, hi, result = 0, len(ticks) - 1, None
    while lo <= hi:
        m = (lo + hi) // 2
        if ticks[m]["ts_ms"] <= ts_ms:
            result = ticks[m]; lo = m + 1
        else:
            hi = m - 1
    return result


def _mid_at(ticks: list[dict], ts_ms: int) -> float | None:
    t = _nearest_before(ticks, ts_ms)
    return t["mid"] if t else None


def _ask_at(ticks: list[dict], ts_ms: int) -> float | None:
    t = _nearest_before(ticks, ts_ms)
    return t["best_ask"] if t else None


def run_backtest(min_lag: float, size_usd: float, exit_sec: int) -> list[Trade]:
    all_trades: list[Trade] = []

    for seg in SEGMENTS:
        db = sqlite3.connect(seg["db"])
        match_key = seg["match_key"]

        d_range = db.execute(
            "SELECT MIN(ts_ms), MAX(ts_ms) FROM dota_ticks WHERE match_key=?", (match_key,)
        ).fetchone()
        if not d_range or not d_range[0]:
            db.close()
            continue

        start_ms, end_ms = d_range
        rad_ticks = _load_market(db, seg["radiant_token"], start_ms, end_ms)
        dire_ticks = _load_market(db, seg["dire_token"], start_ms, end_ms)
        db.close()

        if not rad_ticks and not dire_ticks:
            continue

        mkt_start = min((t["ts_ms"] for t in rad_ticks + dire_ticks), default=start_ms)
        mkt_end   = max((t["ts_ms"] for t in rad_ticks + dire_ticks), default=end_ms)
        db2 = sqlite3.connect(seg["db"])
        dota_snaps = _load_dota(db2, match_key, mkt_start, mkt_end)
        db2.close()

        if not dota_snaps:
            continue

        # Pre-game market price: earliest available tick
        pre_game_rad = rad_ticks[0]["mid"] if rad_ticks else 0.5
        pre_game_dire = dire_ticks[0]["mid"] if dire_ticks else 0.5

        # NOTE: The backtest lag model (expected_move - (price_at_event - pre_game_price))
        # measures accumulated movement from game start, whereas the live engine measures
        # movement over the last short latency window. These are not equivalent: a backtest lag of
        # 0.05 may correspond to a live lag of -0.10 or +0.20 depending on prior events.
        # Backtest results are directionally informative but cannot be used to calibrate
        # live MIN_LAG thresholds or position-sizing multipliers directly.
        #
        # NOTE: structure events may never fire in backtest — _load_dota does not select
        # building_state or tower_state, so EventDetector._tower_events has no data.

        detector = EventDetector()
        cooldown_until_ms: dict[tuple[str, str], int] = {}  # (direction, event_type) -> wall_ms

        for snap in dota_snaps:
            ts = snap["ts_ms"]
            events = detector.observe(snap)

            for evt in events:
                if evt.event_type not in EVENT_EXPECTED_MOVE:
                    continue

                if evt.event_type in _HIGH_SEVERITY_ONLY and evt.severity != "high":
                    continue

                direction = evt.direction  # "radiant" or "dire"
                if direction not in ("radiant", "dire"):
                    continue

                if ts < cooldown_until_ms.get((direction, evt.event_type), 0):
                    continue

                expected_move = EVENT_EXPECTED_MOVE[evt.event_type]

                # Scale by magnitude for events that support it
                if evt.delta is not None:
                    if evt.event_type in ("LEAD_SWING_30S", "LEAD_SWING_60S"):
                        threshold = EVENT_LEAD_SWING_30S if "30S" in evt.event_type else EVENT_LEAD_SWING_60S
                        expected_move *= min(abs(evt.delta) / threshold, 3.0)
                    elif evt.event_type == "COMEBACK":
                        expected_move *= min(abs(evt.delta) / 3000, 2.0)
                    elif evt.event_type == "KILL_CONFIRMED_LEAD_SWING":
                        expected_move *= min(abs(evt.delta) / 2500, 2.0)
                    elif evt.event_type in ("KILL_BURST_30S", "LATE_GAME_WIPE", "ULTRA_LATE_WIPE") :
                        expected_move *= min(abs(evt.delta) / 5, 2.0)
                    elif evt.event_type in ("T2_TOWER_FALL", "T3_TOWER_FALL", "MULTIPLE_T3_TOWERS_DOWN", "FIRST_T4_TOWER_FALL", "SECOND_T4_TOWER_FALL"):
                        if evt.delta > 1:
                            expected_move *= min(float(evt.delta), 2.0)

                if direction == "radiant":
                    token_ticks = rad_ticks
                    pre_game_price = pre_game_rad
                    terminal = float(seg["radiant_win"])
                    side = "BUY_RADIANT"
                else:
                    token_ticks = dire_ticks
                    pre_game_price = pre_game_dire
                    terminal = float(1 - seg["radiant_win"])
                    side = "BUY_DIRE"

                price_at_event = _mid_at(token_ticks, ts)
                if price_at_event is None:
                    continue

                # How much has the market already moved from pre-game in the right direction?
                actual_move = price_at_event - pre_game_price  # positive = market moved toward this team
                lag = expected_move - actual_move

                if lag < min_lag:
                    continue

                # Entry: buy at ask
                ask = _ask_at(token_ticks, ts)
                if ask is None:
                    continue

                trade = Trade(
                    label=seg["label"],
                    event_type=evt.event_type,
                    direction=direction,
                    severity=evt.severity,
                    game_time_sec=snap["game_time_sec"],
                    wall_ts_ms=ts,
                    side=side,
                    fill=ask,
                    pre_game_price=pre_game_price,
                    price_at_event=price_at_event,
                    expected_move=round(expected_move, 4),
                    actual_move=round(actual_move, 4),
                    lag=round(lag, 4),
                    radiant_win=seg["radiant_win"],
                )

                exit_ms = ts + exit_sec * 1000
                for horizon_ms, attr in [(15_000, "pnl_15s"), (30_000, "pnl_30s"), (60_000, "pnl_60s")]:
                    fp = _mid_at(token_ticks, ts + horizon_ms)
                    if fp is not None:
                        setattr(trade, attr, (fp - ask) * size_usd)

                trade.pnl_term = (terminal - ask) * size_usd
                all_trades.append(trade)

                cooldown_until_ms[(direction, evt.event_type)] = exit_ms

    return all_trades


def _fmt(v: float | None) -> str:
    return f"{v:+7.2f}" if v is not None else "    n/a"


def print_results(trades: list[Trade], min_lag: float, size_usd: float, exit_sec: int):
    print(f"\nEvent-driven backtest  min_lag={min_lag}  size=${size_usd}  exit={exit_sec}s  segments={len(SEGMENTS)}")
    print(f"Total signals: {len(trades)}\n")

    if not trades:
        print("No signals fired. Lower --lag threshold.")
        return

    hdr = (f"{'match':>30}  {'gt':>5}  {'event':>22}  {'dir':>5}  {'sev':>4}  "
           f"{'fill':>5}  {'lag':>5}  {'15s':>7}  {'30s':>7}  {'60s':>7}  {'term':>7}")
    print(hdr)
    print("-" * len(hdr))

    buckets: dict[str, list[float]] = {"pnl_15s": [], "pnl_30s": [], "pnl_60s": [], "pnl_term": []}
    for t in sorted(trades, key=lambda x: (x.label, x.game_time_sec)):
        print(
            f"{t.label:>30}  {t.game_time_sec:>5}  {t.event_type:>22}  {t.direction:>5}  {t.severity:>4}  "
            f"{t.fill:.3f}  {t.lag:.3f}  {_fmt(t.pnl_15s)}  {_fmt(t.pnl_30s)}  {_fmt(t.pnl_60s)}  {_fmt(t.pnl_term)}"
        )
        for k in buckets:
            v = getattr(t, k)
            if v is not None:
                buckets[k].append(v)

    print("-" * len(hdr))
    for label, key in [("15s", "pnl_15s"), ("30s", "pnl_30s"), ("60s", "pnl_60s"), ("terminal", "pnl_term")]:
        vals = buckets[key]
        if vals:
            wins = sum(1 for v in vals if v > 0)
            print(f"  {label:>8}: avg {sum(vals)/len(vals):+.2f}  total {sum(vals):+.2f}  wins={wins}/{len(vals)}")

    print("\nPer-game:")
    from collections import defaultdict
    by_game: dict[str, list] = defaultdict(list)
    for t in trades:
        by_game[t.label].append(t)
    for label, ts in sorted(by_game.items()):
        terms = [t.pnl_term for t in ts if t.pnl_term is not None]
        s15 = [t.pnl_15s for t in ts if t.pnl_15s is not None]
        correct = sum(1 for t in ts if (t.direction == "radiant") == (t.radiant_win == 1))
        if terms:
            print(f"  {label:>30}: n={len(ts)}  correct_dir={correct}/{len(ts)}  "
                  f"15s_avg={sum(s15)/len(s15):+.2f}  term_avg={sum(terms)/len(terms):+.2f}")

    print("\nBy event type:")
    by_evt: dict[str, list] = defaultdict(list)
    for t in trades:
        by_evt[t.event_type].append(t)
    for evt, ts in sorted(by_evt.items()):
        terms = [t.pnl_term for t in ts if t.pnl_term is not None]
        s15 = [t.pnl_15s for t in ts if t.pnl_15s is not None]
        correct = sum(1 for t in ts if (t.direction == "radiant") == (t.radiant_win == 1))
        if terms:
            print(f"  {evt:>25}: n={len(ts)}  correct_dir={correct}/{len(ts)}  "
                  f"15s_avg={sum(s15)/len(s15) if s15 else 0:+.2f}  term_avg={sum(terms)/len(terms):+.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lag",  type=float, default=0.05, help="Min market lag to fire (default 0.05)")
    parser.add_argument("--size", type=float, default=PAPER_SIZE_USD)
    parser.add_argument("--exit", type=int,   default=30,  help="Exit horizon in seconds (default 30)")
    args = parser.parse_args()
    trades = run_backtest(min_lag=args.lag, size_usd=args.size, exit_sec=args.exit)
    print_results(trades, min_lag=args.lag, size_usd=args.size, exit_sec=args.exit)
