"""Offline model calibration using OpenDota pro match history.

Fetches completed pro matches, replays per-minute game state through
estimate_p_radiant(), and compares model output to actual outcomes.

Output:
  logs/calibration.csv   — raw snapshot rows
  stdout calibration table — model bins vs empirical win rate

Usage:
  python3 calibrate_offline.py             # 50 matches (default)
  python3 calibrate_offline.py --n 200     # more data, slower
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
from collections import defaultdict

import math

import aiohttp

sys.path.insert(0, os.path.dirname(__file__))

OPENDOTA_BASE = "https://api.opendota.com/api"


def _sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def estimate_p_radiant(game: dict) -> float | None:
    """Win-probability model for radiant. Coefficients fitted via log-loss grid search
    on 3542 snapshots from 100 OpenDota pro matches. Re-run with --n 200+ to refine."""
    game_time_sec = game.get("game_time_sec")
    radiant_lead = game.get("radiant_lead")
    radiant_score = game.get("radiant_score")
    dire_score = game.get("dire_score")

    if game_time_sec is None or radiant_lead is None:
        return None

    game_min = max(game_time_sec / 60, 1)
    kill_diff = (radiant_score - dire_score) if (radiant_score is not None and dire_score is not None) else 0

    lead_weight = 0.0025
    score = (radiant_lead / game_min) * lead_weight + kill_diff * 0.049
    return _sigmoid(score)
CALIBRATION_CSV = "logs/calibration.csv"
_FIELDNAMES = [
    "match_id", "league_name", "radiant_name", "dire_name",
    "game_time_sec", "nw_lead", "radiant_score", "dire_score",
    "model_p", "radiant_win",
]


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 429:
                print("  rate-limited, waiting 60s...")
                await asyncio.sleep(60)
                return await _fetch_json(session, url)
            if r.status != 200:
                return None
            return await r.json()
    except Exception as e:
        print(f"  fetch error {url}: {e}")
        return None


def _build_snapshots(match: dict) -> list[dict]:
    match_id = match.get("match_id")
    radiant_win = int(bool(match.get("radiant_win")))
    gold_adv = match.get("radiant_gold_adv") or []
    radiant_name = match.get("radiant_name") or match.get("radiant_team", {}).get("name", "")
    dire_name = match.get("dire_name") or match.get("dire_team", {}).get("name", "")
    league_name = (match.get("league") or {}).get("name") or str(match.get("leagueid", ""))

    # Accumulate per-minute kill counts from players' kills_log
    r_by_min: dict[int, int] = defaultdict(int)
    d_by_min: dict[int, int] = defaultdict(int)
    for player in match.get("players") or []:
        is_radiant = bool(player.get("isRadiant"))
        for kill in player.get("kills_log") or []:
            minute = int(kill.get("time", 0)) // 60
            if is_radiant:
                r_by_min[minute] += 1
            else:
                d_by_min[minute] += 1

    snapshots = []
    r_total = d_total = 0
    for minute, nw_lead in enumerate(gold_adv):
        r_total += r_by_min[minute]
        d_total += d_by_min[minute]
        game_time_sec = minute * 60

        game = {
            "game_time_sec": game_time_sec,
            "radiant_lead": nw_lead,
            "radiant_score": r_total,
            "dire_score": d_total,
        }
        p = estimate_p_radiant(game)
        if p is None:
            continue

        snapshots.append({
            "match_id": match_id,
            "league_name": league_name,
            "radiant_name": radiant_name,
            "dire_name": dire_name,
            "game_time_sec": game_time_sec,
            "nw_lead": nw_lead,
            "radiant_score": r_total,
            "dire_score": d_total,
            "model_p": round(p, 4),
            "radiant_win": radiant_win,
        })

    return snapshots


def _log_loss(snapshots: list[dict], lead_w: float, kill_w: float) -> float:
    total = 0.0
    for s in snapshots:
        t = max(s["game_time_sec"] / 60, 1)
        lead_per_min = s["nw_lead"] / t
        kill_diff = s["radiant_score"] - s["dire_score"]
        score = lead_per_min * lead_w + kill_diff * kill_w
        p = 1 / (1 + math.exp(-score))
        p = max(1e-7, min(1 - 1e-7, p))
        y = s["radiant_win"]
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(snapshots)


def _fit_coefficients(snapshots: list[dict]) -> tuple[float, float]:
    """Grid search over lead_weight and kill_weight to minimise log-loss."""
    best_loss = float("inf")
    best = (0.0014, 0.015)

    lead_candidates = [x / 10000 for x in range(1, 40, 2)]   # 0.0001 – 0.0039
    kill_candidates = [x / 1000  for x in range(1, 50, 2)]   # 0.001  – 0.049

    for lw in lead_candidates:
        for kw in kill_candidates:
            loss = _log_loss(snapshots, lw, kw)
            if loss < best_loss:
                best_loss = loss
                best = (lw, kw)

    return best


def _print_calibration(snapshots: list[dict]) -> None:
    # Bin into 5-cent buckets
    bins: dict[float, dict] = {}
    for s in snapshots:
        b = round(round(s["model_p"] * 20) / 20, 2)
        if b not in bins:
            bins[b] = {"n": 0, "wins": 0}
        bins[b]["n"] += 1
        bins[b]["wins"] += s["radiant_win"]

    print(f"\n{'Model P':>10} {'Empirical P':>12} {'N':>8} {'Error':>10}")
    print("-" * 46)
    total_err = 0.0
    for p in sorted(bins):
        d = bins[p]
        emp = d["wins"] / d["n"]
        err = p - emp
        total_err += abs(err)
        flag = " <-- LARGE" if abs(err) > 0.08 else ""
        print(f"{p:>10.2f} {emp:>12.3f} {d['n']:>8} {err:>+10.3f}{flag}")

    n_bins = len(bins)
    mae = total_err / n_bins if n_bins else 0
    print(f"\nMean absolute calibration error: {mae:.3f}  ({len(snapshots)} snapshots, {n_bins} bins)")

    if mae < 0.03:
        verdict = "Model is well-calibrated."
    elif mae < 0.07:
        verdict = "Model has moderate calibration error — weights need tuning."
    else:
        verdict = "Model is poorly calibrated — use the suggested weights below."
    print(f"Verdict: {verdict}")

    # Suggest direction
    over = sum(1 for p in sorted(bins) if p > 0.5 and bins[p]["n"] > 10 and (p - bins[p]["wins"] / bins[p]["n"]) > 0.04)
    under = sum(1 for p in sorted(bins) if p > 0.5 and bins[p]["n"] > 10 and (bins[p]["wins"] / bins[p]["n"] - p) > 0.04)
    if over > under:
        print("Direction: model is OVERCONFIDENT — reduce lead_weight and kill coefficients in model.py.")
    elif under > over:
        print("Direction: model is UNDERCONFIDENT — increase lead_weight and kill coefficients in model.py.")

    # Fit better coefficients
    print("\nFitting coefficients via log-loss grid search...")
    best_lw, best_kw = _fit_coefficients(snapshots)
    current_loss = _log_loss(snapshots, 0.0025, 0.049)  # current model.py coefficients
    fitted_loss  = _log_loss(snapshots, best_lw, best_kw)
    print(f"  Current coefficients (lead=0.0025, kill=0.049):  log-loss = {current_loss:.4f}")
    print(f"  Fitted  coefficients (lead={best_lw:.4f}, kill={best_kw:.3f}):  log-loss = {fitted_loss:.4f}")
    print(f"\nSuggested model.py update:")
    print(f"  Replace lead_weight with:  {best_lw:.4f}")
    print(f"  Replace kill coefficient (0.049) with: {best_kw:.3f}")
    print(f"  (Or re-run with --n 200+ for more data before updating.)")


async def _fetch_tier1_league_ids(session: aiohttp.ClientSession) -> set[int]:
    """Return league IDs tagged 'premium' (tier-1) by OpenDota."""
    data = await _fetch_json(session, f"{OPENDOTA_BASE}/leagues")
    if not isinstance(data, list):
        return set()
    return {int(lg["leagueid"]) for lg in data if lg.get("tier") == "premium"}


async def _collect_match_stubs(
    session: aiohttp.ClientSession,
    n_wanted: int,
    tier1_ids: set[int],
) -> list[dict]:
    """Paginate /proMatches until we have n_wanted tier-1 stubs."""
    stubs: list[dict] = []
    less_than: int | None = None
    seen: set[int] = set()

    while len(stubs) < n_wanted:
        url = f"{OPENDOTA_BASE}/proMatches"
        if less_than:
            url += f"?less_than_match_id={less_than}"
        page = await _fetch_json(session, url)
        if not isinstance(page, list) or not page:
            break
        for m in page:
            mid = int(m.get("match_id", 0))
            if mid in seen:
                continue
            seen.add(mid)
            if not tier1_ids or int(m.get("leagueid", 0)) in tier1_ids:
                stubs.append(m)
        less_than = min(int(m.get("match_id", 0)) for m in page)
        await asyncio.sleep(1.0)
        print(f"  ...{len(stubs)} tier-1 stubs collected (page min_id={less_than})")

    if len(stubs) < n_wanted:
        print(f"Warning: only {len(stubs)} stubs collected, {n_wanted} requested (API exhausted or rate-limited)")
    return stubs[:n_wanted]


async def main(n_matches: int = 50, tier1_only: bool = False) -> None:
    os.makedirs("logs", exist_ok=True)
    all_snapshots: list[dict] = []

    async with aiohttp.ClientSession() as session:
        tier1_ids: set[int] = set()
        if tier1_only:
            print("Fetching tier-1 league list...")
            tier1_ids = await _fetch_tier1_league_ids(session)
            print(f"  {len(tier1_ids)} premium leagues found")

        print(f"Collecting {n_matches} match stubs{'  (tier-1 only)' if tier1_only else ''}...")
        stubs = await _collect_match_stubs(session, n_matches, tier1_ids)
        print(f"Processing {len(stubs)} matches (1.2s delay)...")

        for i, m in enumerate(stubs):
            match_id = int(m["match_id"])
            await asyncio.sleep(1.2)
            data = await _fetch_json(session, f"{OPENDOTA_BASE}/matches/{match_id}")
            if not isinstance(data, dict) or not data.get("radiant_gold_adv"):
                print(f"  [{i+1}/{len(stubs)}] {match_id}: no parse data, skip")
                continue

            snaps = _build_snapshots(data)
            all_snapshots.extend(snaps)
            rname = data.get("radiant_name") or "?"
            dname = data.get("dire_name") or "?"
            league = data.get("league_name") or data.get("leagueid") or ""
            print(f"  [{i+1}/{len(stubs)}] {match_id}: {rname} vs {dname} [{league}] — {len(snaps)} snaps")

    if not all_snapshots:
        print("No data collected.")
        return

    with open(CALIBRATION_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        w.writeheader()
        w.writerows(all_snapshots)
    print(f"\nWrote {len(all_snapshots)} rows to {CALIBRATION_CSV}")

    _print_calibration(all_snapshots)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=50, help="Number of matches to fetch")
    parser.add_argument("--tier1", action="store_true", help="Restrict to premium/tier-1 leagues")
    args = parser.parse_args()
    asyncio.run(main(args.n, tier1_only=args.tier1))
