from __future__ import annotations

import asyncio
import traceback
import aiohttp

from steam_client import fetch_all_live_games, LeagueGameCache
from poly_ws import listen_books, BookStore
from signal_engine import EventSignalEngine
from paper_trader import PaperTrader
from storage import SignalLogger, DotaEventLogger, BookEventLogger, PositionLogger, RawSnapshotLogger, LiveAttemptLogger
from mapping import load_valid_mappings
from event_detector import EventDetector
from live_executor import LiveExecutor
from config import (
    STEAM_API_KEY, STEAM_POLL_SECONDS, PAPER_EXECUTION_DELAY_MS, LIVE_TRADING,
    ALLOW_CONFIRMATION_ONLY_LIVE_TRADES,
)
from sync_markets import sync_markets_to_games, load_markets, write_markets

MAPPING_REFRESH_SECONDS = 60


def _best_signal_candidate(candidates: list[dict]) -> dict | None:
    """Choose the strongest executable same-poll signal candidate.

    Each candidate is {"signal": dict, "direction": str, "events": list}.
    Prefer executable edge, then expected move. This keeps chaotic same-poll
    updates from entering the first arbitrary direction cluster.
    """
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda c: (
            float(c["signal"].get("executable_edge") or 0.0),
            float(c["signal"].get("expected_move") or 0.0),
        ),
    )


def _book_mid(book: dict | None) -> float | None:
    if not book:
        return None
    bid = book.get("best_bid")
    ask = book.get("best_ask")
    try:
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
        if ask is not None:
            return float(ask)
        if bid is not None:
            return float(bid)
    except (TypeError, ValueError):
        return None
    return None


async def _log_live_attempt_with_markouts(attempt, book_store: BookStore, live_logger: LiveAttemptLogger):
    """Log submit row immediately, then a final row after 30s with markouts."""
    live_logger.log_attempt(attempt, phase="submit")
    reference = attempt.avg_fill_price or attempt.best_ask or attempt.price_cap
    markouts = {}
    if reference is None:
        await asyncio.sleep(30)
        live_logger.log_attempt(attempt, phase="markout", markouts=markouts)
        return

    async def sample(delay: int) -> float | None:
        await asyncio.sleep(delay)
        mid = _book_mid(book_store.get(attempt.token_id))
        return round(mid - reference, 4) if mid is not None else None

    m3 = await sample(3)
    markouts["markout_3s"] = m3
    m10 = await sample(7)
    markouts["markout_10s"] = m10
    m30 = await sample(20)
    markouts["markout_30s"] = m30
    live_logger.log_attempt(attempt, phase="markout", markouts=markouts)


async def steam_loop(
    book_store: BookStore,
    trader: PaperTrader,
    signal_logger: SignalLogger,
    event_detector: EventDetector,
    signal_engine: EventSignalEngine,
    event_logger: DotaEventLogger,
    position_logger: PositionLogger,
    snapshot_logger: RawSnapshotLogger,
    live_executor: LiveExecutor | None,
    live_logger: LiveAttemptLogger | None,
    mappings: list[dict],
    asset_ids: list[str],
):
    if not STEAM_API_KEY or STEAM_API_KEY == "replace_me":
        print("Missing STEAM_API_KEY. Copy .env.example to .env and fill it in.")
        return

    last_mapping_refresh = 0.0
    league_cache = LeagueGameCache()
    max_game_times: dict[str, int] = {}  # match_id -> max game_time_sec seen

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                import time as _time
                now = _time.monotonic()
                if now - last_mapping_refresh >= MAPPING_REFRESH_SECONDS:
                    last_mapping_refresh = now
                    games_for_sync = await fetch_all_live_games(session, league_cache)
                    mdata = load_markets()
                    raw_markets = mdata.setdefault("markets", [])
                    updates = sync_markets_to_games(raw_markets, games_for_sync)
                    if updates:
                        write_markets(mdata)
                        for u in updates:
                            print(
                                f"AUTO-MAPPED {u['market_name']} → {u['dota_match_id']} "
                                f"({u['radiant_team']} vs {u['dire_team']})"
                            )
                    fresh_mappings, _ = load_valid_mappings()
                    new_ids = {(m["yes_token_id"], m["no_token_id"]) for m in fresh_mappings}
                    old_ids = {(m["yes_token_id"], m["no_token_id"]) for m in mappings}
                    added = new_ids - old_ids
                    removed = old_ids - new_ids
                    if added or removed:
                        mappings.clear()
                        mappings.extend(fresh_mappings)
                        new_assets = [tid for m in fresh_mappings for tid in (m["yes_token_id"], m["no_token_id"])]
                        # Keep the shared websocket subscription list exact. listen_books()
                        # detects the set change and reconnects with the new subscription.
                        asset_ids.clear()
                        asset_ids.extend(new_assets)
                        if added:
                            print(f"Mappings added: {len(added)} market(s). Restart not required.")
                        if removed:
                            print(f"Mappings removed: {len(removed)} market(s).")

                games = await fetch_all_live_games(session, league_cache)
                
                # Filter to only 'tracked' games: those that are already mapped 
                # OR are candidates for mapping (matching teams in our list).
                tracked_match_ids = {str(m["dota_match_id"]) for m in mappings if m.get("dota_match_id")}
                # Also include games that sync_markets might want to see
                mdata_all = load_markets()
                all_raw_markets = mdata_all.get("markets", [])
                
                def is_relevant(g):
                    mid = str(g.get("match_id") or "")
                    if mid in tracked_match_ids:
                        return True
                    # Check if this game is a potential match for any of our markets
                    from sync_markets import match_direction
                    return any(match_direction(m, g) for m in all_raw_markets)

                relevant_games = [g for g in games if is_relevant(g)]

                game_over_match_ids: set[str] = set()
                current_game_times: dict[str, int | None] = {}

                # 1. First pass: log and process only relevant games
                active_games = []
                for game in relevant_games:
                    match_id = str(game.get("match_id") or "")
                    game_time = game.get("game_time_sec")
                    data_source = game.get("data_source")

                    snapshot_logger.log_game(game)

                    # Guard: Ignore non-TopLive sources for event detection (too stale)
                    if data_source != "top_live":
                        continue

                    # Guard: Ignore backward-moving game time (stale/out-of-order snapshots)
                    if game_time is not None:
                        prev_max = max_game_times.get(match_id, -1)
                        if game_time < prev_max:
                            continue
                        max_game_times[match_id] = game_time

                    current_game_times[match_id] = game_time
                    if game.get("game_over"):
                        game_over_match_ids.add(match_id)
                    else:
                        active_games.append(game)

                # Check exits before processing new signals
                closed = trader.check_exits(
                    book_store,
                    game_over_match_ids,
                    current_game_times,
                )
                for cp in closed:
                    position_logger.log_exit(cp)
                    print(
                        f"EXIT [{cp.exit_reason}] {cp.market_name} {cp.side} "
                        f"entry={cp.entry_price:.4f} exit={cp.exit_price:.4f} "
                        f"pnl=${cp.pnl_usd:+.2f} hold={cp.hold_sec:.0f}s"
                    )

                for game in active_games:
                    for mapping in mappings:
                        if str(mapping["dota_match_id"]) not in {
                            game.get("match_id"), game.get("lobby_id")
                        }:
                            continue

                        yes_book = book_store.get(mapping["yes_token_id"])
                        no_book = book_store.get(mapping["no_token_id"])

                        # Record prices into rolling history (drives lag calculation)
                        game_time = game.get("game_time_sec")
                        for tok, book in [
                            (mapping["yes_token_id"], yes_book),
                            (mapping["no_token_id"], no_book),
                        ]:
                            if book and book.get("best_bid") is not None and book.get("best_ask") is not None:
                                book_mid = (book["best_bid"] + book["best_ask"]) / 2
                                signal_engine.record_price(tok, book_mid, game_time)

                        dota_events = event_detector.observe(game, mapping)
                        if dota_events:
                            event_logger.log_events(dota_events)
                            # Final model: score same-direction event clusters once,
                            # log every cluster decision, then enter only the best passing
                            # candidate instead of the first arbitrary direction.
                            clusters = {}
                            for evt in dota_events:
                                clusters.setdefault(evt.direction or "", []).append(evt)

                            candidates = []
                            for event_direction, cluster_events in clusters.items():
                                if not event_direction:
                                    continue

                                signal = signal_engine.evaluate_cluster(
                                    events=cluster_events,
                                    game=game,
                                    mapping=mapping,
                                    yes_book=yes_book,
                                    no_book=no_book,
                                    require_primary=not (LIVE_TRADING and ALLOW_CONFIRMATION_ONLY_LIVE_TRADES),
                                )
                                if signal.get("reason") == "no_primary_event":
                                    shadow_signal = signal_engine.evaluate_cluster(
                                        events=cluster_events,
                                        game=game,
                                        mapping=mapping,
                                        yes_book=yes_book,
                                        no_book=no_book,
                                        require_primary=False,
                                    )
                                    if shadow_signal.get("decision") == "paper_buy_yes":
                                        shadow_signal = dict(shadow_signal)
                                        shadow_signal["decision"] = "skip"
                                        shadow_signal["reason"] = "shadow_no_primary"
                                        signal = shadow_signal
                                tok_id = signal.get("token_id", "")
                                tok_side = signal.get("side", "")
                                event_names = [evt.event_type for evt in cluster_events]
                                signal_logger.log_signal(
                                    game, mapping, signal,
                                    event_type=signal.get("cluster_event_types") or "+".join(event_names),
                                    event_direction=event_direction,
                                    severity=signal.get("severity") or "+".join(evt.severity for evt in cluster_events),
                                    token_id=tok_id,
                                    side=tok_side,
                                )

                                if signal["decision"] == "paper_buy_yes":
                                    candidates.append({
                                        "signal": signal,
                                        "direction": event_direction,
                                        "events": cluster_events,
                                    })

                            best = _best_signal_candidate(candidates)
                            if best:
                                signal = best["signal"]
                                cluster_events = best["events"]
                                event_direction = best["direction"]
                                tok_id = signal.get("token_id", "")
                                tok_side = signal.get("side", "")
                                event_names = [evt.event_type for evt in cluster_events]

                                print(
                                    f"EVENT_CLUSTER {signal.get('cluster_event_types') or '+'.join(event_names)} "
                                    f"dir={event_direction} t={game_time}s "
                                    f"edge={signal.get('executable_edge')}"
                                )

                                # Determine the opposing token for this binary market
                                opposing_tok = (
                                    mapping["no_token_id"] if tok_id == mapping["yes_token_id"]
                                    else mapping["yes_token_id"]
                                )

                                # Adverse event exit: if the opposing token is open, close it
                                # before entering the new direction (the event contradicts it)
                                if opposing_tok in trader.positions:
                                    cp = trader.force_exit(opposing_tok, book_store, "adverse_event")
                                    if cp:
                                        position_logger.log_exit(cp)
                                        print(
                                            f"ADVERSE EXIT {mapping['name']} {cp.side} "
                                            f"pnl=${cp.pnl_usd:+.2f} hold={cp.hold_sec:.0f}s"
                                        )

                                if live_executor and live_logger:
                                    attempt = await live_executor.try_buy(
                                        signal=signal,
                                        mapping=mapping,
                                        game=game,
                                        book_store=book_store,
                                    )
                                    asyncio.create_task(_log_live_attempt_with_markouts(attempt, book_store, live_logger))
                                    print(
                                        f"LIVE_ATTEMPT {mapping['name']} {tok_side} "
                                        f"status={attempt.order_status} "
                                        f"size=${attempt.submitted_size_usd:.2f} "
                                        f"filled=${attempt.filled_size_usd:.2f} "
                                        f"cap={attempt.price_cap} "
                                        f"reason={attempt.reason_if_rejected}"
                                    )
                                    if attempt.submitted_size_usd > 0:
                                        signal_engine.commit_signal(signal)
                                else:
                                    if PAPER_EXECUTION_DELAY_MS > 0:
                                        await asyncio.sleep(PAPER_EXECUTION_DELAY_MS / 1000.0)

                                    pos, reason = trader.enter(
                                        signal=signal,
                                        token_id=tok_id,
                                        side=tok_side,
                                        book_store=book_store,
                                        match_id=str(game.get("match_id") or ""),
                                        market_name=mapping.get("name"),
                                        opposing_token_id=opposing_tok,
                                    )
                                    if pos:
                                        signal_engine.commit_signal(signal)
                                        position_logger.log_entry(pos)
                                        print(
                                            f"ENTER {mapping['name']} {tok_side} "
                                            f"price={pos.entry_price:.4f} "
                                            f"shares={pos.shares:.2f} "
                                            f"cost=${pos.cost_usd:.2f} "
                                            f"lag={pos.lag:.3f} "
                                            f"exp={pos.expected_move:.3f} "
                                            f"event={signal.get('event_type')}"
                                        )
                                    else:
                                        print(f"SKIP ENTRY: {reason}")

            except Exception as e:
                print("steam_loop error:", repr(e))
                traceback.print_exc()

            await asyncio.sleep(STEAM_POLL_SECONDS)


async def main():
    # Initial sync: try to link any already-live Steam games before starting
    print("Running initial Steam market sync...")
    try:
        async with aiohttp.ClientSession() as session:
            games = await fetch_all_live_games(session)
        mdata = load_markets()
        updates = sync_markets_to_games(mdata.setdefault("markets", []), games)
        if updates:
            write_markets(mdata)
            for u in updates:
                print(f"  linked {u['market_name']} → {u['dota_match_id']}")
        else:
            print("  no live games matched markets.yaml right now (will retry every 60s)")
    except Exception as e:
        print(f"  initial sync error (non-fatal): {e}")

    mappings, errors = load_valid_mappings()
    for err in errors:
        print(f"Skipping mapping #{err.index} ({err.name or 'unnamed'}): {err.reason}")

    if not mappings:
        print("No active mappings yet — bot will keep checking every 60s for live games.")

    store = BookStore()
    trader = PaperTrader()
    signal_logger = SignalLogger()
    event_detector = EventDetector()
    signal_engine = EventSignalEngine()
    event_logger = DotaEventLogger()
    book_logger = BookEventLogger()
    position_logger = PositionLogger()
    snapshot_logger = RawSnapshotLogger()
    live_logger = LiveAttemptLogger() if LIVE_TRADING else None
    live_executor = LiveExecutor() if LIVE_TRADING else None

    asset_ids = []
    for m in mappings:
        asset_ids.extend([m["yes_token_id"], m["no_token_id"]])

    def _on_book_update():
        """Called after every Polymarket WS message. Checks TP/SL/horizon exits."""
        for cp in trader.check_exits(store, set(), None):
            position_logger.log_exit(cp)
            print(
                f"EXIT [{cp.exit_reason}] {cp.market_name} {cp.side} "
                f"entry={cp.entry_price:.4f} exit={cp.exit_price:.4f} "
                f"pnl=${cp.pnl_usd:+.2f} hold={cp.hold_sec:.0f}s"
            )

    if LIVE_TRADING:
        print(f"Starting GUARDED LIVE TEST with {len(mappings)} active mapping(s). $10 hard cap is enforced by LiveExecutor.")
    else:
        print(f"Starting paper bot with {len(mappings)} active mapping(s). Checking for new games every {MAPPING_REFRESH_SECONDS}s.")

    try:
        await asyncio.gather(
            listen_books(asset_ids, store, book_logger=book_logger, on_book_update=_on_book_update),
            steam_loop(store, trader, signal_logger, event_detector, signal_engine, event_logger, position_logger, snapshot_logger, live_executor, live_logger, mappings, asset_ids),
        )
    except asyncio.CancelledError:
        pass
    finally:
        summary = trader.summary()
        print(f"\nSession summary: {summary}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")
