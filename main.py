from __future__ import annotations

import asyncio
import traceback
import aiohttp
import time

from steam_client import fetch_all_live_games, LeagueGameCache
from poly_ws import listen_books, BookStore
from signal_engine import EventSignalEngine
from paper_trader import PaperTrader
from storage import SignalLogger, DotaEventLogger, BookEventLogger, PositionLogger, RawSnapshotLogger, LiveAttemptLogger, LatencyLogger, LiveLeagueRawLogger, LiveLeagueFeatureLogger
from mapping import load_valid_mappings
from event_detector import EventDetector
from live_executor import LiveExecutor
from liveleague_features import LiveLeagueContextCache
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
    latency_logger: LatencyLogger,
    live_executor: LiveExecutor | None,
    live_logger: LiveAttemptLogger | None,
    llg_raw_logger: LiveLeagueRawLogger,
    llg_feature_logger: LiveLeagueFeatureLogger,
    llg_cache: LiveLeagueContextCache,
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
                
                # Update LiveLeague context cache from the league games in this poll
                lg_raw = await league_cache.get(session) if league_cache else []
                if lg_raw:
                    llg_received_at = time.time_ns()
                    llg_cache.update(lg_raw, llg_received_at)
                    for raw_game in lg_raw:
                        llg_raw_logger.log_raw(raw_game, llg_received_at)

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

                    # Attach LiveLeague context as metadata (non-blocking,
                    # never changes expected_move/edge/sizing)
                    llg_cache.attach_to_game(game, feature_logger=llg_feature_logger)

                    # Validate mapping identity against LLG context immediately
                    for mapping in mappings:
                        if str(mapping.get("dota_match_id") or "") in {match_id, str(game.get("lobby_id") or "")}:
                            mismatches = llg_cache.validate_mapping(game, mapping)
                            if mismatches:
                                for mm in mismatches:
                                    print(f"MAPPING MISMATCH {match_id}: {mm}")

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
                        event_detected_ns = time.time_ns()
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

                                signal_eval_start_ns = time.time_ns()
                                signal = signal_engine.evaluate_cluster(
                                    events=cluster_events,
                                    game=game,
                                    mapping=mapping,
                                    yes_book=yes_book,
                                    no_book=no_book,
                                    require_primary=not (LIVE_TRADING and ALLOW_CONFIRMATION_ONLY_LIVE_TRADES),
                                )
                                signal_evaluated_ns = time.time_ns()

                                # Attach LiveLeague context metadata to signal dict.
                                # This does NOT change expected_move, edge, sizing, or live entry.
                                # It is shadow-only unless freshness is proven.
                                ctx = game.get("liveleague_context")
                                ctx_fresh = (
                                    ctx is not None
                                    and game.get("liveleague_age_ms", 999999) <= 3000
                                    and game.get("liveleague_minus_toplive_game_time_sec") is not None
                                    and abs(game.get("liveleague_minus_toplive_game_time_sec", 999999)) <= 2
                                )
                                signal["liveleague_context_status"] = game.get("liveleague_context_status")
                                signal["liveleague_age_ms"] = game.get("liveleague_age_ms")
                                signal["liveleague_minus_toplive_game_time_sec"] = game.get("liveleague_minus_toplive_game_time_sec")
                                if ctx_fresh:
                                    signal["aegis_team"] = ctx.get("aegis_team")
                                    signal["radiant_dead_count"] = ctx.get("radiant_dead_count")
                                    signal["dire_dead_count"] = ctx.get("dire_dead_count")
                                    signal["radiant_core_dead_count"] = ctx.get("radiant_core_dead_count")
                                    signal["dire_core_dead_count"] = ctx.get("dire_core_dead_count")
                                    signal["radiant_max_respawn"] = ctx.get("radiant_max_respawn")
                                    signal["dire_max_respawn"] = ctx.get("dire_max_respawn")
                                    signal["radiant_top3_nw"] = ctx.get("radiant_top3_nw")
                                    signal["dire_top3_nw"] = ctx.get("dire_top3_nw")
                                    signal["liveleague_derived_events"] = game.get("liveleague_derived_events", [])
                                else:
                                    signal["liveleague_derived_events"] = []

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

                                # Latency logging
                                selected_book = (yes_book if tok_id == mapping["yes_token_id"] else no_book) if tok_id else (yes_book or no_book)
                                latency_row = {
                                    "match_id": str(game.get("match_id") or ""),
                                    "market_name": mapping.get("name"),
                                    "event_type": signal.get("event_type") or "+".join(event_names),
                                    "cluster_event_types": signal.get("cluster_event_types") or "+".join(event_names),
                                    "event_direction": event_direction,
                                    "game_time_sec": game.get("game_time_sec"),
                                    "data_source": game.get("data_source"),
                                    "steam_received_at_ns": game.get("received_at_ns"),
                                    "steam_source_update_age_sec": game.get("source_update_age_sec"),
                                    "stream_delay_s": game.get("stream_delay_s"),
                                    "event_detected_ns": event_detected_ns,
                                    "signal_eval_start_ns": signal_eval_start_ns,
                                    "signal_evaluated_ns": signal_evaluated_ns,
                                    "token_id": tok_id,
                                    "side": tok_side,
                                    "book_received_at_ns": selected_book.get("received_at_ns") if selected_book else None,
                                    "book_age_at_signal_ms": signal.get("book_age_ms"),
                                    "best_bid": selected_book.get("best_bid") if selected_book else None,
                                    "best_ask": selected_book.get("best_ask") if selected_book else None,
                                    "spread": signal.get("spread"),
                                    "ask_size": signal.get("ask_size"),
                                    "decision": signal.get("decision"),
                                    "skip_reason": signal.get("reason"),
                                    "fair_price": signal.get("fair_price"),
                                    "executable_price": signal.get("executable_price"),
                                    "executable_edge": signal.get("executable_edge"),
                                    "remaining_move": signal.get("remaining_move"),
                                    "required_edge": signal.get("required_edge"),
                                    "lag": signal.get("lag"),
                                }
                                latency_logger.log_latency(latency_row)

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
                                        "latency_row": latency_row,
                                    })

                            best = _best_signal_candidate(candidates)
                            if best:
                                signal = best["signal"]
                                cluster_events = best["events"]
                                event_direction = best["direction"]
                                latency_row = best["latency_row"]
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
                                    # Log live latency result
                                    live_latency_row = dict(latency_row)
                                    live_latency_row.update({
                                        "decision": "live_attempt_result",
                                        "live_submit_start_ns": attempt.submit_start_ns,
                                        "live_response_received_ns": attempt.response_received_ns,
                                        "live_submit_latency_ms": attempt.submit_latency_ms,
                                        "live_order_status": attempt.order_status,
                                        "live_reject_reason": attempt.reason_if_rejected,
                                        "live_submitted_size_usd": attempt.submitted_size_usd,
                                        "live_filled_size_usd": attempt.filled_size_usd,
                                        "live_avg_fill_price": attempt.avg_fill_price,
                                    })
                                    latency_logger.log_latency(live_latency_row)

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
                                    paper_attempt_ns = time.time_ns()
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
                                    paper_fill_ns = time.time_ns()

                                    # Log paper latency result
                                    paper_latency_row = dict(latency_row)
                                    paper_latency_row.update({
                                        "decision": "paper_entry_result",
                                        "paper_delay_ms": PAPER_EXECUTION_DELAY_MS,
                                        "paper_attempt_ns": paper_attempt_ns,
                                        "paper_fill_ns": paper_fill_ns,
                                        "paper_entry_result": "filled" if pos else "skipped",
                                        "paper_fill_price": pos.entry_price if pos else None,
                                        "skip_reason": reason if not pos else None,
                                    })
                                    latency_logger.log_latency(paper_latency_row)

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
    latency_logger = LatencyLogger()
    llg_raw_logger = LiveLeagueRawLogger()
    llg_feature_logger = LiveLeagueFeatureLogger()
    llg_cache = LiveLeagueContextCache()
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
            steam_loop(store, trader, signal_logger, event_detector, signal_engine, event_logger, position_logger, snapshot_logger, latency_logger, live_executor, live_logger, llg_raw_logger, llg_feature_logger, llg_cache, mappings, asset_ids),
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
