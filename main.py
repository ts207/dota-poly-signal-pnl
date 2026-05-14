from __future__ import annotations

import asyncio
import traceback
import aiohttp
import time
import os
import json

from steam_client import fetch_all_live_games, LeagueGameCache
from poly_ws import listen_books, BookStore
from signal_engine import EventSignalEngine, apply_probability_move
from paper_trader import PaperTrader
from storage import SignalLogger, DotaEventLogger, BookEventLogger, PositionLogger, RawSnapshotLogger, LiveAttemptLogger, LatencyLogger, LiveLeagueRawLogger, LiveLeagueFeatureLogger, SourceDelayLogger, BookRefreshRescueLogger, MatchWinnerSignalLogger
from mapping import load_valid_mappings
from event_detector import EventDetector
from live_executor import LiveExecutor
from liveleague_features import LiveLeagueContextCache, classify_liveleague_lag
from mapping_validator import validate_mapping_identity
from hybrid_nowcast import compute_hybrid_nowcast
from realtime_enrichment import maybe_enrich_realtime
from book_refresh import fetch_fresh_book
from event_taxonomy import event_tier, TIER_A_EVENTS, TIER_B_EVENTS
from series_model import compute_bo3_match_p
from team_utils import norm_team
from config import (
    STEAM_API_KEY, STEAM_POLL_SECONDS, PAPER_EXECUTION_DELAY_MS, LIVE_TRADING,
    ALLOW_CONFIRMATION_ONLY_LIVE_TRADES, MAX_BOOK_AGE_MS, MIN_EXECUTABLE_EDGE,
    MIN_LAG, MAX_SPREAD, MIN_ASK_SIZE_USD, PAPER_SLIPPAGE_CENTS, PAPER_TRADE_SIZE_USD,
    PRICE_LOOKBACK_SEC, REQUIRE_TOP_LIVE_FOR_SIGNALS, DOTA_FAIR_MODEL_PATH,
    MIN_ML_EDGE, ML_STRATEGY_ENABLED
)
from sync_markets import sync_markets_to_games, load_markets, write_markets
from dota_fair_model.inference import load_bundle
from dota_fair_model.features import build_feature_row

MAPPING_REFRESH_SECONDS = 60


def age_ms(ns: int | None) -> int:
    if not ns:
        return 10 ** 9
    return int((time.time_ns() - ns) / 1_000_000)


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


def _yes_fair_from_radiant(mapping: dict, game: dict, p_rad: float) -> tuple[float, str] | None:
    side_map = mapping.get("steam_side_mapping")
    if side_map == "normal":
        return p_rad, "radiant"
    if side_map == "reversed":
        return 1.0 - p_rad, "dire"

    yes_team = norm_team(mapping.get("yes_team"))
    radiant_team = norm_team(game.get("radiant_team"))
    dire_team = norm_team(game.get("dire_team"))
    if yes_team and radiant_team and yes_team == radiant_team:
        return p_rad, "radiant"
    if yes_team and dire_team and yes_team == dire_team:
        return 1.0 - p_rad, "dire"
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
    source_delay_logger: SourceDelayLogger,
    rescue_logger: BookRefreshRescueLogger,
    match_winner_logger: MatchWinnerSignalLogger,
    llg_cache: LiveLeagueContextCache,
    mappings: list[dict],
    asset_ids: list[str],
    model_bundle: Any | None = None,
    http_session: aiohttp.ClientSession | None = None,
):
    if not STEAM_API_KEY or STEAM_API_KEY == "replace_me":
        print("Missing STEAM_API_KEY. Copy .env.example to .env and fill it in.")
        return

    last_mapping_refresh = 0.0
    league_cache = LeagueGameCache()
    max_game_times: dict[str, int] = {}  # match_id -> max game_time_sec seen

    # Load team win stats for ML features
    team_stats = {}
    stats_path = "dota_fair_model/models/team_stats.json"
    if os.path.exists(stats_path):
        try:
            with open(stats_path, "r") as f:
                team_stats = json.load(f)
            print(f"Loaded {len(team_stats)} team win ratios from {stats_path}")
        except Exception as e:
            print(f"Failed to load team stats: {e}")

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

                    # Enrich with Realtime Stats as the base (120s delayed)
                    # This provides the high-fidelity features (per-player NW, dead counts)
                    # that GetTopLiveGame (0s delayed) lacks.
                    await maybe_enrich_realtime(game, session)

                    # Validate mapping identity against LLG context immediately
                    for mapping in mappings:
                        if str(mapping.get("dota_match_id") or "") in {match_id, str(game.get("lobby_id") or "")}:
                            mapping_result = validate_mapping_identity(mapping, game)
                            game["mapping_confidence"] = mapping_result.mapping_confidence
                            game["mapping_errors"] = ";".join(mapping_result.mapping_errors)
                            game["team_id_match"] = mapping_result.team_id_match
                            game["market_game_number_match"] = mapping_result.market_game_number_match
                            game["duplicate_match_id_error"] = mapping_result.duplicate_match_id_error
                            if mapping_result.mapping_errors:
                                print(f"MAPPING MISMATCH {match_id}: {game['mapping_errors']}")

                    if game.get("liveleague_context"):
                        ctx = game["liveleague_context"]
                        llg_gt = ctx.get("game_time_sec")
                        top_gt = game.get("game_time_sec")
                        lag = None
                        if llg_gt is not None and top_gt is not None:
                            lag = top_gt - llg_gt
                        wall_gap = None
                        if game.get("received_at_ns") and ctx.get("received_at_ns"):
                            wall_gap = round((game["received_at_ns"] - ctx["received_at_ns"]) / 1_000_000_000, 3)
                        source_delay_logger.log_source_delay({
                            "match_id": match_id,
                            "lobby_id": game.get("lobby_id"),
                            "league_id": game.get("league_id"),
                            "liveleague_received_at_ns": ctx.get("received_at_ns"),
                            "liveleague_game_time_sec": llg_gt,
                            "nearest_toplive_received_at_ns": game.get("received_at_ns"),
                            "nearest_toplive_game_time_sec": top_gt,
                            "game_time_lag_sec": lag,
                            "stream_delay_s": ctx.get("stream_delay_s"),
                            "wall_clock_receive_gap_sec": wall_gap,
                            "liveleague_usage": classify_liveleague_lag(lag),
                        })

                    # Guard: Ignore non-TopLive sources for event detection if required
                    if REQUIRE_TOP_LIVE_FOR_SIGNALS and data_source != "top_live":
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
                                    and game.get("game_time_lag_sec") is not None
                                    and abs(game.get("game_time_lag_sec", 999999)) <= 2
                                )
                                signal["liveleague_context_status"] = game.get("liveleague_context_status")
                                signal["liveleague_age_ms"] = game.get("liveleague_age_ms")
                                signal["game_time_lag_sec"] = game.get("game_time_lag_sec")
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
                                # ML prediction for slow_model_fair
                                slow_model_fair = None
                                if model_bundle is not None:
                                    try:
                                        # Use event_direction to decide if we want Radiant or Dire probability
                                        feat_row = build_feature_row(game)
                                        pred = model_bundle.predict_radiant(feat_row)
                                        p_rad = pred.get("radiant_fair_probability")
                                        if p_rad is not None:
                                            # If event is for radiant, slow_model_fair is p_radiant.
                                            # If event is for dire, slow_model_fair is 1 - p_radiant.
                                            slow_model_fair = p_rad if event_direction == "radiant" else (1.0 - p_rad)
                                    except Exception as e:
                                        print(f"ML prediction error: {e}")

                                lag = game.get("game_time_lag_sec")
                                nowcast = compute_hybrid_nowcast(
                                    latest_liveleague_features=ctx,
                                    latest_toplive_snapshot=game,
                                    toplive_event_cluster=cluster_events,
                                    source_delay_metrics={"game_time_lag_sec": lag},
                                    slow_model_fair=slow_model_fair,
                                    event_only_fair=signal.get("fair_price"),
                                )
                                nowcast_data = nowcast.to_dict()
                                signal.update(nowcast_data)
                                signal["mapping_confidence"] = game.get("mapping_confidence")
                                signal["mapping_errors"] = game.get("mapping_errors")
                                signal["team_id_match"] = game.get("team_id_match")
                                signal["market_game_number_match"] = game.get("market_game_number_match")
                                signal["duplicate_match_id_error"] = game.get("duplicate_match_id_error")

                                if nowcast.hybrid_fair is not None:
                                    hybrid_signal = signal_engine.evaluate_cluster(
                                        events=cluster_events,
                                        game=game,
                                        mapping=mapping,
                                        yes_book=yes_book,
                                        no_book=no_book,
                                        require_primary=not (LIVE_TRADING and ALLOW_CONFIRMATION_ONLY_LIVE_TRADES),
                                        fair_price_override=nowcast.hybrid_fair,
                                        fair_source="hybrid",
                                    )
                                    for key in (
                                        "liveleague_context_status", "liveleague_age_ms", "game_time_lag_sec",
                                        "aegis_team", "radiant_dead_count", "dire_dead_count",
                                        "radiant_core_dead_count", "dire_core_dead_count",
                                        "radiant_max_respawn", "dire_max_respawn",
                                        "radiant_top3_nw", "dire_top3_nw", "liveleague_derived_events",
                                        "mapping_confidence", "mapping_errors", "team_id_match",
                                        "market_game_number_match", "duplicate_match_id_error",
                                    ):
                                        if key in signal:
                                            hybrid_signal[key] = signal[key]
                                    hybrid_signal.update(nowcast_data)
                                    signal = hybrid_signal

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

                                # ── Stale-book rescue for Tier A/B events ──
                                # When a Tier A/B signal is blocked by a stale/missing local book,
                                # fetch a fresh orderbook via REST and re-evaluate.
                                _rescue_tok_id = signal.get("token_id", "")
                                _rescue_skip = signal.get("reason", "")
                                _rescue_evt = signal.get("event_type") or ""
                                _rescue_tier = signal.get("event_tier") or ""
                                if (
                                    _rescue_skip in {"book_stale", "missing_book"}
                                    and _rescue_evt in (TIER_A_EVENTS | TIER_B_EVENTS)
                                    and http_session is not None
                                    and _rescue_tok_id
                                ):
                                    _local_book = book_store.get(_rescue_tok_id) or {}
                                    _local_bid = _local_book.get("best_bid")
                                    _local_ask = _local_book.get("best_ask")
                                    _local_spread = None
                                    if _local_bid is not None and _local_ask is not None:
                                        try:
                                            _local_spread = round(float(_local_ask) - float(_local_bid), 4)
                                        except (TypeError, ValueError):
                                            _local_spread = None
                                    _local_ask_size = _local_book.get("ask_size")
                                    _local_book_age = signal.get("book_age_ms")

                                    _rescue_row = {
                                        "match_id": str(game.get("match_id") or ""),
                                        "event_type": _rescue_evt,
                                        "event_tier": _rescue_tier,
                                        "event_direction": event_direction,
                                        "token_id": _rescue_tok_id,
                                        "local_book_age_ms": _local_book_age,
                                        "local_bid": _local_bid,
                                        "local_ask": _local_ask,
                                        "local_spread": _local_spread,
                                        "local_ask_size": _local_ask_size,
                                    }

                                    try:
                                        _fresh_book = await fetch_fresh_book(http_session, _rescue_tok_id, timeout_ms=2000)
                                    except Exception:
                                        _fresh_book = None

                                    if _fresh_book and _fresh_book.get("best_ask") is not None:
                                        _stored_fresh_book = book_store.update_direct(
                                            _rescue_tok_id,
                                            best_bid=_fresh_book.get("best_bid"),
                                            best_ask=_fresh_book.get("best_ask"),
                                            bid_size=_fresh_book.get("bid_size"),
                                            ask_size=_fresh_book.get("ask_size"),
                                            raw=_fresh_book.get("raw"),
                                        )
                                        _rescue_row["refresh_request_start_ns"] = _fresh_book.get("refresh_latency_ns")
                                        _rescue_row["refresh_response_ns"] = _fresh_book.get("received_at_ns")
                                        _rescue_row["refresh_latency_ms"] = round(_fresh_book.get("refresh_latency_ns", 0) / 1_000_000, 1)
                                        _rescue_row["fresh_bid"] = _fresh_book.get("best_bid")
                                        _rescue_row["fresh_ask"] = _fresh_book.get("best_ask")
                                        _rescue_row["fresh_spread"] = _fresh_book.get("spread")
                                        _rescue_row["fresh_ask_size"] = _fresh_book.get("ask_size")
                                        _fresh_ts = _fresh_book.get("received_at_ns")
                                        if _fresh_ts:
                                            _rescue_row["fresh_book_age_ms_if_available"] = int((time.time_ns() - _fresh_ts) / 1_000_000)
                                        else:
                                            _rescue_row["fresh_book_age_ms_if_available"] = None

                                        if _local_ask is not None and _fresh_book.get("best_ask") is not None:
                                            try:
                                                _rescue_row["local_to_fresh_ask_change"] = round(float(_fresh_book["best_ask"]) - float(_local_ask), 4)
                                            except (TypeError, ValueError):
                                                _rescue_row["local_to_fresh_ask_change"] = None

                                        # Re-evaluate with fresh book substituted
                                        _fresh_yes_book = yes_book
                                        _fresh_no_book = no_book
                                        _fresh_side = signal.get("side", "")
                                        if _fresh_side == "YES":
                                            if _rescue_tok_id == mapping.get("yes_token_id"):
                                                _fresh_yes_book = _stored_fresh_book
                                            else:
                                                _fresh_no_book = _stored_fresh_book
                                        else:
                                            if _rescue_tok_id == mapping.get("no_token_id"):
                                                _fresh_no_book = _stored_fresh_book
                                            else:
                                                _fresh_yes_book = _stored_fresh_book

                                        _fresh_signal = signal_engine.evaluate_cluster(
                                            events=cluster_events,
                                            game=game,
                                            mapping=mapping,
                                            yes_book=_fresh_yes_book,
                                            no_book=_fresh_no_book,
                                            require_primary=not (LIVE_TRADING and ALLOW_CONFIRMATION_ONLY_LIVE_TRADES),
                                            fair_price_override=signal.get("hybrid_fair"),
                                            fair_source="hybrid_rescue" if signal.get("hybrid_fair") is not None else None,
                                        )
                                        _fresh_signal.update(nowcast_data)
                                        for key in (
                                            "liveleague_context_status", "liveleague_age_ms", "game_time_lag_sec",
                                            "aegis_team", "radiant_dead_count", "dire_dead_count",
                                            "radiant_core_dead_count", "dire_core_dead_count",
                                            "radiant_max_respawn", "dire_max_respawn",
                                            "radiant_top3_nw", "dire_top3_nw", "liveleague_derived_events",
                                            "mapping_confidence", "mapping_errors", "team_id_match",
                                            "market_game_number_match", "duplicate_match_id_error",
                                        ):
                                            if key in signal:
                                                _fresh_signal[key] = signal[key]
                                        _rescue_row["fresh_executable_edge"] = _fresh_signal.get("executable_edge")
                                        _rescue_row["fresh_remaining_move"] = _fresh_signal.get("remaining_move")
                                        _rescue_row["fresh_decision"] = _fresh_signal.get("decision")
                                        _rescue_row["fresh_skip_reason"] = _fresh_signal.get("reason")
                                        _rescue_row["fresh_fair_source"] = _fresh_signal.get("fair_source")
                                        _rescue_row["fresh_hybrid_fair"] = _fresh_signal.get("hybrid_fair")

                                        # Markouts are sampled asynchronously after logging the rescue row.
                                        # A background task will fill them in later.
                                        _rescue_row["markout_3s"] = None
                                        _rescue_row["markout_10s"] = None
                                        _rescue_row["markout_30s"] = None
                                        if _fresh_signal.get("decision") == "paper_buy_yes":
                                            signal = _fresh_signal
                                            yes_book = _fresh_yes_book
                                            no_book = _fresh_no_book
                                    else:
                                        _rescue_row["refresh_latency_ms"] = None
                                        _rescue_row["fresh_bid"] = None
                                        _rescue_row["fresh_ask"] = None
                                        _rescue_row["fresh_spread"] = None
                                        _rescue_row["fresh_ask_size"] = None
                                        _rescue_row["fresh_book_age_ms_if_available"] = None
                                        _rescue_row["local_to_fresh_ask_change"] = None
                                        _rescue_row["fresh_executable_edge"] = None
                                        _rescue_row["fresh_remaining_move"] = None
                                        _rescue_row["fresh_decision"] = "rescue_failed"
                                        _rescue_row["fresh_skip_reason"] = "fresh_book_fetch_empty" if _fresh_book is None else "fresh_book_missing_ask"
                                        _rescue_row["markout_3s"] = None
                                        _rescue_row["markout_10s"] = None
                                        _rescue_row["markout_30s"] = None

                                    rescue_logger.log_rescue(_rescue_row)
                                    _rescue_lat = _rescue_row.get("refresh_latency_ms")
                                    _rescue_lat_str = f"{_rescue_lat:.0f}ms" if _rescue_lat is not None else "timeout"
                                    print(
                                        f"BOOK_RESCUE {_rescue_evt} tier={_rescue_tier} "
                                        f"local_age={_local_book_age}ms fresh_decision={_rescue_row.get('fresh_decision', '')} "
                                        f"fresh_reason={_rescue_row.get('fresh_skip_reason', '')} latency={_rescue_lat_str}"
                                    )

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
                                    "fair_source": signal.get("fair_source"),
                                    "required_edge": signal.get("required_edge"),
                                    "lag": signal.get("lag"),
                                    "mapping_confidence": game.get("mapping_confidence"),
                                    "mapping_errors": game.get("mapping_errors"),
                                    "team_id_match": game.get("team_id_match"),
                                    "market_game_number_match": game.get("market_game_number_match"),
                                    "duplicate_match_id_error": game.get("duplicate_match_id_error"),
                                    "slow_model_fair": signal.get("slow_model_fair"),
                                    "fast_event_adjustment": signal.get("fast_event_adjustment"),
                                    "hybrid_fair": signal.get("hybrid_fair"),
                                    "hybrid_confidence": signal.get("hybrid_confidence"),
                                    "uncertainty_penalty": signal.get("uncertainty_penalty"),
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

                                if mapping.get("market_type") == "MATCH_WINNER":
                                    # Task 4: Match Winner research mode sidecar
                                    try:
                                        m_yes_book = yes_book or {}
                                        m_no_book = no_book or {}
                                        match_bid = m_yes_book.get("best_bid") if tok_side == "YES" else m_no_book.get("best_bid")
                                        match_ask = m_yes_book.get("best_ask") if tok_side == "YES" else m_no_book.get("best_ask")

                                        # Find the corresponding Map Winner mapping to get Map prices
                                        map_m = next((m for m in mappings if str(m.get("dota_match_id")) == str(mapping.get("dota_match_id")) and m.get("market_type") == "MAP_WINNER"), None)

                                        row = {
                                            "timestamp_ns": time.time_ns(),
                                            "match_id": str(game.get("match_id") or ""),
                                            "event_type": signal.get("event_type") or "+".join(event_names),
                                            "event_direction": event_direction,
                                            "match_token_id": tok_id,
                                            "match_bid": match_bid,
                                            "match_ask": match_ask,
                                            "match_book_age_ms": signal.get("book_age_ms"),
                                            "match_fair_after": signal.get("fair_price"),
                                            "match_edge": signal.get("executable_edge"),
                                            "decision": "skip",
                                            "skip_reason": "research_mode_match_winner",
                                        }

                                        # Try to fill in map-based fair values if we have a map mapping
                                        if map_m:
                                            map_yes_tok = map_m.get("yes_token_id")
                                            map_no_tok = map_m.get("no_token_id")
                                            row["map_token_id"] = map_yes_tok if tok_side == "YES" else map_no_tok

                                            m_yes_b = book_store.get(map_yes_tok) or {}
                                            m_no_b = book_store.get(map_no_tok) or {}
                                            map_book = m_yes_b if tok_side == "YES" else m_no_b
                                            row["map_bid"] = map_book.get("best_bid")
                                            row["map_ask"] = map_book.get("best_ask")
                                            row["map_book_age_ms"] = age_ms(map_book.get("received_at_ns"))

                                            # Anchor for map before event
                                            map_anchor = signal_engine._price_n_seconds_ago(row["map_token_id"], PRICE_LOOKBACK_SEC)
                                            if map_anchor is None:
                                                map_anchor = signal_engine._pregame_price.get(row["map_token_id"])

                                            if map_anchor is not None:
                                                row["current_map_p_before"] = map_anchor
                                                expected_move = signal.get("expected_move") or 0.0
                                                row["current_map_p_after"] = apply_probability_move(map_anchor, expected_move)

                                                # Compute match_fair_before
                                                p_next_yes = float(mapping.get("p_next_yes") or row["current_map_p_after"])
                                                row["p_next_yes"] = p_next_yes
                                                row["p_next_source"] = "mapping" if mapping.get("p_next_yes") else "map_fair"
                                                row["neutral_p_next_yes"] = 0.5

                                                series_score_yes = int(game.get("series_score_yes", 0))
                                                series_score_no = int(game.get("series_score_no", 0))
                                                current_game_number = int(game.get("current_game_number") or game.get("game_number_in_series") or 1)
                                                series_type_val = int(mapping.get("series_type") or 1)

                                                try:
                                                    p_map_before = map_anchor if tok_side == "YES" else 1.0 - map_anchor
                                                    p_next_yes_val = p_next_yes if tok_side == "YES" else 1.0 - p_next_yes

                                                    row["match_fair_before"] = compute_bo3_match_p(
                                                        p_current_map_yes=max(0.01, min(0.99, p_map_before)),
                                                        p_next_yes=max(0.01, min(0.99, p_next_yes_val)),
                                                        series_score_yes=series_score_yes,
                                                        series_score_no=series_score_no,
                                                        current_game_number=current_game_number,
                                                        series_type=series_type_val,
                                                    )
                                                    if tok_side == "NO":
                                                        row["match_fair_before"] = 1.0 - row["match_fair_before"]

                                                    if row["match_fair_before"] is not None and row["match_fair_after"] is not None:
                                                        row["match_fair_delta"] = row["match_fair_after"] - row["match_fair_before"]
                                                except Exception:
                                                    pass

                                        match_winner_logger.log_match_signal(row)
                                    except Exception as e:
                                        print(f"Error in MATCH_WINNER sidecar: {e}")
                                        traceback.print_exc()
                                    finally:
                                        signal["decision"] = "skip"
                                        signal["reason"] = "research_mode_match_winner"

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

                # 2. Tick-level ML valuation arbitrage (always-on)
                if ML_STRATEGY_ENABLED and model_bundle:
                    for game in active_games:
                        # Skip if we already just processed events for this match in this poll
                        # (The hybrid nowcast already incorporated the ML fair for those)
                        # Actually, running ML check every tick is safer to catch gradual drifts.
                        
                        match_id = str(game.get("match_id") or "")
                        game_time = game.get("game_time_sec") or 0
                        if game_time < 300: # 5m guard
                            continue
                            
                        # Inject team win ratios from historical stats
                        r_id = str(game.get("radiant_team_id") or "")
                        d_id = str(game.get("dire_team_id") or "")
                        game["radiant_team_win_ratio"] = team_stats.get(r_id, 0.5)
                        game["dire_team_win_ratio"] = team_stats.get(d_id, 0.5)
                        
                        for mapping in mappings:
                            if str(mapping["dota_match_id"]) not in {match_id, str(game.get("lobby_id") or "")}:
                                continue
                            
                            yes_tok = mapping["yes_token_id"]
                            no_tok = mapping["no_token_id"]
                                
                            try:
                                feat_row = build_feature_row(game)
                                pred = model_bundle.predict_radiant(feat_row)
                                p_rad = pred.get("radiant_fair_probability")
                                if p_rad is None: continue
                                
                                yes_book = book_store.get(yes_tok)
                                if not yes_book or not yes_book.get("best_ask"): continue
                                yes_bid = yes_book.get("best_bid")
                                if yes_bid is None:
                                    continue
                                
                                yes_fair_direction = _yes_fair_from_radiant(mapping, game, p_rad)
                                if yes_fair_direction is None:
                                    continue
                                yes_fair, yes_direction = yes_fair_direction
                                trader.update_fair_value(yes_tok, yes_fair)
                                trader.update_fair_value(no_tok, 1.0 - yes_fair)

                                # Only enter if we don't already have an open position in this market.
                                # Existing positions use the refreshed fair value in check_exits().
                                if yes_tok in trader.positions or no_tok in trader.positions:
                                    continue

                                # Evaluate the market YES token using the mapped Steam side.
                                mkt_price = float(yes_book["best_ask"])
                                spread = mkt_price - float(yes_bid)
                                ask_size = yes_book.get("ask_size")
                                if mkt_price <= 0.05 or mkt_price >= 0.95:
                                    continue
                                if spread > MAX_SPREAD:
                                    continue
                                if ask_size is not None and mkt_price * float(ask_size) < MIN_ASK_SIZE_USD:
                                    continue

                                edge = yes_fair - mkt_price
                                
                                if edge >= MIN_ML_EDGE:
                                    signal = {
                                        "event_type": "ML_ARBITRAGE",
                                        "event_direction": yes_direction,
                                        "side": "YES",
                                        "token_id": yes_tok,
                                        "fair_price": round(yes_fair, 4),
                                        "executable_price": mkt_price,
                                        "executable_edge": round(edge, 4),
                                        "remaining_move": round(edge, 4),
                                        "expected_move": round(edge, 4),
                                        "required_edge": MIN_ML_EDGE,
                                        "ask": mkt_price,
                                        "bid": float(yes_bid),
                                        "spread": round(spread, 4),
                                        "ask_size": ask_size,
                                        "decision": "paper_buy_yes",
                                        "reason": "ml_valuation_edge",
                                        "severity": "ML",
                                        "game_time_sec": game_time,
                                    }
                                    
                                    # Reuse paper entry logic
                                    paper_attempt_ns = time.time_ns()
                                    pos, reason = trader.enter(
                                        signal=signal,
                                        token_id=yes_tok,
                                        side="YES",
                                        book_store=book_store,
                                        match_id=match_id,
                                        market_name=mapping.get("name"),
                                        opposing_token_id=no_tok,
                                    )
                                    if pos:
                                        position_logger.log_entry(pos)
                                        print(f"ML_ENTER {mapping['name']} YES price={pos.entry_price:.4f} edge={edge:.4f}")

                            except Exception as e:
                                print(f"Tick-level ML error: {e}")

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
    source_delay_logger = SourceDelayLogger()
    llg_cache = LiveLeagueContextCache()
    live_logger = LiveAttemptLogger() if LIVE_TRADING else None
    live_executor = LiveExecutor() if LIVE_TRADING else None
    rescue_logger = BookRefreshRescueLogger()
    match_winner_logger = MatchWinnerSignalLogger(log_dir="logs")

    model_bundle = None
    if os.path.exists(DOTA_FAIR_MODEL_PATH):
        print(f"Loading dota_fair model from {DOTA_FAIR_MODEL_PATH}...")
        try:
            model_bundle = load_bundle(DOTA_FAIR_MODEL_PATH)
            print(f"  model loaded (phases: {', '.join(model_bundle.models.keys())})")
        except Exception as e:
            print(f"  failed to load model: {e}")
    else:
        print(f"No model found at {DOTA_FAIR_MODEL_PATH} (skipping ML features)")

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
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                listen_books(asset_ids, store, book_logger=book_logger, on_book_update=_on_book_update),
                steam_loop(store, trader, signal_logger, event_detector, signal_engine, event_logger, position_logger, snapshot_logger, latency_logger, live_executor, live_logger, llg_raw_logger, llg_feature_logger, source_delay_logger, rescue_logger, match_winner_logger, llg_cache, mappings, asset_ids, model_bundle=model_bundle, http_session=session),
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
