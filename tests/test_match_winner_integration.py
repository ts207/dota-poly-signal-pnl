import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

import main
from main import steam_loop
from poly_ws import BookStore
from paper_trader import PaperTrader
from storage import (
    SignalLogger, DotaEventLogger, PositionLogger, RawSnapshotLogger,
    LatencyLogger, LiveLeagueRawLogger, LiveLeagueFeatureLogger,
    SourceDelayLogger, BookRefreshRescueLogger, MatchWinnerSignalLogger
)
from signal_engine import EventSignalEngine
from event_detector import EventDetector, DotaEvent
from liveleague_features import LiveLeagueContextCache

@pytest.mark.asyncio
async def test_match_winner_sidecar_integration(tmp_path):
    # Mock loggers
    signal_logger = MagicMock(spec=SignalLogger)
    event_logger = MagicMock(spec=DotaEventLogger)
    position_logger = MagicMock(spec=PositionLogger)
    snapshot_logger = MagicMock(spec=RawSnapshotLogger)
    latency_logger = MagicMock(spec=LatencyLogger)
    llg_raw_logger = MagicMock(spec=LiveLeagueRawLogger)
    llg_feature_logger = MagicMock(spec=LiveLeagueFeatureLogger)
    source_delay_logger = MagicMock(spec=SourceDelayLogger)
    rescue_logger = MagicMock(spec=BookRefreshRescueLogger)
    
    match_winner_logger = MatchWinnerSignalLogger(log_dir=str(tmp_path))
    match_winner_logger.append = MagicMock()
    
    # Mock store and engine
    book_store = BookStore()
    book_store.update_direct("map_yes", best_bid=0.40, best_ask=0.45, bid_size=100, ask_size=100)
    book_store.update_direct("map_no", best_bid=0.50, best_ask=0.55, bid_size=100, ask_size=100)
    book_store.update_direct("match_yes", best_bid=0.60, best_ask=0.65, bid_size=100, ask_size=100)
    book_store.update_direct("match_no", best_bid=0.30, best_ask=0.35, bid_size=100, ask_size=100)
    
    trader = PaperTrader()
    signal_engine = EventSignalEngine(trader=trader)
    
    # Mock mappings
    mappings = [
        {
            "dota_match_id": "123",
            "market_type": "MATCH_WINNER",
            "series_type": 1, # BO3
            "current_game_number": 2,
            "series_score_yes": 0,
            "series_score_no": 1,
            "yes_token_id": "match_yes",
            "no_token_id": "match_no",
            "yes_team": "Team A",
            "no_team": "Team B",
        },
        {
            "dota_match_id": "123",
            "market_type": "MAP_WINNER",
            "game_number": 2,
            "yes_token_id": "map_yes",
            "no_token_id": "map_no",
            "yes_team": "Team A",
            "no_team": "Team B",
        }
    ]
    
    asset_ids = ["map_yes", "map_no", "match_yes", "match_no"]

    # Mock fetch_all_live_games to return a game
    async def mock_fetch(session, cache):
        return [
            {
                "match_id": "123",
                "game_time_sec": 600,
                "radiant_team": {"team_name": "Team A"},
                "dire_team": {"team_name": "Team B"},
                "data_source": "STEAM_LIVE",
                "score": [10, 5],
                "radiant_net_worth": 15000,
                "dire_net_worth": 10000,
            }
        ]
        
    # Mock event detector to force an event
    event_detector = MagicMock(spec=EventDetector)
    def mock_observe(game, mapping):
        # Only yield for the MAP_WINNER, wait - actually MATCH_WINNER mapping also receives observe?
        # main.py does event_detector.observe(game, mapping) for ALL mappings.
        return [DotaEvent(
            event_type="HERO_KILL",
            game_time=600,
            details={"hero_kill": 1},
            direction="radiant" if mapping["yes_team"] == "Team A" else "dire",
            severity="medium"
        )]
    event_detector.observe.side_effect = mock_observe
    
    llg_cache = LiveLeagueContextCache()

    # Mock asyncio.sleep to break the loop after first iteration
    async def mock_sleep(*args, **kwargs):
        raise asyncio.CancelledError()

    with patch('main.fetch_all_live_games', new=mock_fetch), patch('asyncio.sleep', new=mock_sleep):
        try:
            await steam_loop(
                book_store=book_store,
                trader=trader,
                signal_logger=signal_logger,
                event_detector=event_detector,
                signal_engine=signal_engine,
                event_logger=event_logger,
                position_logger=position_logger,
                snapshot_logger=snapshot_logger,
                latency_logger=latency_logger,
                live_executor=None,
                live_logger=None,
                llg_raw_logger=llg_raw_logger,
                llg_feature_logger=llg_feature_logger,
                source_delay_logger=source_delay_logger,
                rescue_logger=rescue_logger,
                match_winner_logger=match_winner_logger,
                llg_cache=llg_cache,
                mappings=mappings,
                asset_ids=asset_ids
            )
        except asyncio.CancelledError:
            pass
            
    # Check that match_winner_logger.append was called
    assert match_winner_logger.append.call_count >= 1
    
    # Check arguments
    found = False
    for call in match_winner_logger.append.call_args_list:
        args = call[0][0]
        if args.get("decision") == "skip" and args.get("skip_reason") == "research_mode_match_winner":
            found = True
            assert args["match_id"] == "123"
            assert args["event_type"] == "HERO_KILL"
            assert args["match_fair_after"] is not None
            assert args["map_bid"] == 0.40 or args["map_bid"] == 0.50
    assert found
