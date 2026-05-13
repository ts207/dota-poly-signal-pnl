from discover_markets import _extract_dota_event_urls, _is_map_winner_market, _parse_teams


def test_parse_teams_from_game_winner_question():
    yes, no = _parse_teams("Dota 2: Two Move vs Power Rangers - Game 3 Winner")
    assert yes == "Two Move"
    assert no == "Power Rangers"


def test_is_map_winner_market_accepts_game_winner_only():
    assert _is_map_winner_market({
        "question": "Dota 2: Two Move vs Power Rangers - Game 1 Winner",
        "outcomes": ["Two Move", "Power Rangers"],
    })
    assert not _is_map_winner_market({
        "question": "Dota 2: Two Move vs Power Rangers (BO5) - European Pro League Playoffs",
        "outcomes": ["Two Move", "Power Rangers"],
    })
    assert not _is_map_winner_market({
        "question": "Game 1: Both Teams Beat Roshan?",
        "outcomes": ["Yes", "No"],
    })


def test_extract_dota_event_urls_dedupes_listing_links():
    html = '''
    <a href="/esports/dota-2/european-pro-league/dota2-tm6-pr1-2026-05-12">Game View</a>
    <a href="/esports/dota-2/european-pro-league/dota2-tm6-pr1-2026-05-12">Game View</a>
    <a href="/esports/cs2/foo">CS2</a>
    '''
    urls = _extract_dota_event_urls(html)
    assert urls == [
        "https://polymarket.com/esports/dota-2/european-pro-league/dota2-tm6-pr1-2026-05-12"
    ]

from discover_markets import _outcome_token_pairs


def test_outcome_token_pairs_preserves_payload_order():
    market = {
        "question": "Dota 2: Team A vs Team B - Game 1 Winner",
        "outcomes": '["Team B", "Team A"]',
        "clobTokenIds": '["TOKEN_B", "TOKEN_A"]',
    }
    assert _outcome_token_pairs(market) == [("Team B", "TOKEN_B"), ("Team A", "TOKEN_A")]
