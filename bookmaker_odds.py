"""Live bookmaker odds client (The Odds API).

Used as a reference probability during live matches when Polymarket has
no active Dota markets. Compare estimate_p_radiant() against the sharp
market to detect calibration drift in real time.

Setup:
  1. Get a free key at https://the-odds-api.com  (500 req/month free)
  2. Add ODDS_API_KEY=<your_key> to .env

Usage in signal loop:
  odds_client = BookmakerOdds()
  ref = await odds_client.get_match_prob(session, "Team Radiant", "Team Dire")
  if ref:
      calibration_error = estimate_p_radiant(game) - ref.implied_p_home
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import aiohttp
from dotenv import load_dotenv

load_dotenv()

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

DOTA_SPORT_KEY = "esports_dota2"  # The Odds API sport key for Dota 2


@dataclass(frozen=True)
class MatchOdds:
    home_team: str
    away_team: str
    bookmaker: str
    home_price: float   # decimal odds (e.g. 1.80)
    away_price: float
    implied_p_home: float   # vig-removed implied probability for home/radiant
    implied_p_away: float


def _remove_vig(p1: float, p2: float) -> tuple[float, float]:
    """Normalise raw implied probabilities to sum to 1.0."""
    total = p1 + p2
    if total <= 0:
        return 0.5, 0.5
    return p1 / total, p2 / total


def _implied(decimal_odds: float) -> float:
    if decimal_odds <= 0:
        return 0.5
    return 1.0 / decimal_odds


def _parse_match_odds(market: dict, prefer_bookmaker: str = "pinnacle") -> MatchOdds | None:
    bookmakers = market.get("bookmakers") or []
    if not bookmakers:
        return None

    # Prefer Pinnacle (sharpest) then take the first available
    bm = next((b for b in bookmakers if b.get("key") == prefer_bookmaker), bookmakers[0])
    markets = bm.get("markets") or []
    h2h = next((m for m in markets if m.get("key") == "h2h"), None)
    if not h2h:
        return None

    outcomes = {o["name"]: o["price"] for o in h2h.get("outcomes") or []}
    home = market.get("home_team", "")
    away = market.get("away_team", "")
    if home not in outcomes or away not in outcomes:
        return None

    hp = _implied(outcomes[home])
    ap = _implied(outcomes[away])
    norm_hp, norm_ap = _remove_vig(hp, ap)

    return MatchOdds(
        home_team=home,
        away_team=away,
        bookmaker=bm.get("key", ""),
        home_price=outcomes[home],
        away_price=outcomes[away],
        implied_p_home=round(norm_hp, 4),
        implied_p_away=round(norm_ap, 4),
    )


def _norm(s: str) -> str:
    return (s or "").strip().casefold()


class BookmakerOdds:
    def __init__(self, api_key: str = ODDS_API_KEY, sport: str = DOTA_SPORT_KEY):
        self.api_key = api_key
        self.sport = sport
        self._cached: list[dict] = []

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_key != "replace_me")

    async def fetch_events(self, session: aiohttp.ClientSession) -> list[dict]:
        if not self.is_configured():
            return []
        url = f"{ODDS_API_BASE}/sports/{self.sport}/odds/"
        params = {
            "apiKey": self.api_key,
            "regions": "us,uk,eu",
            "markets": "h2h",
            "oddsFormat": "decimal",
        }
        try:
            async with session.get(url, params=params, timeout=8) as r:
                if r.status == 401:
                    print("BookmakerOdds: invalid API key — set ODDS_API_KEY in .env")
                    return []
                if r.status == 422:
                    print(f"BookmakerOdds: sport '{self.sport}' not found or not active")
                    return []
                r.raise_for_status()
                self._cached = await r.json()
                return self._cached
        except Exception as e:
            print(f"BookmakerOdds fetch error: {e}")
            return []

    async def get_match_prob(
        self,
        session: aiohttp.ClientSession,
        radiant_team: str,
        dire_team: str,
        prefer_bookmaker: str = "pinnacle",
        *,
        force_refresh: bool = False,
    ) -> MatchOdds | None:
        """Find match odds for the given team pair and return vig-removed probabilities.

        radiant_team is treated as the home side for comparison purposes.
        Uses the in-memory cache from the last fetch_events() call to avoid
        exhausting the free-tier quota (500 req/month). Pass force_refresh=True
        to re-fetch from the API.
        Returns None if no matching market is found.
        """
        if force_refresh or not self._cached:
            await self.fetch_events(session)
        events = self._cached
        r_norm = _norm(radiant_team)
        d_norm = _norm(dire_team)

        for event in events:
            home = _norm(event.get("home_team", ""))
            away = _norm(event.get("away_team", ""))
            if (r_norm in home or home in r_norm) and (d_norm in away or away in d_norm):
                return _parse_match_odds(event, prefer_bookmaker)
            if (d_norm in home or home in d_norm) and (r_norm in away or away in r_norm):
                # Teams are flipped relative to Dota sides — swap implied probs
                odds = _parse_match_odds(event, prefer_bookmaker)
                if odds:
                    return MatchOdds(
                        home_team=odds.away_team,
                        away_team=odds.home_team,
                        bookmaker=odds.bookmaker,
                        home_price=odds.away_price,
                        away_price=odds.home_price,
                        implied_p_home=odds.implied_p_away,
                        implied_p_away=odds.implied_p_home,
                    )
        return None

    async def list_dota_events(self, session: aiohttp.ClientSession) -> None:
        events = await self.fetch_events(session)
        if not events:
            print("No events found. Check ODDS_API_KEY and that Dota 2 markets are active.")
            return
        print(f"Found {len(events)} Dota 2 events:")
        for e in events:
            print(f"  {e.get('home_team')} vs {e.get('away_team')}  [{e.get('commence_time', '')[:16]}]")
