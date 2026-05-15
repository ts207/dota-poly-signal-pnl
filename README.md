# Dota 2 → Polymarket Signal Bot

This project monitors Steam live Dota 2 game state and compares it with Polymarket order-book data to trade in-game events.

Paper by default. Optional guarded live-path test exists and is disabled by default.

## New in latest update

- **Latency Observability:** Dedicated `logs/latency.csv` decomposes the full path from Steam snapshot receipt → event detection → signal evaluation → order attempt.
- **Cadence-aware events:** Fixed `30s/60s` primary events are retired. Events now use actual TopLive snapshot gaps and emit tactical `POLL_*`, `OBJECTIVE_CONVERSION_*`, and `BASE_PRESSURE_*` names with `event_schema_version=cadence_v1`.
- **Survival Analysis:** `reaction_lag.py` now produces `logs/stale_ask_survival.csv`, estimating how many seconds an executable price persisted after a signal.
- **Pressure Metadata:** Dota events now include `base_pressure_score`, `fight_pressure_score`, `economic_pressure_score`, and `conversion_score`.
- **Bug Fixes:** Corrected latency-row cluster mapping and synchronized signal evaluation timestamps.

## Note on Live Status

Live trading is disabled by default. When enabled, the default live allowlist keeps Tier A and Tier B primary events enabled. Live execution still rejects non-`cadence_v1`, stale-gap, invalid-gap, low-quality, research, unknown, and retired events.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `STEAM_API_KEY` in `.env`.

## Discover candidate markets

```bash
python discover_markets.py
```

Copy the relevant market/token IDs into `markets.yaml` only after manually confirming the market is a **current map winner** market. Version 1 intentionally supports only `MAP_WINNER`.

## Configure one market

Edit `markets.yaml` with a real Polymarket YES/NO token pair and the Steam match/lobby ID:

```yaml
markets:
  - name: "Team A vs Team B Game 1"
    market_id: "..."
    condition_id: "..."
    yes_token_id: "..."
    no_token_id: "..."
    market_type: "MAP_WINNER"
    yes_team: "Team A"
    no_team: "Team B"
    dota_match_id: "..."
    confidence: 1.0
```

Only use `MAP_WINNER` until you add a proper series model. Do not treat current-map probability as series probability.

## Run

```bash
python main.py
```

The bot now writes six main logs after you run the analysis scripts:

- `logs/signals.csv` — every signal/skip decision,
- `logs/paper_trades.csv` — bid/ask-realistic paper entries and exits,
- `logs/dota_events.csv` — meaningful Dota state changes,
- `logs/book_events.csv` — Polymarket top-of-book updates from WebSocket messages,
- `logs/positions.csv` — reconstructed open paper positions marked at latest bid,
- `logs/pnl_summary.csv` — fill rate and bid-marked PnL by latency scenario.

## Analyze logs

Run the basic summary:

```bash
python analyze_logs.py
```

Then run reaction-lag and PnL analysis:

```bash
python reaction_lag.py
python mark_positions.py
python analyze_logs.py
```

`reaction_lag.py` writes `logs/reaction_lag.csv` and measures:

- time from Dota event → any YES ask move,
- time from Dota event → expected-direction YES ask move,
- time from Dota event → spread widening,
- time from Dota event → ask-side liquidity drop.

The main question is still:

```text
Did fillable edge survive realistic latency and slippage?
```

Do not look only at whether the price eventually moved. `mark_positions.py` values long YES paper positions at the latest visible bid, not midpoint, ask, or model fair value.

## Key paper-trading knobs

The current signal-layer knobs are explicit in `.env.example`:

```text
MIN_LAG=0.08
MIN_EXECUTABLE_EDGE=0.03
PRICE_LOOKBACK_SEC=10
DEFAULT_MAX_FILL_PRICE=0.80
MAX_SPREAD=0.06
MIN_ASK_SIZE_USD=25
```

`DEFAULT_MAX_FILL_PRICE` is a conservative fallback. Important events have higher in-code safety caps, while the real trade filter remains `fair_price - executable_price`.

## Suggested validation rule

Continue only if you see:

- 100+ paper signals,
- near-zero mapping errors,
- positive bid-marked paper PnL under best-ask entries and best-bid exits,
- enough top-book size,
- reaction-lag rows showing stale asks survived long enough to fill.

Stop or redesign if profits disappear after best-ask entry / best-bid exit accounting.

## Tests

```bash
pytest
```

## Safety notes

Keep `.env`, `.venv`, `logs/`, CSVs, and keys out of Git/ZIPs. Rotate any API key that was accidentally shared.


## Recent implementation notes

This patched build includes several safety/correctness improvements for paper latency research:

- Polymarket discovery now maps `outcomes[i]` to `clobTokenIds[i]` instead of assuming the question-string team order matches token order.
- Shared team-name normalization lives in `team_utils.py` and is used by sync, signal evaluation, and reaction-lag analysis.
- `GetLiveLeagueGames` is cached with `LeagueGameCache`; the fast poll loop can keep using `GetTopLiveGame` without blocking on league metadata every tick.
- 30s/60s event windows now require a nearby historical snapshot so irregular polling cannot label a 90s swing as a 30s event.
- Paper positions store `fair_price`, and take-profit exits use model fair when available instead of blindly using `entry + expected_move`.
- `PAPER_EXECUTION_DELAY_MS` can simulate taker latency before a paper entry is filled.

## Guarded $10 live-path test

This build now includes an optional live-path executor for a tiny order-flow test.
It is disabled by default. Its purpose is to verify that FAK/FOK order submission,
tick rounding, caps, rejection logging, and markouts work. It is not a free-running
profit bot.

Install the optional live dependency only on the machine that will run the live test:

```bash
pip install -r requirements-live.txt
```

Recommended first live settings:

```bash
LIVE_TRADING=true \
MAX_TOTAL_LIVE_USD=10 \
MAX_TRADE_USD=1 \
MAX_OPEN_POSITIONS=1 \
ORDER_TYPE=FAK \
MIN_EXECUTABLE_EDGE=0.08 \
MIN_LAG=0.08 \
MAX_SPREAD=0.06 \
ALLOW_GAME_OVER_ONLY=false \
ALLOW_EVENT_TRADES=true \
TRADE_EVENTS=BASE_PRESSURE_T3_COLLAPSE,BASE_PRESSURE_T4,OBJECTIVE_CONVERSION_T3,OBJECTIVE_CONVERSION_T4,POLL_COMEBACK_RECOVERY,POLL_FIGHT_SWING,POLL_KILL_BURST_CONFIRMED,POLL_LATE_FIGHT_FLIP,POLL_LEAD_FLIP_WITH_KILLS,POLL_MAJOR_COMEBACK_RECOVERY,POLL_STOMP_THROW_CONFIRMED,POLL_ULTRA_LATE_FIGHT_FLIP,THRONE_EXPOSED \
DISABLE_STRUCTURE_TRADES=false \
LIVE_REQUIRE_CADENCE_SCHEMA=true \
LIVE_ALLOWED_CADENCE_QUALITIES=direct,normal \
LIVE_MIN_EVENT_QUALITY=0.60 \
DEFAULT_MAX_FILL_PRICE=0.80 \
PRICE_LOOKBACK_SEC=10 \
MAX_BOOK_AGE_MS=1000 \
MAX_STEAM_AGE_MS=1500 \
python main.py
```

The live executor sends only capped BUY market orders using FAK/FOK semantics. For
BUY orders, `price` is used as the worst acceptable price cap. The code computes:

```text
price_cap = round_down_to_tick(fair_price - LIVE_SAFETY_MARGIN, tick_size)
```

Then it rejects the order if the fresh best ask is above the cap, the edge is below
8 cents, the lag is below 8 cents, spread is wider than 6 cents, the book is stale,
or the Steam update is stale.

Live attempts are written to:

```text
logs/live_attempts.csv
```

Each attempt logs event type, direction, fair price, best ask, price cap, edge,
spread, book age, Steam age, order type, submitted size, filled size, average fill,
raw order status, rejection reason, and 3s/10s/30s markouts. A submit row is written
immediately, and a markout row is written after 30 seconds.

Safety behavior:

- `MAX_TOTAL_LIVE_USD` caps total submitted live notional in-process.
- `MAX_TRADE_USD` defaults to $1.
- `MAX_OPEN_POSITIONS=1` means one successful fill stops further live attempts in
  the same process unless you add live exits or restart intentionally.
- `DISABLE_STRUCTURE_TRADES=false` keeps Tier A/B objective and base-pressure
  events enabled. Set it to `true` only if you want to block all objective/base
  pressure entries and trade fight-only events.
- `LIVE_REQUIRE_CADENCE_SCHEMA=true` requires `event_schema_version=cadence_v1`.
- `LIVE_ALLOWED_CADENCE_QUALITIES=direct,normal` blocks stale and invalid poll gaps.
- `LIVE_MIN_EVENT_QUALITY=0.60` blocks weak event clusters before order submission.
- No GTC resting orders are sent by this module.

Credential environment variables are read as:

```text
POLY_PRIVATE_KEY or PK
POLY_CLOB_API_KEY or CLOB_API_KEY
POLY_CLOB_SECRET or CLOB_SECRET
POLY_CLOB_PASS_PHRASE or CLOB_PASS_PHRASE
POLY_SIGNATURE_TYPE optional
POLY_FUNDER_ADDRESS optional
```

Do not run live unless your account, jurisdiction, balances, allowances, and platform
eligibility are valid.

### Event/strategy model notes

The signal model separates raw components from higher-quality tactical events:

- `OBJECTIVE_CONVERSION_T3` / `OBJECTIVE_CONVERSION_T4`: a high-ground/T4 structure falls in the same short window as a same-direction kill burst, net-worth swing, comeback, or throw event. These are preferred over tower-only entries because they indicate fight-to-objective conversion.
- `OBJECTIVE_CONVERSION_T2`: available for paper research, but not in the default live-test allowlist.
- Raw structure-only events are components. Primary structure signals are `BASE_PRESSURE_T3_COLLAPSE`, `BASE_PRESSURE_T4`, and `THRONE_EXPOSED` when source reliability permits.
- `POLL_FIGHT_SWING`, `POLL_KILL_BURST_CONFIRMED`, `POLL_LEAD_FLIP_WITH_KILLS`, recovery, throw, late-fight, and ultra-late flip events are based on actual observed snapshot gaps rather than idealized windows.
- Entries now require both `executable_edge >= required_edge` and `remaining_move >= MIN_LAG`, so paper signals are closer to the guarded live executor behavior.


### v7 strategy hardening notes

The default live allowlist includes Tier A/B primary events and excludes research/retired triggers. Live execution also respects the signal's event-specific `max_fill_price`, cadence quality, event schema, and event quality, so small-capital tests do not accidentally trade stale or retired event names.
