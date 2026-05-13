# Dota 2 → Polymarket Signal Bot (Paper / Read-Only)

This project monitors Steam live Dota 2 game state and compares it with Polymarket order-book data to study whether a **realistic paper fill** would have existed before the market repriced.

This build is intentionally **paper-only**. It does not place live orders.

## What changed in this update

Paper-trading readiness fixes:

- Paper entries now fill at the current best ask, not midpoint.
- Paper exits now sell at the current best bid, not midpoint.
- Signal edge is now `fair_price - executable_price`, where fair value is estimated in logit space from the pre-event anchor price and event impact.
- The signal log now records `fair_price`, `executable_price`, `executable_edge`, `remaining_move`, `source_update_age_sec`, `stream_delay_s`, and `data_source`.
- The bot defaults to trading only `top_live` Steam data and skips stale source updates; `stream_delay_s` is logged as spectator/broadcast metadata only and is not a freshness skip.
- Lead-swing scaling now uses the same dynamic game-time thresholds as event detection.
- Added event-specific max-fill safety caps so high-priority T4/wipe events are not blocked by the conservative default cap.
- Same-poll direction clusters are all evaluated first; the bot now enters only the best executable candidate by `executable_edge`, then `expected_move`.
- Removed legacy `market_move_90s` naming from code/dashboard/log headers.
- Added tests for bid/ask paper fills, source freshness guards, stale-source skips, event-specific fill caps, and best-candidate selection.

Previous event-model update included:

- Split T4 events into `FIRST_T4_TOWER_FALL` and `SECOND_T4_TOWER_FALL`.
- Removed generic `TOWER_FALL` and replaced it with `T2_TOWER_FALL`.
- Removed `KILL_SWING_30S` and replaced it with `KILL_BURST_30S`.
- Added `ULTRA_LATE_WIPE`, `MAJOR_COMEBACK`, and `MULTIPLE_T3_TOWERS_DOWN`.
- Kept Ancient/game-over out of tradable events; it is terminal-only.
- Added cluster scoring and overlap suppressions.

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
TRADE_EVENTS=ULTRA_LATE_WIPE,LATE_GAME_WIPE,STOMP_THROW,MAJOR_COMEBACK,EXTREME_LEAD_SWING_30S,OBJECTIVE_CONVERSION_T3,OBJECTIVE_CONVERSION_T4,FIRST_T4_TOWER_FALL,SECOND_T4_TOWER_FALL \
DISABLE_STRUCTURE_TRADES=true \
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
- `DISABLE_STRUCTURE_TRADES=true` blocks tower-only/structure-triggered trades for
  the first run.
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

The signal model separates raw events from higher-quality composite events:

- `OBJECTIVE_CONVERSION_T3` / `OBJECTIVE_CONVERSION_T4`: a high-ground/T4 structure falls in the same short window as a same-direction kill burst, net-worth swing, comeback, or throw event. These are preferred over tower-only entries because they indicate fight-to-objective conversion.
- `OBJECTIVE_CONVERSION_T2`: available for paper research, but not in the default live-test allowlist.
- Raw structure-only events are damped by the signal engine unless they also have same-direction support.
- Entries now require both `executable_edge >= required_edge` and `remaining_move >= MIN_LAG`, so paper signals are closer to the guarded live executor behavior.


### v7 strategy hardening notes

The default live allowlist now excludes confirmation-only triggers unless `ALLOW_CONFIRMATION_ONLY_LIVE_TRADES=true` is explicitly set. New `MULTIPLE_T2_TOWERS_DOWN` and `ALL_T2_TOWERS_DOWN` events are map-control context signals; they are used for scoring and edge buffers, not as default standalone live triggers. Live execution also respects the signal's event-specific `max_fill_price`, so high-confidence T3/T4 conversion signals are not accidentally blocked by the generic default fill cap.
