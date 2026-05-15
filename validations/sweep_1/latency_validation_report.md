# Latency Validation Report

Sweep root: `validations/sweep_1`

## Scenario Comparison

| delay | attempts | filled | fill rate | bid PnL | PnL % | m3 avg | m10 avg | m30 avg | survive delay | src lag p95 | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0ms | 0 | 0 |  |  |  |  |  |  |  |  | no paper attempts |
| 250ms | 0 | 0 |  |  |  |  |  |  |  |  | no paper attempts |
| 500ms | 0 | 0 |  |  |  |  |  |  |  |  | no paper attempts |
| 1000ms | 0 | 0 |  |  |  |  |  |  |  |  | no paper attempts |
| 2000ms | 0 | 0 |  |  |  |  |  |  |  |  | no paper attempts |

## Verdict

No realistic-delay scenario passed PnL/fill sanity.
No mechanical collapse flags were triggered.

## Metric Definitions

- Fill rate uses `latency.csv` rows where `decision == paper_entry_result`.
- Bid-marked PnL uses the `overall` row from archived `pnl_summary.csv`.
- Markouts use archived `markouts.csv` 3s/10s/30s columns.
- Stale survival reports the share of survival rows where `stale_ask_survival_ms >= delay_ms`.
- Source-delay stats use archived `source_delay.csv` only.
