# Trigger Tuning Guide

This document explains how to adjust the adaptive trigger knobs that emulate (and refine) the @gabagool22 playbook.

## Shadow Mode (Market Intelligence)

Run the lightweight logger to collect real fills before touching production thresholds:

```bash
python shadow_listener.py --market btc-updown-15m-1764871200 --output data/btc-trades.log
```

Each line is JSON with timestamp, price, side, size, maker/taker. After ~24 h you get a “heatmap” of where smart money strikes. Use that to calibrate the momentum thresholds below.

## Key Environment Variables

| Variable | Description | Typical Value |
| --- | --- | --- |
| `MAX_PAIR_COST` | Combined YES+NO cap before arming pair traps (adaptive cap sits below this) | `0.995` |
| `ORDER_SIZE` | Size (shares) for every signed order | `50` |
| `ENABLE_MOMENTUM` | Master switch for single-side shots | `true` |
| `MOMENTUM_YES_MAX_PRICE` / `MOMENTUM_BUY_LOW_THRESHOLD` | Arm YES trap when price ≤ value | `0.55` |
| `MOMENTUM_NO_MIN_PRICE` / `MOMENTUM_BUY_LOW_THRESHOLD_NO` | Arm NO trap when price ≥ value | `0.55` |
| `MOMENTUM_PRICE_BUFFER` | Basis-point buffer applied to momentum bids | `0.01` |
| `MOMENTUM_SPREAD_MAX` | Require YES+NO sum to stay within this band of 1.0 to allow momentum | `0.01` |
| `MOMENTUM_SPREAD_DISABLE` | Hard cutoff where spread is considered too wide for momentum | `0.05` |
| `MOMENTUM_TIMEOUT_SECONDS` | Max time an unhedged leg can remain open before momentum auto-disables | `15` |
| `MOMENTUM_MAX_POSITIONS` | Max simultaneous unmatched legs | `20` |
| `COMBINED_GUARD_WINDOW` | Rolling window (ticks) for spread volatility | `40` |
| `COMBINED_GUARD_STD_MULT` | Std-dev multiplier applied to combined cap | `2.0` |
| `FEE_BUFFER_EXTRA` | Extra cushion applied to combo cap | `0.0`–`0.02` |
| `MAX_UNHEDGED_EXPOSURE` | Absolute YES/NO delta before momentum halts | `1000` |
| `MIN_PROBABILITY_FLOOR` | Warn/log when pair bids drop below this price (still signs safety bids) | `0.02` |
| `LOSS_LEASH` | If realized PnL drops below this, bot switches to PASSIVE | `-50` |
| `TRADING_MODE` | `ACTIVE` (pair + momentum) or `PASSIVE` (pair-only) | `ACTIVE` |
| `TRADE_LOG_PATH` | Where fills & PnL snapshots are written | `data/trade_log.json` |

## Operating Modes

- **PAIR** – default behavior: only fire when YES+NO <= adaptive cap.
- **MOMENTUM** – allowed when `ENABLE_MOMENTUM=true`, `TRADING_MODE=ACTIVE`, exposure is within limits, and the momentum thresholds derived from shadow data are met.
- The bot automatically drops back to PASSIVE if:
  - `MAX_UNHEDGED_EXPOSURE` is exceeded.
  - An unmatched leg survives longer than `MOMENTUM_TIMEOUT_SECONDS`.
  - `realized_pnl <= LOSS_LEASH`.

## Suggested Process

1. Run `shadow_listener.py` in parallel with the bot for a day.
2. Plot the logged prices to find the percentiles where aggressive actors fire.
3. Set `MOMENTUM_NO_MIN_PRICE` / `MOMENTUM_YES_MAX_PRICE` (or the alias `MOMENTUM_BUY_LOW_THRESHOLD(_NO)`) to the 70–80th percentile levels taken from the shadow-mode heatmap.
4. Keep `MOMENTUM_SPREAD_MAX` tight (~0.01) for low-latency venues; widen only if you want more single-side churn during chaotic windows.
5. Start with `COMBINED_GUARD_STD_MULT=2`. If you see no pair fills, increase `FEE_BUFFER_EXTRA` slightly (e.g., `0.01`) or widen the std multiplier.
6. Review `data/trade_log.json` periodically. If the rolling PnL trend drops, tighten the thresholds or raise `LOSS_LEASH`.

Keeping these knobs in version control (via `.env`) lets you emulate @gabagool22’s aggressiveness while remaining one config edit away from conservative mode.

