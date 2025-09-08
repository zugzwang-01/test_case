# **Task 1 — Funding Rate Strategy Backtest**

## Overview
Backtested three trading strategies on cryptocurrency perpetual swap **funding rate** data from Hyperliquid and OKEX to identify profitable patterns.

## Strategies
- **Mean Reversion**: Fade extreme funding rates before settlement (Z-score > threshold → short, < -threshold → long).
- **Momentum**: Go with extreme funding rates before settlement (Z-score > threshold → long, < -threshold → short).
- **Cross-Exchange Spread**: Trade convergence between the same symbol’s funding rates on both exchanges.

## Method
- Cleaned and filtered data (`min_points` per symbol).
- Calculated rolling mean/std over 48h to compute Z-scores.
- Applied settlement-hour filter (0, 8, 16 UTC).
- Evaluated performance via trades, total return, avg return, Sharpe ratio, win rate.

## Results
- **Mean Reversion**: Consistently negative — market not mean-reverting in this dataset.
- **Momentum**: Positive for some symbols — extremes often persist.
- **Cross-Spread**: Few trades — needs lower thresholds and relaxed filters.

## Key Insight
Momentum aligns best with current market behaviour; cross-spread has potential with tuning.