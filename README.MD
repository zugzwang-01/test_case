# Project README — Task 1 & Task 2

## 📌 Overview
This project contains two related research and development tasks focused on analyzing and backtesting trading strategies using cryptocurrency **funding rate** data.

---

## **Task 2 — Exploratory Data Analysis (EDA) & Pattern Discovery**
**Goal:**  
Perform in‑depth exploratory analysis of funding rate behaviour to uncover patterns, anomalies, and potential trading signals.

**Key Steps:**
- Aggregate and visualize funding rates by:
  - Hour of day
  - Day of week
  - Symbol and exchange
- Identify statistical properties:
  - Distribution, volatility, and outliers
  - Seasonal or cyclical patterns
- Use heatmaps and time‑series plots to highlight:
  - Periods of extreme funding
  - Exchange‑specific differences
- Generate insights to guide strategy design and parameter tuning.

---

## **Task 1 — Funding Rate Strategy Backtest**
**Goal:**  
Explore and evaluate different trading strategies based on perpetual swap funding rates to identify profitable patterns.

**Key Steps:**
- Load and clean funding rate data from multiple exchanges.
- Engineer rolling statistics (mean, std, Z‑score).
- Implement and backtest multiple strategies:
  - **Mean Reversion** — fade extreme funding rates before settlement.
  - **Momentum** — go with extreme funding rates before settlement.
  - **Cross‑Exchange Spread** — trade convergence between exchanges.
- Evaluate performance using metrics such as total return, Sharpe ratio, and win rate.
- Visualize results with equity curves and performance summaries.