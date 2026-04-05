# EdgeRunner Data Limitations — What We Can't Do

## What We Tried
Pulled 5,100 settled NBA PTS props from Kalshi API to backtest strategies.

## What We Found
The data is NOT sufficient for proper backtesting:

1. **No opening prices** — `previous_price_dollars` is $0.00 (not populated)
2. **No trade history** — historical trades endpoint returns empty for settled props
3. **No candlesticks** — 404 for settled market candlesticks
4. **Last price is near settlement** — useless for simulating entry points
5. **Only ~25 days of data** (March 11 - April 4, 2026)

## What This Means
- We CANNOT simulate realistic entry/exit prices
- We CANNOT verify if a strategy would have been profitable at actual entry prices
- The YES rate analysis (35.3% for PTS props) is DIRECTIONALLY correct but the profitability simulation is unreliable because it uses settlement prices, not entry prices
- Any strategy conclusions drawn from this data are UNRELIABLE for profit estimation

## What We Actually Know (with confidence)
1. YES/OVER on PTS props hits ~35% historically on Kalshi (directional — more props resolve NO)
2. Higher prop lines (25+, 30+) resolve YES less often than lower lines (10+, 15+)
3. We CANNOT say what the ROI would be because we don't know the entry prices

## What We Need
- Real-time data collection going forward (log opening prices, track through game)
- Build our own historical database by recording prices as markets open
- Paper trade to collect realistic entry/exit data
- At minimum 2-4 weeks of this data before drawing profit conclusions

## Honest Assessment
We don't have enough data to confidently say ANY specific strategy is profitable on Kalshi.
The only way to find out is to paper trade and collect data ourselves.
