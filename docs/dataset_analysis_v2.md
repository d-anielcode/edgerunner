# EdgeRunner Dataset Analysis v2 — Real Entry Prices from 6.5M Trades

## Dataset
- Source: Jon-Becker prediction-market-analysis (36GB)
- Kalshi data: 7.68M markets, 6.5M NBA trades
- Date range: December 2024 — November 2025
- Entry price: FIRST TRADE on each market (opening price proxy)

## Player Points Props (337 settled with trade data)

| Opening YES Price | Count | YES Hit % | NO Win % | NO ROI |
|---|---|---|---|---|
| 1-20c (longshots) | 97 | 2.1% | 97.9% | +1.9% |
| 21-40c | 31 | 41.9% | 58.1% | -15.5% |
| 41-60c | 90 | 51.1% | 48.9% | -0.9% |
| 61-80c | 38 | 57.9% | 42.1% | +35.2% |
| 81-99c | 81 | 90.1% | 9.9% | +138.8% |

Overall: 53.7% NO win rate, +4.0% ROI buying NO at opening

## Game Winners (738 settled)
- YES hit: 50.0% (perfectly efficient)
- YES ROI at opening: -7.4%
- NO ROI at opening: +8.7%

## Key Insights
1. Blind "buy NO" is barely profitable (+4%) — fees likely eat this
2. The 21-40c YES bucket LOSES money buying NO (-15.5%)
3. The 61-80c YES bucket has real edge but on the YES side (+35.2%)
4. Game winners are efficiently priced (50/50)
5. The market is more efficient than our earlier flawed analysis suggested
6. Sample size is small (337 PTS props) — need deeper analysis

## Questions for Deeper Analysis
- What about other prop types (rebounds, assists)?
- Does the edge change by time of day?
- Does the maker/taker dynamic apply to NBA specifically?
- What about in-game vs pre-game trades?
- How does volume affect profitability?
- Can we identify specific prop line thresholds (20+, 25+, 30+)?
