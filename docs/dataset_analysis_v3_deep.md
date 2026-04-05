# EdgeRunner Deep Dataset Analysis — 7,365 NBA Markets, 6.5M Trades

## Dataset
- Jon-Becker prediction-market-analysis (36GB Parquet)
- 7,365 finalized NBA markets with trade data
- 6.5 million NBA trades
- December 2024 — November 2025

## ALL NBA Market Types — ROI at OPENING Price

| Type | Count | YES Hit% | Avg Open | YES ROI | NO ROI | Best Side |
|------|-------|----------|----------|---------|--------|-----------|
| TOTAL | 2,651 | 52.8% | 51c | +3.0% | -3.2% | YES |
| SPREAD | 2,584 | 32.0% | 34c | -5.8% | +3.0% | NO |
| GAME | 738 | 50.0% | 54c | -7.4% | +8.7% | NO |
| PTS | 337 | 46.3% | 48c | -4.2% | +4.0% | NO |
| REB | 256 | 51.2% | 51c | -0.5% | +0.6% | ~neutral |
| AST | 200 | 56.0% | 54c | +4.7% | -5.4% | YES |

## CRITICAL: Maker vs Taker on NBA

| Taker Side | Trades | Taker Win % | Maker Win % |
|------------|--------|-------------|-------------|
| YES | 365,259 (73%) | 31.5% | 68.5% |
| NO | 134,741 (27%) | 56.9% | 43.1% |

**KEY FINDING: 73% of NBA takers buy YES and win only 31.5% of the time.**
This is the structural edge — retail optimism bias on Kalshi NBA.

## Time of Day Analysis

| Period | Taker Win % | Interpretation |
|--------|-------------|----------------|
| Morning (5-14 UTC) | 33-37% | Worst — stale lines, smart money picks off retail |
| Game time (17-18 UTC) | 43-44% | Best — more information, tighter spreads |
| Late night (21-23 UTC) | 35-39% | Poor again — thin liquidity |

## Actionable Strategy Conclusions

1. **GAME winners: NO at opening is the best single strategy (+8.7% ROI)**
   - But this is an OPENING price. During games, prices move.
   - The edge might be in being a MAKER (posting limit orders) not a taker.

2. **The real edge is being on the MAKER side of YES trades**
   - 68.5% win rate when providing liquidity to YES buyers
   - This means: POST limit orders on the NO side, wait for retail to cross

3. **Avoid PTS props as a taker** — only +4% ROI, fees likely eat this

4. **AST props are the exception** — YES takers actually profit (+4.7%)

5. **Trade during game time (5-6 PM ET)** — taker win rate is highest then

6. **The market is more efficient than we assumed** — most edges are 3-8% ROI,
   which gets eaten by Kalshi's ~2-4% fee drag

## Implications for EdgeRunner

The data suggests our agent should:
- Consider being a MAKER (liquidity provider) not a TAKER
- If staying as taker: only trade with >10% net edge after fees
- Focus on GAME winners (NO side) and AST props (YES side)
- Avoid PTS and REB props (razor-thin edge)
- Trade during game hours, not pre-game
- The 5-gate risk system we built is critical — prevents fee-negative trades
