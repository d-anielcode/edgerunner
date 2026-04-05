# EdgeRunner P&L Analysis — April 4, 2026

## Raw Data (from Kalshi API)
- Total deposited: ~$50
- Cash balance: $0.12
- Open positions: 23 (mostly from DET-PHI game, likely to settle soon)
- Total settled positions: 37 with cost > 0

## Win Rate by Market Type

| Market Type | Wins | Losses | Win Rate | Status |
|-------------|------|--------|----------|--------|
| Game Winners | 0 | 4 | 0% | TERRIBLE |
| Spreads | 0 | 2 | 0% | TERRIBLE |
| Player Props (PTS) | 2 | 6 | 25% | BAD |
| Overall | 12 | 37 | 24% | NOT PROFITABLE |

## Key Conclusions

1. **The agent is NOT profitable.** 24% win rate needs to be 50%+ to break even.
2. **Game winners are the worst** — 0 for 4. Claude's game outcome predictions are wrong every time.
3. **Player props are slightly better** but still losing — 25% is not enough.
4. **Night 1 profit ($30 → $50) was luck** — SAC came back in a game NOP was favored to win.
5. **Night 2 was a disaster** — bugs (restarts wiping peaks, trading dead games, opposite-side netting) plus bad predictions.

## Root Causes

### Why game winners lose:
- Game winner markets are EFFICIENTLY PRICED — thousands of participants set these lines
- Claude has no edge over the aggregate market on game outcomes
- The research confirms: LLMs do NOT beat prediction markets on game outcomes

### Why player props lose:
- Claude doesn't have the player's current in-game stats
- Without live data, Claude guesses based on season averages vs the line
- Season averages are ALREADY priced into the market — no edge
- Adverse selection: limit orders fill when you're wrong

### Why the agent overtrades:
- Too many "edges" found at low thresholds (was 3%, then 6%, now 10%)
- Claude is overconfident — assigns 65% when real probability is 50%
- No calibration tracking — we don't know HOW wrong Claude is

## What the Research Says

From our deep research:
- Average Kalshi contract return: MINUS 20%
- Takers lose 32% on average
- 92.4% of Polymarket wallets lose money
- No published evidence of LLM trading profitability on Kalshi
- Profitable bots use LATENCY ARBITRAGE, not prediction
- Calibration matters more than accuracy (+34% ROI vs -35% ROI)

## What Needs to Change

1. **STOP trading game winners** — no edge, efficiently priced
2. **STOP trading without calibration** — track predictions vs outcomes
3. **Backtest before live trading** — use Kalshi historical data
4. **Consider: is the LLM the right approach at all?** — maybe a statistical model would be better
5. **If we continue: only trade player props where we have a specific data advantage**
