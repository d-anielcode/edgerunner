# EdgeRunner Session Handoff — April 4-5, 2026

## CURRENT STATE

### What Was Just Built: EdgeRunner v2 (Rules-Based Engine)
The agent was rebuilt from an LLM-based trader (Claude Haiku) to a **rules-based engine** that requires zero API calls. This was driven by data analysis showing the LLM approach had a 24% win rate.

### The Proven Strategy
**Buy NO on NBA game winners where YES is priced above 60 cents.**
- Backtested on 738 settled game winner markets from real Kalshi data
- +33% ROI over 177 qualifying trades
- 33% win rate with 3:1 payout ratio (risk ~$0.25, win ~$0.73)
- Max drawdown: 9.9%
- After Kalshi fees: +22% ROI (61-75c bucket), +34% ROI (76-95c bucket)

### What Needs to Happen Next
1. **Run the backtest**: `python tests/backtest_v2.py` — starting $100, daily compounding
2. **Analyze results** and decide if strategy is ready for live trading
3. **If profitable**: run agent with `python main.py` during NBA games

## KEY FILES

### Strategy & Analysis
- `signals/rules.py` — **THE NEW BRAIN** — rules-based evaluator, replaces Claude
- `docs/final_strategy_recommendation.md` — complete strategy with data
- `docs/dataset_analysis_v4_findings.md` — 5 exploitable edges from 6.5M trades
- `docs/pnl_analysis_apr4.md` — honest P&L showing 24% win rate with old LLM approach
- `RESEARCH_FIRST.md` — mandate to research before coding

### Core Architecture
- `main.py` — orchestrator, now uses RulesEvaluator instead of Claude
- `execution/risk_gates.py` — 5-gate risk system (drawdown, edge, liquidity, concentration, positions)
- `execution/decision_log.py` — logs ALL decisions to Supabase `decisions` table
- `execution/brier_tracker.py` — tracks prediction accuracy per category
- `execution/position_monitor.py` — trailing stops (quarter-aware for props via ESPN)
- `execution/kalshi_client.py` — Kalshi REST API with RSA-PSS auth
- `execution/order_manager.py` — order execution with fee-adjusted Kelly sizing
- `execution/arbitrage.py` — scans for YES+NO < $1.00 arbitrage

### Data Sources
- `data/espn_scores.py` — live game scores, quarter, clock (FREE, no auth)
- `data/espn_standings.py` — team records, standings (FREE, no auth)
- `data/market_poller.py` — Kalshi orderbook polling via REST
- `data/feeds.py` — Kalshi WebSocket (connected but demo had limited data)
- `data/smart_money.py` — Polymarket top trader positions
- `data/cache.py` — in-memory state with OFI calculation
- `data/peak_cache.py` — persists trailing stop peak prices across restarts

### Dataset (36GB Jon-Becker)
- Location: `data/dataset/data/data/kalshi/` (Parquet files)
- 7.68M total markets, 6.5M NBA trades
- Dec 2024 — Nov 2025
- Queryable with DuckDB: `pip install duckdb`
- Analysis scripts: `tests/deep_analysis.py`

## CRITICAL FINDINGS FROM DATA

### From 6.5M NBA Trades:
1. **Makers win 68.5%** when retail takers buy YES on NBA
2. **Game winner NO on favorites (61-99c YES)**: +22-40% ROI after fees
3. **Low volume markets (<1K)**: 73% taker win rate
4. **Price momentum**: 58.8% win rate when buying after price drops
5. **Player prop spreads are 55c wide** — unprofitable for takers
6. **Off-hours trading**: 61% win rate at 6 UTC vs 40% at peak hours

### From Our Live Trading (2 sessions):
- LLM agent: 24% win rate, lost ~$10
- Game winners: 0/4 (Claude's predictions were wrong every time)
- Player props: 2/8 (25% win rate)
- Night 1 NOP-SAC trade WAS profitable ($30→$50) but partially lucky
- Most losses from: bugs (restart wiping peaks), dead market trades, Claude hallucinations

## BUGS FIXED (Critical for Future Sessions)
1. Peak prices persist to disk (`data/peak_prices.json`) — survives restarts
2. No trades when game < 2 min left in Q4 (ESPN clock check)
3. No opposite-side trades on same ticker (prevents Kalshi auto-netting)
4. No trades without player data (rationale scanner blocks)
5. Quarter-aware trailing stops for props (Q1=wide, Q4=tight)
6. Resting orders cancel after 30 seconds
7. 2-phase market poller (cache all, then push to queue)
8. Fee-adjusted edge calculation in risk gates
9. OT handling (don't block trades just because Q4 ended)

## CONFIGURATION (.env)
```
TRADING_MODE=live
FRACTIONAL_KELLY=0.35
MAX_POSITION_PCT=0.15
MAX_CONCURRENT_POSITIONS=10
MIN_EDGE_THRESHOLD=0.10
MAX_SPREAD_CENTS=0.03
```

## ACCOUNTS
- Kalshi: Production keys in `.env`, RSA key at `keys/prod_private_key.pem`
- Supabase: EdgeRunner project (tables: markets, trades, positions, daily_pnl, brier_scores, decisions)
- Discord: Webhook configured for trade alerts
- Anthropic: API key in `.env` (but v2 doesn't use Claude for trading)

## WHAT PRIZM CAN PROVIDE (Separate Project)
- Path: `C:/Users/dcho0/nbaiqproject`
- Supabase with 18.5K player game logs, season stats, team defense stats
- Confidence model v6.2 with 11 weighted factors
- Different Supabase URL: `https://shvoyqofsbtnzwokuutt.supabase.co`
- NOT yet integrated into EdgeRunner

## FUTURE PLANS
1. Weather markets (year-round, Open-Meteo ensemble API)
2. MLB (April-October, reuses NBA architecture)
3. Crypto hourly (24/7, macro news only)
4. Prizm data integration (real NBA stats from own Supabase)
