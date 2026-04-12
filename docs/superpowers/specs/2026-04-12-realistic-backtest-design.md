# Realistic Agent Simulation Backtest

## Goal

Build a full-fidelity historical simulation of the current EdgeRunner agent against the TrevorJS dataset (154M+ Kalshi trades, 2024-2025). Compare two pricing models (first-trade vs 2h-pre-close) to quantify the execution gap. Include BMA Bayesian updating and CUSUM detection running live during the simulation.

## Why

The existing backtests use first-trade prices, which overstate edge by 5-30c per Gemini research. We need to know what the **current** agent — with all fixes from today (raised min_edge, removed trailing stops, BMA engine, prior capping, unitized risk, etc.) — would actually return in a realistic execution environment.

## Data Source

TrevorJS dataset in `data/trevorjs/`:
- `trades-*.parquet`: 154M+ trade prints (ticker, price, timestamp, side, count)
- `markets-*.parquet`: market metadata (ticker, result, close_time, event_ticker, title)

Loaded via DuckDB (proven pattern from existing backtests).

## Architecture

Single Python script: `tests/backtest_agent_sim.py`

### Phase 1: Data Preparation (DuckDB)

```sql
-- For each market, extract:
-- 1. First trade price (legacy comparison)
-- 2. Last trade 2h before close (realistic entry)
-- 3. Settlement result (yes/no)
-- 4. Sport (from ticker pattern)
-- 5. Close time (for seasonal logic)
```

Join trades to markets on ticker. For each settled market with result in ('yes', 'no'):
- `first_trade_price`: MIN(timestamp) trade's yes_price
- `pregame_price`: last trade where `timestamp < close_time - 2 hours`
- Compute NO price = 100 - YES price (in cents)

### Phase 2: Agent Simulation Loop

Process markets chronologically (by close_time). For each market:

1. **Sport identification** — extract sport from ticker using `get_sport()` logic
2. **Price selection** — use first-trade OR pregame price (run both)
3. **Edge table lookup** — current EDGE_TABLES from rules.py
4. **Fee-aware edge calculation**:
   ```
   no_price = (100 - yes_price) / 100
   fee = ceil(0.07 * 1 * yes_price/100 * no_price) / 100  # Per-contract fee
   actual_yes_rate = edge_table_lookup(sport, yes_price)
   edge = (1 - actual_yes_rate) - no_price - fee
   ```
5. **Min edge filter** — reject if edge < sport's min_edge
6. **Spread penalty** — subtract 1.5c from NO price (half-spread estimate)
7. **Seasonal filters**:
   - April 0.5x for NBA/NHL only
   - NBA playoff veto after April 19
   - NHL playoff veto after April 16
8. **Position limits** — MAX_PER_GAME=2, max 25 concurrent positions
9. **Kelly sizing**:
   ```
   kelly = sport_kelly_mult * 0.33 * bankroll
   bet = min(kelly, MAX_BET, bankroll * max_position)
   contracts = bet / no_price
   ```
10. **Drawdown circuit breakers** — unitized NAV with tiered Kelly (15/25/40%)
11. **Execute** — deduct cost + fee from bankroll, add to open positions

### Phase 3: Settlement & Bayesian Updates

When a market settles (processing in chronological order):
- If result == "no": add $1 * contracts to bankroll (WIN)
- If result == "yes": position lost (bankroll already deducted)
- Apply profit-take if tracked (check if max intra-game price hit threshold)
- **Update BMA Bayesian engine**: call update logic with sport, yes_price, result
- **Update CUSUM**: track per-sport cumulative evidence
- **Update NAV**: recompute unitized drawdown

### Phase 4: Profit-Take Simulation

For markets where we hold a position, check if the profit-take threshold was hit during the game by scanning trade prices between entry and settlement:
- Extract all trades for this ticker between entry_time and close_time
- Find max NO price reached during that window
- If max_no_price >= entry_no_price * (1 + sport_profit_take_pct):
  - Exit at the profit-take price instead of settlement
  - Deduct exit fee (Taker fee on sell side)

### Phase 5: Output

```
=== BACKTEST RESULTS (2024-01 to 2025-12) ===

                FIRST-TRADE     PREGAME (2h)
Starting:       $100.00         $100.00
Final:          $XXX.XX         $XXX.XX
Return:         +XX.X%          +XX.X%
Max Drawdown:   XX.X%           XX.X%
Sharpe:         X.XXX           X.XXX
Win Rate:       XX.X%           XX.X%
Total Trades:   XXXX            XXXX
Trades/Month:   XX              XX

Per-Sport Breakdown:
  NBA:    $XX.XX  (XX trades, XX.X% WR)
  NHL:    $XX.XX  (XX trades, XX.X% WR)
  EPL:    $XX.XX  (XX trades, XX.X% WR)
  ...

BMA Weight History: (slow vs fast filter dominance over time)
CUSUM Alarms: (which sports, when)
Drawdown Chart: (equity curve data points)
```

## Key Realism Features

| Feature | Old Backtests | This Simulation |
|---------|--------------|-----------------|
| Entry price | First trade (days before game) | Last trade 2h before close |
| Spread cost | None | 1.5c penalty per entry |
| Fees | Continuous (no rounding) | ceil() rounding on per-contract basis |
| Position sizing | Fixed or simple Kelly | Full Kelly with bankroll dynamics |
| Position limits | None | MAX_PER_GAME=2, max 25 concurrent |
| Drawdown | None | Unitized NAV with tiered circuit breakers |
| Bayesian | Static edge tables | BMA dual-state updating live during sim |
| CUSUM | None | Per-sport regime shift detection |
| Seasonal | Some | April debuff (NBA/NHL only), playoff veto |
| Profit-take | Some backtests | Sport-specific thresholds with exit fees |
| Trailing stop | 25% (old) | Removed (per research) |

## Files

| File | Description |
|------|-------------|
| `tests/backtest_agent_sim.py` | **New** — full agent simulation script |

## Running

```bash
cd /path/to/edgerunner
.venv/Scripts/python tests/backtest_agent_sim.py
```

Expected runtime: 5-15 minutes (DuckDB handles the heavy data lifting, Python iterates ~3-5K tradeable markets).

## Success Criteria

1. Produces comparable results for both pricing models
2. BMA weights shift appropriately during the simulation period
3. CUSUM fires alarms for sports where edge decayed historically
4. Output includes per-sport P&L breakdown, equity curve, and Sharpe ratio
5. No sport shows positive return with pregame pricing that showed negative with first-trade (sanity check)
