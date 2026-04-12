# EdgeRunner Research Findings

Comprehensive compilation of all research conducted during the EdgeRunner development and optimization session (April 2-11, 2026). Includes findings from Gemini Deep Research, empirical backtesting, out-of-sample validation, and Claude analysis.

---

## Table of Contents

1. [Favorite-Longshot Bias Research](#1-favorite-longshot-bias-research)
2. [Out-of-Sample Validation Results](#2-out-of-sample-validation-results)
3. [NBA Seasonal Analysis](#3-nba-seasonal-analysis)
4. [Per-Market Drawdown Analysis](#4-per-market-drawdown-analysis)
5. [WNBA and NBA2D Recalibration](#5-wnba-and-nba2d-recalibration)
6. [Exit Strategy Research](#6-exit-strategy-research)
7. [Kelly Criterion Research](#7-kelly-criterion-research)
8. [Market Microstructure](#8-market-microstructure)
9. [Bayesian Updating](#9-bayesian-updating)
10. [Agent Architecture Audit](#10-agent-architecture-audit)
11. [Backtest Methodology Issues](#11-backtest-methodology-issues)

---

## 1. Favorite-Longshot Bias Research

### What We Researched

Whether the Favorite-Longshot Bias (FLB) on Kalshi prediction markets is persistent, exploitable, and durable enough to sustain an automated trading strategy.

### Key Findings

**The FLB is real, academically validated, and deeply embedded in prediction market microstructure.**

- Contracts priced at 5c win only 4.18% of the time (implied mispricing of -16.36%), while contracts at 95c win 95.83% of the time (Becker, 72.1M Kalshi trades analysis).
- The bias is driven by the Maker-Taker microstructure: retail traders with extreme beliefs self-select as "Takers," impulsively crossing the bid-ask spread to buy longshot YES contracts. "Makers" supply this liquidity, extracting an "optimism tax."
- Calibration errors explain 87.3% of pricing variance when decomposed into horizon effects, domain biases, domain-by-horizon interactions, and trade-size effects (Le 2026, 292M trades across Kalshi and Polymarket).

**Sportsbook validation (17 years of NBA data, 19,820 games):**

| Price Range | Games | Fav Win% | NO Win% | NO ROI |
|-------------|-------|----------|---------|--------|
| 55-59c | 2,907 | 54.5% | 45.5% | +5.3% |
| 60-64c | 3,325 | 58.6% | 41.4% | +8.4% |
| 65-69c | 2,439 | 65.1% | 34.9% | +5.5% |
| 70-74c | 2,511 | 69.8% | 30.2% | +6.7% |
| 75-79c | 2,345 | 73.5% | 26.5% | +13.5% |
| 85-89c | 1,409 | 83.5% | 16.5% | +24.1% |
| 90-94c | 1,237 | 88.7% | 11.3% | +36.0% |
| 95-99c | 514 | 94.6% | 5.4% | +44.4% |

**Critical finding: Kalshi FLB is STRONGER than sportsbook FLB.** At 70c, sportsbook favorites win 69.8% (nearly fair), while Kalshi favorites win only 54-61%. The peer-to-peer CLOB structure amplifies the bias vs professional bookmakers.

**However: sportsbook data CANNOT validate Kalshi edge tables.** The markets are fundamentally different. When the same strategy was applied to sportsbook odds, it produced only 2/11 winning seasons (18%) for NBA and 1/11 (9%) for NHL. The edge is platform-specific.

### FLB Decay Timeline

- Woodland & Woodland found a Reverse FLB in NHL that was profitable for 3 seasons, then completely disappeared over the subsequent 7 seasons.
- Kalshi's maturation timeline is compressed due to API access and institutional liquidity.
- By late 2025/early 2026, SIG, Jump Trading, and Tradeweb had integrated heavily into Kalshi.
- SIG became a designated market maker for Kalshi with a dedicated trading department.
- Jump Trading began aggressively making markets.
- Tradeweb announced a strategic partnership in February 2026 for institutional access to Kalshi order books.

| Market Regime | Dominant Participants | FLB Status |
|--------------|----------------------|------------|
| 2023-2024 | Retail traders, hobbyist MMs | Highly pronounced |
| 2025 | Retail influx, early institutional pilots | Compressing |
| 2026 (Current) | SIG, Jump Trading, Tradeweb institutional clients | Arbitraged in high-liquidity markets (NBA, NFL) |

### Academic Citations (from Gemini Deep Research)

- Burgi, Deng & Whelan (2026): "Makers and Takers: The Economics of the Kalshi Prediction Market" -- 300,000+ contracts analyzed, confirmed FLB driven by Maker-Taker microstructure
- Le (2026): 292M trades across Kalshi and Polymarket, decomposed calibration into 4 components explaining 87.3% of variance
- Becker: 72.1M Kalshi trades, quantified FLB precisely across price levels
- Arora & Malpani (2026): PredictionMarketBench framework for standardized backtesting
- Ottaviani & Sorensen (2015): Biases among risk-averse bettors with bounded wealth
- Woodland & Woodland: NHL Reverse FLB decay over 10 seasons

### Changes Made

- Strategy validated as academically sound for fade-the-favorite on Kalshi
- Institutional MM entry recognized as permanent structural shift for high-liquidity markets
- Focus shifted to lower-volume markets (Weather, NCAAMB, NFLTD) where institutional presence is minimal

### Unvalidated

- Whether institutional MM presence will expand to currently-inefficient markets (Weather, college basketball)
- Exact timeline for further FLB compression on remaining markets

---

## 2. Out-of-Sample Validation Results

### What We Researched

Validated 2025-calibrated edge tables against unseen 2026 data using two approaches: (1) January 2026 TrevorJS dataset, (2) Feb-Apr 2026 Kalshi API data.

### January 2026 TrevorJS Validation (Reliable)

| Sport | Jan 2026 Result | Predicted YES Rate | Actual YES Rate | Verdict |
|-------|----------------|-------------------|-----------------|---------|
| **NHL** | Edge BIGGER than predicted | ~53% | Better | Confirmed |
| **NFLTD** | Edge BIGGER than predicted | 45-49% | 26-40% | Confirmed |
| **Weather** | Edge BIGGER than predicted | ~40% | 19-29% | Confirmed |
| **NCAAMB** | Confirmed within 1% | Model prediction | Matched | Confirmed |
| **NBA** | Edge DECAYED | 49% YES predicted | 65% YES actual | Decayed |
| **NBASPREAD** | Edge DECAYED | -- | -- | Decayed |
| **NFLSPREAD** | Edge DECAYED badly | -- | -- | Decayed |
| **NHLFG** | Edge DECAYED | -- | -- | Decayed |

### Feb-Apr 2026 Kalshi API Validation (Unreliable)

This validation was deemed unreliable due to a fundamental methodology flaw: the Kalshi API returns the "first trade price" for each market, but first trades often occur 1-2 days before the game when the market opens. These opening prices differ significantly from pre-game prices.

Example: PHI-HOU game had PHI first trade at 60c on Apr 8, but the game was Apr 9. The `previous_price` was 38c -- the actual pre-game price.

**Bottom line:** The Feb-Apr validation data used opening-day prices instead of pre-game prices. The January TrevorJS validation (with proper first-trade methodology) is the only reliable OOS data.

### Changes Made Based on OOS Validation

| Sport | Action | min_edge |
|-------|--------|----------|
| NHL | Boosted Kelly 0.30 to 0.35 | 0.05 |
| NFLTD | Boosted Kelly 0.20 to 0.25 | 0.05 |
| Weather | Boosted Kelly 0.25 to 0.30, min_edge 0.10 to 0.08 | 0.08 |
| NCAAMB | Unchanged | 0.08 |
| NBA | Cut Kelly 0.10 to 0.06, min_edge 0.08 to 0.12 | 0.12 |
| NBASPREAD | Cut Kelly 0.12 to 0.08, min_edge 0.05 to 0.10 | 0.10 |
| NFLSPREAD | Cut Kelly 0.12 to 0.06, min_edge 0.05 to 0.12 | 0.12 |
| NHLFG | Dropped from discovery | Disabled |

**Backtest impact:** Max drawdown dropped from 29.4% to 23.0% with nearly identical profit ($315K to $314K).

### Unvalidated

- Whether "BETTER THAN EXPECTED" verdicts for Weather/NHLSPREAD hold going forward (Feb-Apr data unreliable)
- Q2 2026 edge persistence for any sport

---

## 3. NBA Seasonal Analysis

### What We Researched

Whether the NBA edge decline was seasonal (April/playoffs) or a broader market efficiency trend.

### Key Findings

**The NBA edge decay is NOT seasonal -- it is market efficiency improving mid-season.**

Analysis of NBA monthly performance on Kalshi showed the edge was strongest early season and weakened progressively, not just in April/playoffs:

- NBA early season (Oct-Dec): ~24% ROI
- NBA mid-season (Jan): ~21% ROI
- NBA late season: Decayed further

This pattern is inconsistent with pure seasonality (which would show a sharp drop only during playoffs). Instead, it matches a market that gets more efficient as the season progresses and institutional MMs refine their models.

**Sportsbook historical data (10 years) showed April is fine:**
- NBA April (sportsbooks): +8.5% ROI on 1,137 games -- POSITIVE
- NBA May (playoffs): -1.4% ROI -- barely negative

Our Kalshi April loss (-71%) was likely compounded by the Kelly override bug and edge decay, not purely a seasonal structural problem.

**NHL shows a similar pattern:**
- Early season: +36.5% ROI (strongest -- casual bettors most active)
- Mid-season: +16.4% ROI (edge weakens as market adjusts)

### Changes Made

- NBA/NBASPREAD: Raised min_edge to only take trades with large buffer (marginal trades now filtered)
- NFLSPREAD: Raised min_edge significantly (worst decay)
- NHLFG: Dropped entirely (decayed despite positive backtest)
- NHL, NFLTD, Weather: Slightly more aggressive (confirmed edge)
- Full NBA playoff veto added (April 19 - September)

### Unvalidated

- Whether early-season NBA edge returns in October 2026 or if the institutional MM presence is permanent
- Whether adding more Kalshi seasons of data would reveal true seasonal patterns

---

## 4. Per-Market Drawdown Analysis

### What We Researched

Risk-adjusted performance of all 15 backtest-profitable markets, including Sharpe ratios, max consecutive losses, and per-price bucket analysis.

### Key Findings

**Tier 1 -- Smoothest edge (keep as-is):**

| Market | Sharpe | ROI | Max Drawdown |
|--------|--------|-----|-------------|
| NHLFG | 0.79 | +500% | 0.1% |
| NFLTD | 0.66 | +119% | 0.3% |
| NHLSPREAD | 0.48 | +91% | 0.3% |

**Tier 2 -- Solid but noisier:**
- EPL, Weather, UFC, La Liga, WNBA -- all positive with manageable drawdowns

**Tier 3 -- Profitable overall but with losing buckets inside:**

| Market | Problem Bucket | Bucket ROI | Max Consecutive Losses |
|--------|---------------|-----------|----------------------|
| NCAAMB | 61-65c | -3% | -- |
| NCAAMB | 91-95c | -- | 44 |
| NCAAWB | 86-90c | -6% | 33 |
| UCL | 55-65c, 71-75c | All negative | -- |
| NHL | 81-85c | -8.7% | -- |

### Changes Made

| Sport | Change | Why |
|-------|--------|-----|
| NCAAMB | Dropped 61-65c bucket | -3% ROI, 260 trades (conclusive) |
| NCAAWB | Dropped 86-90c bucket, capped at 85c | -6% ROI, MCL=33 |
| UCL | Narrowed to 66-70c + 76-85c only | 55-65c and 71-75c all losing |
| NHL | Dropped 81-85c bucket | -8.7% ROI |

**After full recalibration:**

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Final P&L | $302,945 | $311,653 | +$8,708 |
| Max DD | 30.9% | 29.7% | -1.2% |
| Win rate | 41.0% | 41.2% | +0.2% |

Every active market became profitable. Zero negative markets after recalibration.

---

## 5. WNBA and NBA2D Recalibration

### What We Researched

Per-price bucket analysis of WNBA and NBA2D markets to identify why they were losing overall despite positive underlying edges.

### WNBA Findings

**Overall edge was strong (+49% avg ROI) but bad price buckets dragged it negative (-5% before recalibration).**

- Best range: 55-65c (60-70% upset rate, massive edge)
- Danger zones: 63-67c, 66-70c, 78-82c, 88c+ (all losing buckets)
- Volume sweet spot: 100K-500K contracts (54.7% NO win rate, +94% ROI)
- Below 100K volume: skip (unreliable markets)
- Morning markets (6-11 AM) strongest

**Recalibration:**
- Narrowed to 3 sweet-spot buckets: 55-62c, 71-77c, 83-87c
- Added volume filter: skip markets below 100K volume
- Raised min_edge from 5% to 8%
- Boosted Kelly from 0.12 to 0.15 (more selective = bet more on what passes)

**Result:** WNBA went from -$62 / -5% ROI to +$3,152 / +39% ROI

### NBA2D Findings

- Profitable in the 55-79c range (all 5c buckets positive)
- 80-89c was 0% NO win rate -- deadly zone
- Only ~50 trades over entire backtest period (small impact)
- Player-level signal was huge: Brunson (95.7% NO win), D. White (100% NO win) vs Cunningham (15.4% NO win) -- but not usable in current architecture

**Decision:** NBA2D dropped entirely (-7% ROI even after recalibration, only 50 trades, minimal portfolio impact).

### Changes Made

- WNBA: 3 tight buckets, volume filter, higher min_edge, boosted Kelly
- NBA2D: Commented out from market discovery
- WTA: Also dropped (previously at -10% ROI)
- Active markets reduced from 19 to 17, then to 15 after further cuts

---

## 6. Exit Strategy Research

### What We Researched

Whether implementing a systematic sell strategy (profit-taking, stop-losses) would improve risk-adjusted returns over the current hold-to-settlement approach. Combined empirical backtest of 4,049 markets with Gemini academic research.

### Empirical Backtest Results (4,049 markets, 2025 data)

| Strategy | Total P&L | Sharpe | Win Rate | Max DD | vs Hold |
|----------|-----------|--------|----------|--------|---------|
| Hold to Settlement | +$312 | 0.171 | 31.4% | $6.67 | baseline |
| Profit-Take 50% | +$299 | 0.343 | 78.7% | $2.44 | -4% P&L, 2x Sharpe |
| Profit-Take 100% | +$344 | 0.308 | 68.9% | -- | +10% |
| **Profit-Take 200%** | **+$374** | **0.246** | **53.1%** | -- | **+20%** |
| Trailing Stop 25% | +$87 | 0.121 | 39.2% | -- | -72% (terrible) |
| Time Exit 80% | +$169 | 0.266 | 47.3% | -- | -46% |

### Gemini Academic Research Findings

**Optimal Stopping Theory:**
- The Bellman equation governs exit: sell when immediate payoff exceeds expected continuation value
- The Secretary Problem's 1/e rule doesn't directly apply because prediction markets allow continuous Bayesian updating
- British binary options research proves hold-to-settlement is sub-optimal -- a dynamic rational exercise boundary exists

**Mid-Game FLB Dynamics:**
- When underdogs take an early lead, a reverse FLB emerges -- the market exaggerates the favorite's comeback probability
- This means selling a winning NO position too early (when the favorite is losing) actually surrenders expected value
- Bounded wealth constraints prevent informed traders from fully correcting mid-game mispricing

**Capital Velocity:**
- Taking 50% profit 3 times in 3 hours: $1,000 x (1.50)^3 = $3,375
- Holding one position to 200% over 3 hours: $1,000 x 3.0 = $3,000
- Higher turnover generates +18.75% more absolute return despite lower per-trade margin
- But this is contingent on having secondary markets available for redeployment

**Double-Fee Impact:**
- Selling mid-game incurs a second Taker fee, which is maximal near the 50c midpoint
- Entry at 30c + exit at 50c: total fees of $3.22 on 100 contracts = 16.1% tax on the exit
- Hold to settlement pays only the entry fee ($1.47)
- Break-even for selling at 50c: true probability must be below 48.25% (fee-adjusted)

**Trailing Stops Destroy Edge:**
- Games are too volatile -- price swings through the stop before reverting
- Backtest confirmed: trailing stops killed 72% of hold P&L
- Gemini confirmed: "easily exploited by market noise"

### Changes Made

| Parameter | Before | After | Evidence |
|-----------|--------|-------|----------|
| Game winner profit-take | None (always hold) | Sell at 200% gain | +20% more P&L (4,049 market backtest) |
| Tail-risk stop | None (hold to $0) | Sell below $0.08 | Exploit retail FLB on lottery tickets |
| Trailing stop | 25% from peak | Disabled | Destroyed 72% of edge in backtest |
| General profit-take | 400% | 200% | Data-backed optimal threshold |
| Tail-risk timing | Immediate | Only after 1st half | Prevents premature exit on early game swings |

### Academic Citations

- Wald: Sequential analysis and optimal stopping theory
- Croxson & Reade: Prediction market efficiency during live events
- Ottaviani & Sorensen (2015): Bounded wealth and liquidity constraints in prediction markets
- British binary options literature: rational exercise boundary for early exits

### Unvalidated

- Whether the 200% profit-take threshold holds on 2026 live data
- Capital velocity gains when concurrent markets are available
- Optimal partial-exit sizing (selling 25% of position at a time)

---

## 7. Kelly Criterion Research

### What We Researched

Optimal position sizing for prediction markets with small bankrolls, discrete contract granularity, and decayed edges.

### Key Findings

**Minimum Viable Bankroll: $133**

Gemini calculated this as follows:
- Standard prediction market edge: ~3% on even-money proposition
- Full Kelly: 3% of bankroll
- Quarter Kelly (0.25x): 0.75% of bankroll
- To execute $1.00 minimum trade at 0.75% sizing: $1.00 / 0.0075 = $133.33

At $55 bankroll with 1% Kelly, bets are $0.55, which hits the $1 minimum filter and gets rejected. The agent literally cannot trade.

**Discrete Contract Granularity Problem:**
- Kelly assumes infinitely divisible capital
- With $55 bankroll and 1.2% Kelly = $0.66 recommended
- Must round to $0 (miss opportunity) or $1 (50% over-allocation)
- Systematic rounding up pushes wagers toward the 2f* ruin threshold

**The 2f* Ruin Threshold:**
- Wagering any fraction greater than 2x the optimal Kelly fraction mathematically guarantees negative geometric growth over infinite horizon
- The Kelly override bug had the agent at 30% global Kelly when sport-specific Kelly was 1-4%
- This was 10x the intended size, well above 2f*, making ruin mathematically certain

**Fractional Kelly Best Practices:**
- 0.33x Kelly sacrifices ~49% of optimal growth rate but reduces variance to 1/9th (11%)
- Professional practitioners use 0.25x-0.50x Kelly
- A 72% drawdown under 0.33x Kelly within 50 trades is "infinitesimally" probable IF the edge is real
- The occurrence of such a drawdown is mathematical proof of negative EV inputs

**Fee-Aware Kelly:**
- Original Kelly calculation did not subtract fees before computing edge
- Correct formula: effective_payout = payout - taker_fee; kelly = (b_effective * p - q) / b_effective
- Without fee adjustment, the agent systematically over-bets

### Changes Made

- Implemented fee-aware edge calculation (subtract 0.07 * P * (1-P) before min_edge check)
- Added ceil() rounding to backtest fee calculations
- Set $50 bankroll floor (halts trading when discrete contract math breaks)
- Fixed the Kelly override bug (risk.py now uses sport-specific Kelly from rules.py)
- Added $1 minimum bet filter

### Unvalidated

- Whether $133 is sufficient or if $500+ is needed for practical operation
- Monte Carlo recovery trajectories from $55 to $310 at proper sizing

---

## 8. Market Microstructure

### What We Researched

Maker vs Taker dynamics, fee structures, slippage, institutional MM behavior, and spread dynamics on Kalshi's CLOB.

### Fee Structure

**Taker fee:** `ceil(0.07 * C * P * (1-P))` -- parabolic, maximum at 50c

**Maker fee:** `ceil(0.0175 * C * P * (1-P))` -- 75% reduction from Taker

Example (2 contracts at 80c YES / 20c NO):
- Taker fee: ceil(0.07 * 2 * 0.80 * 0.20) = $0.03 (7.5% of $0.40 profit)
- Maker fee: ceil(0.0175 * 2 * 0.80 * 0.20) = $0.01 (2.5% of profit)

**Ceil() Rounding Devastation on Small Lots:**
- For institutional traders (10,000 contracts), rounding is negligible
- For retail with $310 bankroll trading 2-3 contracts, rounding adds 5-8% drag per trade
- Fee accumulator tracks cumulative overpayments but requires high volume for rebates

### Maker vs Taker Dynamics

From Burgi, Deng & Whelan (2026):
- Makers win 68.5% on NBA YES trades
- Retail traders self-select as Takers (impulsive, cross the spread)
- Makers extract an "optimism tax" from retail flow
- The spread itself is a profit center for Makers

### Institutional MM Behavior

- SIG, Jump Trading: mirror sharp offshore sportsbooks (primarily Pinnacle)
- When Pinnacle adjusts odds, Kalshi MMs pull resting orders within milliseconds
- Kalshi's LOB is essentially a derivative of Pinnacle's API for high-liquidity sports
- Retail agents attempting to trade during volatility face zero liquidity or catastrophic slippage

### Spread Dynamics

| Game Phase | Spread | Action |
|-----------|--------|--------|
| Pre-game | 1-3c | Tight, tradeable |
| In-play | 3-10c | Widened, MMs compensating for event risk |
| Terminal | Extreme | Liquidity drain, MMs pull orders |

### Maker Order Experience

Attempted transition to Maker orders to save on fees. Result:
- At $55-88 bankroll, orders were too small to attract fills
- 50% of orders went unfilled
- Bankroll tracker became inaccurate ($55 displayed vs $72 real)
- Fee savings on 1-2 contracts (fractions of a cent) didn't justify complexity

**Conclusion:** At small bankroll sizes, Taker execution is preferable for reliable fills. Maker transition requires $1,000+ bankroll where lot sizes attract counterparty interest.

### Changes Made

- Reverted to Taker pricing for reliable fills
- Added fee-aware edge filtering (subtract Taker fee before evaluating edge)
- Added ceil() rounding to backtest

---

## 9. Bayesian Updating

### What We Researched

Replacing static 2025 edge tables with a dynamic probability model that auto-adjusts as 2026 results come in.

### Beta-Binomial Conjugate Model

**The Prior (Historical Belief):**
- Initialize with Beta distribution from 2025 data
- Example: 60c favorites won 49/100 times -> Beta(alpha=49, beta=51)

**The Likelihood (New Data):**
- As 2026 games resolve, results update the model
- Example: First 10 games: 8 favorites win, 2 lose -> Binomial(8, 10)

**The Posterior (Updated Belief):**
- Beta(alpha + wins, beta + losses) = Beta(49+8, 51+2) = Beta(57, 53)
- Mean = alpha / (alpha + beta) = 57/110 = 51.8%
- Edge has narrowed from 49% to 51.8% favorites -- model adapts automatically

**Daily Decay Mechanics:**
- Apply exponential decay factor to historical alpha and beta values
- Recent results weigh more heavily than 2024 retail inefficiencies
- Allows model to "forget" dead edges (e.g., NBA where institutional MMs compressed the FLB)
- Rolling window prevents old, irrelevant data from overpowering recent trends

### Multi-Armed Bandit for Sport Allocation

- Each sport acts as a distinct "slot machine arm" with unknown, shifting probability
- UCB1 or epsilon-greedy balances exploitation (bet on profitable sports) with exploration (test if dead edges returned)
- When MMs compress NBA FLB, the MAB algorithm detects successive losses and automatically routes capital to Weather/NCAAMB

### Adaptive Kelly Sizing

- As Bayesian posterior variance increases (edge becoming uncertain), Kelly fraction automatically decreases
- Self-healing: detects instability and throttles bet sizes from 0.35x down to 0.05x before catastrophic drawdown
- Replaces the static SPORT_PARAMS approach that couldn't adapt to regime changes

### Changes Made

- Bayesian updating model designed but implementation status: ~100 lines of code, deployed to VPS
- Atomic JSON writes (temp file + rename) to prevent corruption during updates
- Capped _scored_tickers memory at 500, trimmed to 200 when exceeded

### Unvalidated

- Whether the decay rate is correctly calibrated
- Whether the Bayesian model actually improves live P&L vs the recalibrated static tables
- Optimal prior strength (how much to weight historical data vs new observations)

---

## 10. Agent Architecture Audit

### What We Researched

Systematic review of all bugs, crashes, and architecture problems discovered during development and live trading.

### Critical Bugs Found and Fixed

| Bug | Severity | Description | Fix |
|-----|----------|-------------|-----|
| **Kelly override** | CRITICAL | risk.py line 222 used global FRACTIONAL_KELLY=0.30 for ALL sports, ignoring sport-specific params in rules.py. Every trade was 10x intended size. | risk.py now reads Kelly from SPORT_PARAMS in rules.py |
| **Bankroll desync** | CRITICAL | After sell orders, sale proceeds were not added to cached bankroll immediately. Display showed $55 when actual was $72. | Sale proceeds now added to cache bankroll immediately |
| **Silent task crashes** | CRITICAL | Background asyncio tasks could crash silently, killing the market poller or position monitor without notification. | Crashed tasks now logged + Discord alert sent |
| **Duplicate exposure** | CRITICAL | Agent bet both NO-NOP and YES-SAC on the same game (identical outcome). | _extract_game_id() detects same-game tickers, blocks duplicate bets |
| **Position monitor infinite timeout** | HIGH | Claude call for stop-loss had no timeout -- could block exits indefinitely. | 5-second timeout, defaults to SELL |
| **Hardcoded volume/depth** | HIGH | volume_24h and depth hardcoded to 0/10 in risk gates, bypassing liquidity checks. | Uses real volume from market discovery + orderbook depth |
| **No persistent circuit breakers** | HIGH | Drawdown gate reset on every restart. Agent restarted daily, so it never triggered. | HWM persists in hwm_cache.json, survives restarts |
| **No pre-event check for non-ESPN sports** | HIGH | UCL, EPL, La Liga, WNBA, Weather had no live game status (only NBA/NHL via ESPN). Agent could trade mid-game. | Check Kalshi market expiration time; if within hours, assume in-progress |
| **Bayesian JSON corruption** | HIGH | Concurrent writes could corrupt the Bayesian prior JSON file. | Atomic write (temp file + rename) |
| **Memory leak: _scored_tickers** | MEDIUM | Set grew unboundedly over 24/7 operation. | Capped at 500, trimmed to 200 when exceeded |
| **Race condition on peak price** | MEDIUM | Position monitor had no lock on peak price tracking. | Added asyncio.Lock |
| **RSA key loaded from disk per request** | MEDIUM | Every API request re-read the RSA private key file. | Cached in __init__ |
| **Tail-risk exit too fast** | MEDIUM | Sold positions at $0.04 within 5 minutes of purchase (before game had time to play out). | Tail-risk exit only fires after 1st half |
| **Watchdog crash in DEBUG mode** | MEDIUM | Referenced analyzer_status (old LLM variable), crashed in debug mode. | Removed dead reference |
| **Price bounds not validated** | MEDIUM | Could send invalid prices to Kalshi API. | Added $0.01-$0.99 bounds check |
| **Same-prop opposite-side trades** | MEDIUM | Bought YES AND NO on same player prop = guaranteed loss. | Enhanced duplicate detection for prop markets |

### Architecture Improvements Deployed

| Feature | Description |
|---------|-------------|
| 6-gate risk system | Kelly, liquidity, concentration, position limit, drawdown, edge gates |
| Tiered circuit breakers | 15% DD: 50% Kelly; 25% DD: 25% Kelly; 40% DD: halt |
| Persistent HWM | High-water mark survives restarts via JSON cache |
| Fee-aware edge | Subtracts Taker fee from edge before min_edge check |
| $1 minimum bet | Prevents trades too small for Kelly math to work |
| $50 bankroll floor | Halts trading when discrete contract math breaks |
| NBA/NHL playoff veto | Blocks game winners April 19 - September |
| Dry-run mode | Full pipeline runs without placing orders (for testing) |
| Resting order duplicate prevention | Prevents placing duplicate limit orders |

---

## 11. Backtest Methodology Issues

### What We Researched

Why the backtest showed $100 to $299,668 (+29.7% max DD) while live trading produced $310 to $55 (-82% DD).

### Issues Identified

**1. First-Trade Price vs Executable Price**

The backtest uses "first trade price" (from the TrevorJS dataset) as entry price. But first trades often occur 1-2 days before the game when the market opens. By game time, the price has moved significantly. The agent cannot execute at the first-trade price because it trades pre-game, not at market open.

Example: PHI-HOU game -- PHI first trade at 60c on Apr 8, game on Apr 9. Pre-game price was 38c. Using 60c as entry in the backtest overstates the edge significantly.

**2. No ceil() Fee Rounding**

The backtest originally computed fees as continuous values (e.g., $0.0224). Kalshi rounds UP to the nearest cent ($0.03). On small lot sizes (1-3 contracts), this rounding adds 3-5% drag per trade that was not modeled.

After adding ceil() rounding with Maker rates: minor impact on aggregate backtest ($315K to $315K) but significant impact on per-trade profitability at small bankrolls.

**3. No Slippage Modeling**

The backtest assumes perfect fills at the signal price. In reality:
- Orderbooks move between poll cycles (60-second intervals)
- Crossing a 2-5c bid-ask spread immediately erodes expected value
- If perceived edge is 6% but spread costs 4% and Taker fee costs 1.75%, the trade is negative EV
- Thin markets (100K-500K volume) have wider spreads than liquid ones

**4. Discrete Contract Rounding Not Modeled**

The backtest simulates $200 max bets on a $100+ bankroll. Live agent operated at $50-300 where:
- Kelly recommends $0.66 bet but minimum contract is $1
- Systematic rounding up to whole contracts over-allocates capital
- This distortion was not captured in the backtest

**5. Static vs Dynamic Pricing**

The backtest uses a single snapshot price per market. The live agent faces a continuously moving orderbook where:
- Institutional MMs pull quotes during volatility
- By the time a signal is processed (poll -> evaluate -> order), the price may have moved
- The historical "mispricing" visible in the dataset may have been corrected by the time a real order reaches the matching engine

### Impact Assessment

| Issue | Estimated Impact | Status |
|-------|-----------------|--------|
| First-trade price bias | Overstates entry by 5-20c on many markets | Unresolved (structural to dataset) |
| No ceil() rounding | 3-5% drag per trade at small lot sizes | Fixed in backtest |
| No slippage | 2-5c per trade on thin markets | Unresolved |
| Discrete rounding | 0.5-1% over-allocation per trade | Unresolved |
| Static pricing | Unknown (varies by market) | Unresolved |

**Conclusion:** The backtest's $299K absolute return is unreliable. However, the relative ranking of which sports are profitable vs which are not is still valid. The backtest's primary value is for comparative analysis (Sport A vs Sport B), not for predicting absolute returns.

### Unvalidated

- What the "true" returns would be with realistic execution modeling
- Whether a Level 2 orderbook replay backtest (vs first-trade prices) would still show profitability
- PredictionMarketBench framework (Arora & Malpani 2026) as a potential solution for execution-realistic backtesting

---

## Appendix A: Complete List of Gemini Deep Research Reports

| # | Title | Focus | Lines in Transcript |
|---|-------|-------|-------------------|
| 1 | Autonomous AI Trading Agent Architecture for HFT Prediction Markets | Initial venue selection, Kalshi vs Polymarket | Line 77 |
| 2 | Strategic Expansion in Kalshi Prediction Markets | 12-sport expansion, edge by vertical | Line 2620 |
| 3 | Quantitative Architecture: The EdgeRunner Protocol Analysis | Risk management, Kelly optimization | Line 3180 |
| 4 | NBA Player Prop Trading: Bayesian Exit Strategies | Exit strategies for player props | Line 3193 |
| 5 | Algorithmic Strategies: Microstructure, Pricing Anomalies, Portfolio Optimization | FLB quantification, portfolio theory | Line 6099 |
| 6 | Calibration Strategies for Kalshi Exchange Dynamics (2021-2026) | Institutional MM entry, FLB decay timeline | Line 6123 |
| 7 | Structural Inefficiencies and Temporal Dynamics in Sports Prediction Markets | Seasonal patterns, monthly calendar | Line 6290 |
| 8 | FLB Persistence, Microstructure Frictions, and Playoff Seasonality | FLB decay, overfitting, April playoffs | Line 7364 |
| 9 | Critical Assessment: Microstructure, Bet Sizing, Strategy Adaptation | Kelly math, Bayesian updating, institutional decay | Line 7926 |
| 10 | Optimal Exit Strategies: Hold-to-Settlement vs Active Profit-Taking | Optimal stopping theory, capital velocity, double-fee impact | Line 8081 |

## Appendix B: Key Numbers Summary

| Metric | Value |
|--------|-------|
| Historical dataset | 154M+ Kalshi trades (TrevorJS, 2024-2025) |
| Sportsbook validation | 19,820 NBA games (17 seasons), 13,000+ NHL games |
| Backtest result (pre-fixes) | $100 to $302,945 |
| Backtest result (post-recalibration) | $100 to $315,457 |
| Backtest max drawdown | 29.7% |
| Live performance | $310 to $55 (-82% drawdown) |
| Minimum viable bankroll | $133 (Gemini calculation) |
| Active markets after recalibration | 15 (dropped WTA, NBA2D, NHLFG) |
| Best Sharpe ratio market | NHLFG: 0.79 (but decayed in OOS, dropped) |
| Best confirmed market | Weather: year-round, OOS edge BIGGER than predicted |
| Worst strategy tested | Trailing stop 25%: destroyed 72% of hold P&L |
| Best exit strategy | 200% profit-take: +20% more P&L than hold |
| Empirical exit backtest | 4,049 markets, 6 strategies tested |
| Kelly override bug impact | 10x intended position sizing on all sports |
| Institutional MMs on Kalshi | SIG, Jump Trading, Tradeweb (2025-2026) |

---

## 12. Parameter Validation Research (Gemini Report #3 — April 11, 2026)

### What Was Researched

Specific validation of 6 agent parameters: Bayesian decay rate, companion signals, minimum bet size, Multi-Armed Bandit allocation, adaptive Kelly, and pre-game price accuracy.

### Key Findings

#### 12.1 Bayesian Decay Rate (0.99 daily)

**Current implementation:** Fixed 0.99 daily decay (69-day half-life)

**Gemini verdict: SUBOPTIMAL.** Fixed decay is a rigid compromise. Academic recommendation is:
- **Bayesian Online Changepoint Detection (BOCPD)** or **CUSUM** control charts — detect discrete regime shifts rather than continuous decay
- **Dual-state Bayesian filter** — slow state (0.95 per event) tracks long-term strength, fast state (0.70 per event) tracks momentum. Ensemble the two.
- The decay should be **event-based, not time-based.** A bucket with 5 updates/month decays differently than one with 50.
- Cap prior pseudo-observations at ~20 effective samples so old 2025 data doesn't dominate.

**Status: NEEDS IMPLEMENTATION.** Current 0.99 daily decay works but is not optimal. Event-based decay with changepoint detection would be better.

#### 12.2 Companion Market Signals (1.5x Kelly boost)

**Current implementation:** Low spread price → 1.5x Kelly, high spread → 0.5x Kelly

**Gemini verdict: MATHEMATICALLY FLAWED.** Directly modifying the Kelly fraction is dangerous:
- Companion data should update the probability estimate (p), NOT the Kelly multiplier
- Over-betting via 1.5x multiplier inflicts "variance drag" that reduces geometric growth
- Companion signals from low-liquidity markets can be manipulated or stale
- Should gate companion signals by 24h volume and bid-ask spread width (discard if spread > 5c)

**Status: NEEDS REWORK.** The companion signal architecture should feed into Bayesian p-updating, not Kelly multiplication. Current implementation is a shortcut that increases risk.

#### 12.3 Minimum Bet Size ($1.00)

**Current implementation:** Reject trades where Kelly sizes below $1.00

**Gemini verdict: OUTDATED after March 2026.** Kalshi introduced fractional trading with subpenny pricing:
- Fee accumulator now tracks sub-cent rounding across all fills (no more ceil() penalty on micro-bets)
- Agent can execute exact Kelly fractions (e.g., $0.37 worth of contracts)
- The $1.00 floor was optimal under integer contracts but is now unnecessarily restrictive
- Should migrate to `_fp` and `_dollars` API fields for full precision

**Status: PARTIALLY ADDRESSED.** We may want to lower the minimum below $1 to capture more trades, but need to verify Kalshi's fractional API is fully live and stable.

#### 12.4 Multi-Armed Bandit (NOT implemented)

**Current implementation:** Fixed Kelly multipliers per sport (e.g., NHL 0.15, NBA 0.04)

**Gemini recommendation:** f-Discounted Sliding-Window Thompson Sampling (f-dsw TS):
- Reward function should be **Sharpe ratio** (not raw P&L or ROI)
- No hardcoded epsilon needed — Thompson Sampling naturally explores
- MAB operates at macro level (sport profitability), Bayesian at micro level (game probability)
- Feed Beta-Binomial posterior variance into MAB as inverse penalty weight
- With 15 arms and 5-20 observations/month, traditional epsilon-greedy wastes too much of $150 bankroll

**Status: NOT IMPLEMENTED.** Would be a significant upgrade but requires ~200 lines of new code.

#### 12.5 Adaptive Kelly (Variance-Aware Sizing)

**Current implementation:** Fixed fractional Kelly (sport_param * 0.33)

**Gemini recommendation: Distributionally Robust Kelly Problem (DRKP)**
- Extract exact variance from Beta posterior: Var(p) = alpha*beta / ((a+b)^2 * (a+b+1))
- Compute conservative probability: p_conservative = mean - z * std_dev (z=1.0 for 1-sigma penalty)
- Feed p_conservative into Kelly formula instead of point estimate
- Automatically zeros out trades when posterior is too uncertain
- "Eliminates the necessity for arbitrary variance thresholds"

**Status: NOT IMPLEMENTED.** Simple to add (~20 lines). Would make the agent automatically cautious on low-data buckets.

#### 12.6 Pre-Game Price Accuracy

**Current implementation:** Passive scanner uses `previous_price_dollars` from Kalshi API

**Gemini verdict: WRONG FIELD.** `previous_price_dollars` returns the price 24 hours ago, NOT the pre-game closing line:
- Should use historical candlestick endpoint: `GET /series/{series}/markets/{ticker}/candlesticks`
- Extract the final candlestick closing price before `close_time`
- Academic standard: Closing Price for model accuracy evaluation, VWAP for execution benchmarking

**Status: NEEDS FIX.** The Bayesian passive scanner is ingesting inaccurate price data. This corrupts the posterior updates.

### Research-Backed Status Summary (All Features)

| Feature | Research Backed? | Source | Status |
|---------|-----------------|--------|--------|
| FLB strategy (fade favorites) | YES | 3 academic papers, 154M trades | Validated |
| Per-sport edge tables | YES | OOS Jan 2026 validation | Validated for 4 sports |
| Fractional Kelly sizing (0.33x) | YES | Academic standard | Implemented |
| Hold-to-settlement | YES | 4,049 market backtest | Implemented |
| 200% profit-take | YES | Our backtest (+20% vs hold) | Implemented |
| No trailing stop | YES | Our backtest (-72% with stop) | Implemented |
| April/playoff veto | YES | Backtest + Gemini research | Implemented |
| Bayesian Beta-Binomial | YES | Standard conjugate model | Implemented |
| Fee-aware edge (Taker) | YES | Kalshi fee docs + ceil() | Implemented |
| Tiered drawdown circuit breaker | PARTIALLY | Standard risk mgmt practice | Implemented, but HWM inflation from deposits unresolved |
| Companion signals (probability adjustment) | YES | Gemini research: modify p not Kelly | **FIXED** — now adjusts actual_yes_rate +/-3% |
| Bayesian decay (event-based 0.995) | YES | Gemini research: event-based > time-based | **FIXED** — 0.995 per event, not 0.99 per day |
| $1 minimum bet | PARTIALLY | Practical for current bankroll | Implemented |
| Passive scanner price source | PARTIALLY | Quality filter (skip 0-10c and 90-99c) | **FIXED** — filters settlement-snapped prices |
| MAB sport confidence | YES | Simplified version of f-dsw TS | **IMPLEMENTED** — 0.5-1.5x auto-scaling |
| Variance-aware Kelly (DRKP) | YES | Gemini: p_conservative = mean + sigma | **IMPLEMENTED** — adds 1-sigma penalty for uncertain buckets |
| ATP Tennis | YES | 2,520 historical markets, FLB at 71-85c, retirement premium | **ADDED** — year-round, fills off-season gap |
| College Football | PARTIALLY | Gemini confirms FLB, no OOS data yet | **ADDED** — conservative params until Sep validation |
| Weather markets | NO — WRONG STRUCTURE | Categorical ranges, not binary favorites | **DISABLED** — no FLB to exploit |
| Early market entry | YES | Gemini + CLV research: FLB strongest at market open | **IMPLEMENTED** — 1hr re-discovery interval |

---

## 13. Market Expansion Research (Gemini Report #4 — April 12, 2026)

### Markets Analyzed for Expansion

| Market | Ticker | FLB? | Action |
|--------|--------|------|--------|
| **ATP Tennis** | KXATPMATCH | **Strong** at 71-85c (65% YES vs 73-83% implied) | **ADDED** — year-round, 2,520 historical markets |
| **College Football** | KXCFBGAME | **Expected strong** — extreme parity, 90c+ favorites overpriced | **ADDED** — conservative params, awaiting Sep data |
| MLB Game Winners | KXMLBGAME | Very weak — favorites win near implied probability | SKIP |
| Crypto Daily | BTC/ETH/DOGE | No FLB — efficiently priced by institutional MMs | SKIP |
| F1 Race Winners | KXF1RACE | Multi-runner (20+ drivers), not binary | SKIP |
| PGA Golf | KXPGA | Multi-runner, favorites rarely above 55c | SKIP |
| Financial Indices | NASDAQ/S&P | No FLB — efficiently priced | SKIP |

### ATP Tennis Key Findings

- **Retirement premium**: ~2.5% of ATP matches end in retirement. On Kalshi, retirements settle as wins for the advancing player. This gives NO buyers a free 2.5% boost not priced into the market.
- **Surface effects**: Clay and hard courts have higher retirement rates (~2.3%) than grass (~1.9%)
- **Grand Slams have STRONGER FLB** despite best-of-5 format — driven by massive retail sentiment influx
- **ATP is more stable than WTA**: Higher serve hold rates, less momentum volatility, more predictable outcomes
- Edge table: `(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765`

### Closing Line Value (CLV) Research

- First-trade prices are 1.5-2.5 days before game time
- YES prices drift down 5-9c by closing (sharp money corrects the line)
- Professional bettors consider +2-5% CLV the gold standard
- Our backtest ROI (30-40%) should be haircut to **4-8% realistic ROI** per Gemini
- **Key implication**: Early market entry captures opening line value before correction
- Agent re-discovery interval reduced from 2h to 1h to capture more OLV

### Scaling Research

- Taker→Maker transition viable at $1,000-5,000 bankroll
- Natural capacity ceiling for FLB strategy: $100K-500K
- Beyond that, agent's own orders move the market against itself
- Kalshi Liquidity Incentive Program: 100% fee rebates at 300K+ contracts/month
