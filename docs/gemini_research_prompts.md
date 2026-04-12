# Gemini Deep Research Prompts — April 12, 2026

Queue these overnight. Each is self-contained with full context so Gemini can work independently.

---

## Prompt 1: Q2 2026 Kalshi FLB Edge Persistence

```
I run an automated trading agent ("EdgeRunner") on Kalshi, the CFTC-regulated prediction market exchange. The strategy exploits the Favorite-Longshot Bias (FLB): betting NO on overpriced favorites in sports game-winner markets. The agent trades NBA, NHL, MLB, NCAAMB, EPL, UCL, La Liga, WNBA, NFLTD, ATP Tennis, and College Football.

Context on what we already know:
- The FLB was validated academically (Burgi/Deng/Whelan 2026, Le 2026, Becker 72.1M trades)
- Out-of-sample validation (Jan 2026 TrevorJS dataset) confirmed edge for NHL, NFLTD, Weather, NCAAMB but showed decay for NBA, NBASPREAD, NFLSPREAD, NHLFG
- SIG became a designated market maker for Kalshi in 2025
- Jump Trading began aggressively making markets on Kalshi in 2025-2026
- Tradeweb announced institutional access partnership in February 2026
- Woodland & Woodland found NHL Reverse FLB was profitable for 3 seasons then disappeared over 7

Questions I need answered:

1. **Current state of Kalshi market efficiency (Q1-Q2 2026):** Have institutional MMs (SIG, Jump, Tradeweb) significantly compressed the FLB across all sports, or is it concentrated in high-liquidity markets (NBA, NFL)? Is there any public data, analysis, or commentary on Kalshi pricing accuracy in 2026?

2. **Low-liquidity market resilience:** For markets with lower daily volume (NCAAMB, EPL, UCL, La Liga, ATP, WNBA, Weather), is there evidence that institutional MMs have expanded into these verticals? Or are they still primarily retail-driven?

3. **FLB decay timeline modeling:** Given the academic literature on prediction market efficiency (including Woodland & Woodland's NHL study), what is the expected timeline for FLB compression on Kalshi? Are there models that predict when a prediction market transitions from "exploitable retail inefficiency" to "efficiently priced"?

4. **MLB-specific FLB:** My agent just expanded into MLB Total (over/under) markets. Is there academic or empirical evidence for FLB in MLB totals on prediction markets or sportsbooks? MLB game-winner markets showed very weak FLB in our analysis.

5. **Seasonal vs structural edge decay:** Our NBA edge decayed progressively through the 2025-26 season (strong Oct-Dec, weak Jan+). Is this pattern consistent with institutional MM learning curves, or could it reflect seasonal factors (roster changes, trade deadline, load management)?

6. **Survivorship of FLB strategies:** Among all documented FLB exploitation strategies across prediction markets and sportsbooks, what is the typical lifespan before the edge is arbitraged away? Are there examples of FLB strategies that remained profitable for 3+ years?

Please cite specific papers, datasets, and quantitative findings where possible. I'm especially interested in any 2025-2026 publications analyzing Kalshi specifically.
```

---

## Prompt 2: Bayesian Decay Rate and Changepoint Detection for Prediction Markets

```
I'm building a Bayesian updating system for an automated sports prediction market trading agent. The system uses Beta-Binomial conjugate priors to track the YES hit rate for sports favorites across 5-cent price buckets (e.g., "NBA favorites priced 70-74c win X% of the time").

Current implementation:
- Prior: Beta(alpha, beta) initialized from 2025 historical data (thousands of resolved markets)
- Update: After each game resolves, alpha or beta incremented by 1
- Decay: 0.995 per event (multiply alpha and beta by 0.995 before each update)
- This gives a half-life of ~139 events per bucket
- Buckets typically see 5-50 events per month depending on sport/season

The problem: A fixed decay rate is a rigid compromise. If a regime shift happens (e.g., institutional market makers enter and compress the FLB overnight), the 0.995 decay takes ~300 events to cut the prior in half — potentially months of losing trades before the model adapts.

Questions I need answered:

1. **Optimal decay rate for sports prediction markets:** Given typical observation frequencies (5-50 events/month per bucket, 15 sports, 5-8 buckets each), what decay rate balances responsiveness to regime shifts vs stability? Is there a principled way to derive this from the data rather than picking a number?

2. **Bayesian Online Changepoint Detection (BOCPD):** How would I implement BOCPD (Adams & MacKay 2007) for this use case? Specifically:
   - What should the run length prior be for sports market regimes?
   - How do I handle the fact that each bucket has sparse observations (5-50/month)?
   - Can BOCPD work with Beta-Binomial observations directly?
   - What's the computational cost per update? (Agent runs on a 1-vCPU VPS)

3. **CUSUM vs BOCPD:** For detecting regime shifts in prediction market efficiency, which is more appropriate — CUSUM control charts or full BOCPD? CUSUM is simpler but less principled. Given my constraints (small bankroll, need fast detection), which is recommended?

4. **Dual-state Bayesian filter:** A previous Gemini research session recommended a slow state (0.95/event) and fast state (0.70/event) ensemble. How should these be combined? Weighted average? Switch based on variance? What's the academic basis for dual-rate filtering in financial applications?

5. **Prior strength capping:** Should I cap the effective prior at ~20 pseudo-observations so old 2025 data doesn't dominate? What's the right way to implement this — hard cap on alpha+beta, or more aggressive decay on the initial prior?

6. **Event-based vs time-based decay:** Current system is event-based (decay on each update). But buckets with 50 events/month decay much faster than buckets with 5 events/month. Should I normalize decay by calendar time instead? Or use a hybrid approach?

Please provide specific mathematical formulations, pseudocode where helpful, and citations to relevant papers. I need implementable recommendations, not just theory.
```

---

## Prompt 3: Execution Realism for Small-Bankroll Prediction Market Trading

```
I run an automated trading agent on Kalshi (CFTC-regulated prediction market, CLOB structure) with a ~$137 bankroll. My backtests show 30-40% ROI using historical "first trade prices" from resolved markets, but live trading has significantly underperformed. I need to understand the realistic execution gap.

Current backtest methodology:
- Entry price: "first trade price" from TrevorJS dataset (154M Kalshi trades, 2024-2025)
- Exit: Hold to settlement (binary outcome: win full payout or lose stake)
- Fees: ceil(0.07 * contracts * price * (1-price)) per trade (Taker rate with ceil rounding)
- Position sizing: Fractional Kelly (0.33x) with sport-specific Kelly fractions (0.04 to 0.35)
- Typical trade: 1-5 contracts at $0.15-$0.40 per contract (NO side of overpriced favorites)

Known issues with the backtest:
1. First-trade prices occur 1-2 days before game time when markets open. By game time, prices move 5-20c as sharp money corrects the line.
2. No bid-ask spread modeling. The agent crosses the spread as a Taker.
3. No slippage modeling. Orderbook may move between poll cycles (60-second intervals).
4. Discrete contract rounding not modeled (must buy whole contracts).

Questions I need answered:

1. **Closing Line Value (CLV) research for Kalshi:** A previous research session estimated 5-9c of CLV drift between first-trade and game-time prices. Is there academic literature quantifying CLV on prediction markets specifically? How does Kalshi CLV compare to sportsbook CLV?

2. **Realistic ROI haircut:** If my backtest shows 30-40% ROI on first-trade prices, what is a defensible estimate of live ROI after accounting for:
   - CLV drift (entry at game-time price, not first-trade)
   - Bid-ask spread costs (typically 1-3c pre-game, 3-10c in-play)
   - Taker fees with ceil() rounding on small lots (1-5 contracts)
   - 60-second poll latency (price may move between signal and execution)
   
3. **Small-bankroll execution disadvantages:** With $137 bankroll trading 1-5 contracts per position, what specific disadvantages do I face vs. larger traders? Quantify the impact of:
   - Ceil() fee rounding on 1-3 contract trades vs 100+ contract trades
   - Inability to split orders across price levels
   - Minimum tick size ($0.01) as % of edge
   
4. **Orderbook replay backtesting:** Is there a methodology for backtesting against historical orderbook snapshots (Level 2 data) rather than trade prices? The PredictionMarketBench framework (Arora & Malpani 2026) was mentioned as a potential solution — what does it offer?

5. **Optimal execution for retail on CLOB prediction markets:** Given my constraints (small lots, 60-second poll cycle, Taker execution), what execution optimizations are possible?
   - Limit orders at bid vs. market orders crossing the spread?
   - Timing optimization (when in the pre-game window is FLB most pronounced)?
   - Should I use Kalshi's WebSocket feed instead of REST polling for lower latency?

6. **Break-even edge threshold:** Given realistic execution costs (spread + fees + slippage), what minimum raw edge is needed for a $137 bankroll Taker to break even? My current min_edge filter is 5-12% depending on sport.

Please provide quantitative estimates with confidence intervals where possible. I need numbers I can plug into my execution model, not just qualitative observations.
```

---

## Prompt 4: Kalshi Fractional Trading and Sub-Penny API (March 2026)

```
I run an automated trading agent on Kalshi using their REST API for order execution. A previous research session (Gemini Report #3, April 11 2026) stated that "Kalshi introduced fractional trading with subpenny pricing" in March 2026, including:
- A fee accumulator that tracks sub-cent rounding across fills
- Ability to execute exact Kelly fractions (e.g., $0.37 worth of contracts)
- New API fields: `_fp` and `_dollars` for full precision

Questions I need answered:

1. **Has Kalshi actually launched fractional trading?** Is there any public announcement, API changelog, or documentation confirming this feature as of April 2026? Check Kalshi's blog, API docs, developer changelog, and community forums.

2. **API field changes:** If fractional trading is live, what are the exact API field names and semantics? Specifically:
   - Do orders still require integer contract quantities, or can I specify fractional amounts?
   - How does the fee accumulator work? Is it per-account, per-day, or per-trade?
   - What API fields should I use for order placement (`count` vs `_fp` vs `_dollars`)?
   - Are there new order types or parameters?

3. **Impact on small-bankroll traders:** If fractional trading IS live:
   - Can I place a $0.37 order (less than 1 full contract)?
   - Does the fee accumulator eliminate the ceil() rounding penalty on micro-trades?
   - What's the minimum order size now?

4. **If fractional trading is NOT yet live:** What is the current minimum order size on Kalshi? Is it still 1 contract minimum? Has anything changed about the fee structure since January 2026?

5. **Kalshi API version and recent changes:** What is the current Kalshi API version? Have there been any breaking changes or new features in 2026 that would affect an automated trading agent using REST + RSA-PSS authentication?

I need to know whether to lower my $1.00 minimum bet floor or keep it. If fractional trading exists, I'm leaving money on the table by rejecting sub-$1 Kelly recommendations.
```

---

## Prompt 5: Drawdown Calculation with Mid-Strategy Capital Changes

```
I run an automated trading agent with a small bankroll ($137). The agent uses tiered drawdown circuit breakers:
- 15% drawdown from high-water mark (HWM) → reduce Kelly to 50%
- 25% drawdown → reduce Kelly to 25%
- 40% drawdown → halt all trading

The problem: When I deposit additional funds (e.g., add $50 to recover from a losing streak), the portfolio value jumps, which updates the HWM. Now the drawdown calculation uses the inflated HWM, making the circuit breakers MORE protective than intended (they trigger at a higher dollar amount).

Example:
- Start: $310 bankroll, HWM = $310
- Lose to $55 (82% drawdown) — circuit breakers should have halted at $186 (40% DD)
- Deposit $100 → portfolio = $155, HWM updates to $155
- Now 40% DD threshold = $93 — but I've only "earned" $55 of that $155
- The $100 deposit inflated the HWM without any trading profit

Questions I need answered:

1. **Industry standard for deposit-adjusted drawdown:** How do professional quant funds, hedge funds, and prop trading firms calculate drawdown when investors add or withdraw capital mid-strategy? Specifically:
   - Modified Dietz method
   - Time-weighted return (TWR) vs money-weighted return (MWR)
   - High-water mark adjustment methods

2. **Deposit-adjusted HWM calculation:** What's the correct formula for updating the high-water mark when a deposit occurs? Options I've considered:
   - Option A: Don't update HWM on deposits (HWM only rises from trading profits)
   - Option B: Track deposits separately and subtract from HWM (HWM_adjusted = HWM - total_deposits)
   - Option C: Use a "shares" approach (NAV per share, deposits buy more shares but don't change per-share HWM)
   - Which is most appropriate for a single-strategy automated agent?

3. **NAV-per-share approach:** If I use a unit-based NAV:
   - Initial: 100 shares at $3.10/share ($310 total)
   - After loss: 100 shares at $0.55/share ($55 total)
   - Deposit $100: buy 181.8 new shares at $0.55/share (281.8 total shares, $155 total)
   - HWM per share: still $3.10 (unchanged by deposit)
   - Current drawdown: ($3.10 - $0.55) / $3.10 = 82.3% (correctly reflects trading performance)
   Is this the right implementation? Are there edge cases or gotchas?

4. **Withdrawal handling:** If I withdraw profits, should the HWM decrease proportionally? How do hedge fund HWM provisions handle withdrawals?

5. **Circuit breaker implications:** Given a small bankroll ($137) where deposits may represent 30-70% of total capital, which drawdown method is most robust for triggering risk circuit breakers? The method must be:
   - Simple to implement (< 50 lines of Python)
   - Correct regardless of deposit/withdrawal timing
   - Persistable to a JSON file (survives agent restarts)

Please provide specific formulas, pseudocode, and cite relevant quantitative finance literature. I need an implementable solution, not just theory.
```

---

## Prompt 6: Small-Sample Validation of Exit Strategy Thresholds

```
I run an automated prediction market trading agent that recently implemented a 200% profit-take exit strategy (sell position when unrealized gain reaches 200% of entry cost). This was based on a backtest of 4,049 historical markets showing +20% more P&L vs hold-to-settlement.

The problem: I now need to validate this threshold with live trading data, but I'll only accumulate 50-200 live trades over the next few months. With such a small sample, how do I determine if the 200% threshold is actually optimal, or if I should adjust it?

Context:
- Binary outcome markets (sports game winners on Kalshi)
- Hold to settlement: binary payout ($1 if correct, $0 if wrong)
- Profit-take: sell mid-game when position value rises to 3x entry price (200% gain)
- Typical entry price: $0.15-$0.40 (buying NO on overpriced favorites)
- Win rate (hold to settlement): ~40-45%
- Average holding period: 2-4 hours (game duration)
- Backtest showed Sharpe ratio improved from 0.171 (hold) to 0.246 (200% PT)

Questions I need answered:

1. **Minimum sample size for exit threshold validation:** How many live trades do I need to statistically distinguish between "200% profit-take is better than hold" vs "200% and hold are equivalent"? Given:
   - Expected effect size: +20% more total P&L
   - Base win rate: ~42%
   - Variance in per-trade returns is high (binary outcomes)
   What power analysis applies here?

2. **Sequential testing methods:** Since I'm accumulating trades one at a time over months, are there sequential hypothesis testing methods (SPRT, Bayesian sequential design) that can give me an earlier answer than waiting for a fixed sample size? Specifically:
   - Can I run a paired comparison (each trade has both "what I did" and "what would have happened with hold")?
   - What stopping rules are appropriate?

3. **Bayesian approach to threshold optimization:** Instead of testing "is 200% better than hold?", I want to estimate the optimal threshold from live data. Can I:
   - Track the maximum unrealized gain for each position
   - Fit a distribution to these maxima
   - Use the posterior to estimate expected P&L at different thresholds (100%, 150%, 200%, 250%, 300%)?
   - What prior should I use? (I have the 4,049-market backtest as prior data)

4. **Regime-dependent thresholds:** The optimal profit-take likely varies by:
   - Sport (NHL games are lower-scoring, more volatile)
   - Entry price (cheap NO positions have more room to run)
   - Game state (selling in the 1st half vs 2nd half)
   Are there methods for estimating conditional thresholds with small samples?

5. **Counterfactual estimation:** For trades where I DO take the 200% profit, I can't observe what would have happened if I held. But I CAN observe the final settlement price. How do I construct a proper counterfactual comparison using:
   - The settlement outcome (did the favorite actually win?)
   - The price at which I sold
   - The entry price
   
6. **Practical decision framework:** Given that I'll have ~100 live profit-take events after 2-3 months, what's a practical decision framework for:
   - Keeping the 200% threshold
   - Adjusting up or down
   - Switching back to hold-to-settlement if the threshold is hurting performance

Please provide specific statistical tests, formulas, and sample size calculations. I need a monitoring framework I can implement in Python, not just theory.
```

---

## Usage Notes

- Paste each prompt into Gemini Pro with Deep Research enabled
- Each is self-contained — no need to reference previous prompts
- Save the responses and bring them back for implementation
- Priority order: Prompt 1 (edge persistence) > Prompt 3 (execution realism) > Prompt 2 (Bayesian) > Prompt 5 (drawdown) > Prompt 6 (exit validation) > Prompt 4 (fractional trading)
