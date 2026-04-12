# Gemini Deep Research Prompts — Round 2 (April 12, 2026)

Follow-up research based on findings from our realistic agent simulation backtest. The backtest showed strong edge in NHLSPREAD, NFLTD, NBASPREAD, and NHL, but zero edge in EPL, LALIGA, UFC, and NCAAWB with realistic pricing.

---

## Prompt 7: European Soccer Draw Market Exploitation on Kalshi

```
I run an automated trading agent on Kalshi (CFTC-regulated prediction market, CLOB structure). My fade-the-favorite strategy on EPL, La Liga, and UCL game winner markets showed 0% win rate across 23 trades with realistic (pre-game) pricing in a 2024-2025 backtest. I've disabled these markets.

However, the same Kalshi platform offers TIE/DRAW markets for European soccer. I want to explore whether draw markets offer a different, exploitable edge.

Context on what we already know:
- Kalshi's user base is predominantly US-based retail participants
- US retail bettors historically underestimate draw probability in soccer (culturally unfamiliar with draws)
- Bürgi, Deng & Whelan (2026) confirmed Kalshi's FLB is driven by Maker-Taker microstructure
- European sharp money (Pinnacle, Asian exchanges) cannot directly interact with Kalshi's order book due to US-only regulatory ring-fencing
- Our companion signal system already tracks draw prices from EPL/UCL markets

Questions I need answered:

1. **Draw mispricing on prediction markets:** Is there academic evidence that draw outcomes are systematically mispriced on prediction markets or sportsbooks? Specifically:
   - Do US-based platforms underprice draws relative to European/Asian books?
   - What is the typical calibration error for draw probabilities at different price levels?

2. **Optimal draw strategy:** If draws are underpriced, what's the optimal execution strategy?
   - Buy YES on the draw at low prices (e.g., 20-30c)?
   - Or continue the fade-favorite approach: buy NO on the favorite, where the draw acts as a second winning outcome?

3. **Draw frequency by league and context:**
   - What % of EPL, La Liga, UCL, Ligue 1 matches end in draws?
   - Are draws more common in specific contexts (derby matches, late season, defensive teams)?
   - Is there a "draw bias" analogous to the FLB?

4. **Kalshi-specific draw pricing:** Given Kalshi's US retail base, what behavioral biases would affect draw pricing?
   - Do US bettors systematically avoid draw markets (action bias)?
   - Is the bid-ask spread wider on draw contracts (less liquidity)?

5. **Historical draw betting profitability:** Are there documented strategies that profitably bet on draws in soccer? What ROI do they achieve? How does the edge persist despite market efficiency?

6. **Implementation:** If a draw strategy is viable, what entry criteria should the agent use?
   - Price range for draw contracts (e.g., 20-35c YES)
   - Minimum volume threshold
   - League/competition selection

Please provide specific data, calibration studies, and academic citations. I need to determine if this is worth building into the agent.
```

---

## Prompt 8: In-Play Mean Reversion in Sports Prediction Markets

```
I run an automated trading agent on Kalshi that currently only enters positions pre-game. Academic research suggests that prediction market prices OVERREACT to in-game events, creating mean-reversion opportunities.

Context:
- Kalshi offers live in-play markets for NBA, NHL, MLB, and soccer
- Our agent already has a WebSocket connection receiving real-time orderbook updates
- We currently only use in-play data for profit-taking (sell when position hits 200% gain)
- Our exit strategy research showed that mid-game FLB dynamics create a "reverse FLB" when underdogs take early leads

Questions I need answered:

1. **In-play price overreaction:** Is there academic evidence that prediction market prices overreact to in-game scoring events?
   - Croxson & Reade's research on prediction market efficiency during live events — what did they find?
   - Does the overreaction magnitude differ by sport (NBA vs NHL vs MLB)?
   - How quickly do prices correct after an overreaction (minutes? quarters?)?

2. **Mean-reversion strategy mechanics:** If prices overreact when underdogs score:
   - Should the agent buy YES on the favorite AFTER the underdog takes a lead (betting on reversion)?
   - Or should it buy NO on the favorite at inflated prices AFTER the favorite scores (the "reverse FLB")?
   - What's the optimal entry timing relative to the scoring event?

3. **Quantifying the opportunity:**
   - What is the typical price swing when an underdog scores first in NBA/NHL?
   - What % of the time does the favorite ultimately win after trailing at halftime?
   - Expected value per trade after Kalshi's Taker fee at the in-play spread

4. **Risk management for in-play:**
   - How do institutional MMs behave during live events? Do they widen spreads or pull liquidity?
   - What's the adverse selection risk for a Taker during volatile in-play pricing?
   - Should position sizing be different for in-play vs pre-game trades?

5. **Implementation feasibility on Kalshi:**
   - Does Kalshi's in-play orderbook have sufficient depth for algorithmic execution?
   - What's the typical bid-ask spread during live games vs pre-game?
   - Are there volume/liquidity thresholds below which in-play trading is not viable?

6. **Backtesting methodology:** How would I backtest an in-play strategy using the TrevorJS dataset (which has 154M trade prints with timestamps)? Can I reconstruct intra-game price paths from trade data?

Please provide specific quantitative findings, not just theory. I need to determine if in-play mean reversion is worth the architectural complexity of adding to the agent.
```

---

## Prompt 9: Player Prop FLB on Kalshi — Points, Rebounds, Assists Over/Under

```
I run an automated trading agent on Kalshi exploiting the Favorite-Longshot Bias (FLB) on game winner markets. My backtest shows strong edge in NHLSPREAD ($23K profit, 126% ROI) and NFLTD ($8K, 71% ROI) but limited diversification. I want to explore whether NBA/NFL player prop markets (over/under on points, rebounds, assists, touchdowns) offer additional FLB edge.

Context:
- Kalshi offers player prop markets: KXNBAPTS (points), KXNBAREB (rebounds), KXNBAAST (assists)
- Our agent already discovers these tickers during market scanning
- The TrevorJS dataset contains historical player prop trade data
- Our current architecture supports player prop evaluation (has prop_type detection and quarter-aware stops)
- Becker (2026) found the Maker-Taker wealth transfer is strongest on retail-sentiment-driven markets

Questions I need answered:

1. **Does the FLB apply to player props?** 
   - Is there academic evidence of systematic mispricing in over/under player prop markets?
   - Do retail bettors systematically overbet the "Over" on popular players (star bias)?
   - How does the calibration error compare to game winner FLB?

2. **Which prop types are most mispriced?**
   - Points vs rebounds vs assists vs three-pointers — which has the most retail sentiment?
   - Are star player props (LeBron, Curry) more or less efficiently priced than role players?
   - NFL anytime TD props vs NBA points props — different FLB magnitudes?

3. **Price level analysis for props:**
   - At what YES price levels do over/under props show the most mispricing?
   - Is there an equivalent to the game-winner "sweet spot" of 70-85c?
   - How does the line (e.g., Over 25.5 vs Over 30.5 points) affect the FLB?

4. **Kalshi-specific prop dynamics:**
   - What's the typical volume and spread on Kalshi player prop markets?
   - Are props liquid enough for algorithmic execution (1-5 contracts)?
   - How do prop prices compare to DraftKings/FanDuel lines?

5. **Strategy design:**
   - Should the agent bet Under (NO on Over) as the default prop strategy (analogous to fade-the-favorite)?
   - What Kelly fraction and min_edge would be appropriate for props?
   - Should props be treated as independent bets or correlated with game winner positions on the same game?

6. **Risk factors:**
   - Load management and late scratches — how do prop markets handle player DNPs?
   - Minutes restriction — do prop markets price in reduced minutes for blowouts?
   - Correlation risk — if we hold NBA game winner NO + player points Under on the same game, are they correlated?

Please provide specific calibration data, pricing studies, and practical implementation guidance. The goal is to determine if player props represent a genuine diversification opportunity with positive expected value.
```

---

## Usage Notes

- Paste each into Gemini Pro with Deep Research enabled
- Each is self-contained with full context
- Priority order: Prompt 7 (draw markets) > Prompt 9 (player props) > Prompt 8 (in-play mean reversion)
- Prompt 8 is the most architecturally complex to implement — only pursue if the research strongly supports it
