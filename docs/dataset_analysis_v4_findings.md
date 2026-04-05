# EdgeRunner Dataset Analysis v4 — Breakthrough Findings

## The 5 Exploitable Edges Found in 6.5M NBA Trades

### Edge 1: Fade the Favorite (Game Winners)
- Buying NO on favorites (YES priced 61-99c) yields +26-40% ROI
- The market systematically overprices favorites
- Best: heavy favorites (76-99c YES) → +40% ROI on NO
- AVOID: slight underdogs (26-40c YES) → NO loses -5.6%

### Edge 2: Trade Low-Volume Markets
- Markets with <1K volume: 73% taker win rate
- Markets with 1K-10K: 56.3%
- High volume (100K+): only 53.4%
- Low volume = inefficient pricing = exploitable

### Edge 3: Follow Price Momentum
- When price just DROPPED: taker wins 58.8%
- When price is FLAT: taker wins only 28.1%
- Buy in the direction of recent price movement
- This means: if YES drops from 65c to 55c, buying YES at 55c is profitable

### Edge 4: Trade Off-Hours
- Late night / early morning (5-7 UTC): 53-61% taker win rate
- Peak hours (18 UTC / 1pm ET): only 40% — worst time
- Fewer participants = less efficient = more edge

### Edge 5: Be Contrarian on Props
- 74% of prop takers buy YES — and win only 34.7%
- Being on the opposite side (NO) is structurally advantaged
- But spreads on props are 55c wide — costs eat the edge for takers
- Consider being a MAKER on props instead

## The Optimal Strategy Based on Data

1. Focus on GAME WINNER markets (tightest spreads, most data)
2. Buy NO on favorites priced 61-99c (highest ROI buckets)
3. Prefer low-volume markets under 10K volume
4. Follow price momentum — buy after price drops
5. Trade off-peak hours when possible
6. Use Claude to identify WHY a favorite might be overpriced (injuries, back-to-back, etc.)
7. The 5-gate risk system prevents fee-negative trades

## What Claude's Role Should Be
Claude should NOT estimate raw probabilities (it's bad at this).
Claude SHOULD:
- Identify favorites that are MORE overpriced than usual (injury news, rest days)
- Flag games where the underdog has a specific advantage the market hasn't priced
- Interpret breaking news that might shift probabilities
- Act as an exception detector: "the base rate says fade the favorite, is there a reason NOT to?"
