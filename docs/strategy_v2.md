# EdgeRunner Strategy v2 — Data-Driven NO Buying with Claude Exception Detection

## The Edge (from 1,000 settled Kalshi PTS props)

| Prop Line | NO Win Rate | Action |
|-----------|-------------|--------|
| 10+ pts | 26% | NEVER buy NO |
| 15+ pts | 49% | Skip — no edge |
| 20+ pts | 72.6% | BUY NO |
| 25+ pts | 81.5% | BUY NO (strong) |
| 30+ pts | 90.5% | BUY NO (very strong) |
| 35+ pts | 84.6% | BUY NO (strong) |

## Strategy

### Step 1: Filter
- Only look at PTS props with lines >= 20
- Prefer low-volume markets (YES rate 24.3% vs 47.7% on high volume)

### Step 2: Claude as Exception Detector
Instead of asking Claude "what's the probability?", ask:
"The base rate for this prop hitting YES is ~27%. Here's the player data.
Is there a SPECIFIC reason this particular prop should be HIGHER than 27%?
If yes, SKIP. If no unusual factors, proceed with NO buy."

Claude is good at identifying exceptions:
- Player averaging 30+ PPG against weak defense → skip NO
- Player on a hot streak AND weak opponent → skip NO
- Player on back-to-back, against elite D → strong NO (even better)

### Step 3: Position Sizing
- 0.25x Kelly based on the base rate edge
- For 25+ lines: edge is ~30% (base rate 81.5% vs paying ~50-60c for NO)
- Max 15% of bankroll per trade

### Step 4: Risk Management
- Quarter-aware trailing stops (already built)
- ESPN game clock integration (already built)
- $0.10 hard floor (already built)

## Why This Should Work
1. Based on REAL Kalshi data, not theoretical assumptions
2. Claude adds value as an exception detector, not probability estimator
3. 72-90% base win rate means we can survive Claude's occasional wrong calls
4. Low-volume props have even bigger edge (less efficient pricing)

## What Needs to Be Validated
1. Paper trade for 1-2 weeks on demo
2. Track: does Claude's exception detection ADD or SUBTRACT value?
3. Verify the edge persists in current markets (not just historical)
4. Factor in actual fees and spreads
