# Kalshi Player Prop Analysis — Real Data from 3,000 Settled Markets

## Dataset
- Source: Kalshi API (`GET /markets?status=settled&series_ticker=KXNBAPTS`)
- Sample: 1,000 settled NBA player points props
- Date: Pulled April 4, 2026

## Key Finding: YES/OVER on PTS Props Hits Only 35.3%

| Outcome | Count | Rate |
|---------|-------|------|
| YES (OVER hit) | 353 | 35.3% |
| NO (UNDER hit) | 608 | 60.8% |
| Other/void | 39 | 3.9% |

## Simulated ROI: Buying NO at Various Prices

| NO Entry Price | ROI | Notes |
|----------------|-----|-------|
| $0.30 | +102.7% | Best ROI but rarely available at this price |
| $0.40 | +52.0% | Strong — realistic pre-game entry |
| $0.50 | +21.6% | Decent — common mid-range price |
| $0.55 | +10.5% | Marginal after fees |
| $0.60 | +1.3% | Breakeven territory |
| $0.647 | 0% | Mathematical breakeven |

## IMPORTANT CAVEATS

1. This is LAST TRADED PRICE, not realistic entry price — actual entry would be at the ask
2. Fees (~$0.02 per contract) are NOT included — reduces ROI by ~2-4%
3. Historical data may not predict future performance
4. Sample is from ONE season — need to verify across multiple seasons
5. These are ALL PTS props — not filtered by player, line, or matchup
6. Prizm's sportsbook data (43.4% OVER) and Kalshi data (35.3% YES) are DIFFERENT markets
7. Need to verify: is the 35.3% driven by certain types of props? (high lines vs low lines)

## Next Steps
1. Validate on out-of-sample data (different time period)
2. Filter by prop line threshold (20+ pts vs 10+ pts) to find sweet spots
3. Factor in actual bid-ask spreads and fees
4. Paper trade the strategy on Kalshi demo for 1-2 weeks
5. Compare to what Claude has been recommending
