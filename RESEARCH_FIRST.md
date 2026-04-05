# EdgeRunner Development Rule: RESEARCH BEFORE CODE

## Mandate
NEVER add features, change parameters, or modify trading logic without first:
1. Researching whether the change is supported by real data
2. Backtesting or simulating the impact with historical data
3. Documenting the evidence in this file or agent_docs/

## What went wrong
- Added features based on intuition without validation
- Changed parameters (Kelly, edge threshold) multiple times without data
- Player prop trading has been net negative — never verified it's profitable
- Multiple restarts wiped peak prices, causing missed stop-losses
- Built 20+ features in one session without testing any thoroughly

## Questions that need REAL answers before next session
1. Are player prop markets on Kalshi actually profitable for our agent? (Check our actual P&L by market type)
2. What is the optimal edge threshold? (Needs backtesting, not guessing)
3. Is 0.35x Kelly right? (Needs simulation with our actual win rate)
4. Should we even trade player props or only game winners/spreads?
5. What does our actual Brier score look like? (Track predictions vs outcomes)

## Rule going forward
- Research first, code second
- If user suggests a feature, research it before building
- If intuition says X, find data that confirms or denies X
- Better to trade less with proven edge than more with unproven features
