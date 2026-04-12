# EdgeRunner Session Handoff — April 7, 2026

## Current State
- **Agent running 24/7** on DigitalOcean VPS (159.65.177.244, $6/mo, NYC3)
- **Balance:** ~$285 (started $310, lost ~$25 on UCL Bayern bug)
- **Mode:** LIVE trading, DRY_RUN=false
- **SSH:** `ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244`
- **Logs:** `journalctl -u edgerunner -f`
- **Restart:** `systemctl restart edgerunner`

## CRITICAL: Do NOT restart the agent while games are in progress
The Bayern Munich UCL bug ($25 loss) happened because 3 restarts during a live game each wiped/contaminated the discovery price cache. Each restart recorded the mid-game price as the "discovery" price, so the drift detection didn't trigger. Discovery prices now persist to disk (`data/discovery_prices.json`) but if the persisted price is already a mid-game price, the damage is done. **Wait until all games finish before restarting.**

## 12 Active Sports (CPI dropped — market dead since Nov 2024)

| Sport | Ticker Pattern | Kelly | Season | Edge Source |
|---|---|---|---|---|
| NBA | KXNBAGAME | 0.10 (conservative) | Oct-Jun | Favorite-longshot bias |
| NHL | KXNHLGAME | 0.30 (aggressive) | Oct-Apr reg season only | Strongest bias |
| EPL | KXEPLGAME | 0.25 | Aug-May | 71-85c bucket only |
| UCL | KXUCLGAME | 0.12 | Sep-Jun | 66-70c + 76-85c |
| La Liga | KXLALIGAGAME | 0.08 | Aug-May | 81-90c only |
| WNBA | KXWNBAGAME | 0.12 | May-Oct | Skip 66-70c bucket |
| UFC | KXUFCFIGHT | 0.12 | Year-round | 76-85c only |
| NCAA Men's BB | KXNCAAMBGAME | 0.10 | Nov-Mar | 61-80c (dropped 81-90c) |
| NCAA Women's BB | KXNCAAWBGAME | 0.12 | Nov-Mar | All buckets |
| WTA Tennis | KXWTAMATCH | 0.08 | Jan-Nov | Conservative sizing |
| Weather | KXHIGHNY/CHI/MIA/etc | 0.25 | Year-round | Highest ROI (+98%) |
| NFL Anytime TD | KXNFLANYTD | 0.20 | Sep-Jan | 53% WR, +47% ROI |

## Key Parameters
- **MAX_BET_DOLLARS:** $100 per trade (hard cap in execution/risk.py)
- **MAX_DRAWDOWN_PCT:** 40% (circuit breaker, permanent halt per session)
- **MAX_CONSECUTIVE_LOSSES:** 6 (then 10min cooldown)
- **SLIPPAGE:** 0.5c (reduced from 1.5c which was blocking all NBA trades)
- **MARKET_POLL_INTERVAL:** 30 seconds
- **AUTO_PROFIT_TAKE:** 400%
- **TRAILING_STOP:** 25% from peak (only for non-game-winner positions)
- **NBA_POLLER:** Disabled (ENABLE_NBA_POLLER=false, was causing 401 errors)

## Trading Rules
1. **Hold game winners to settlement** — no trailing stops, no early sells. Backtest validates hold-to-settlement only. Selling DEN early at 33c instead of $1.00 cost us $13 on night 1.
2. **Pre-event only** — no mid-game bets. ESPN blocks NBA/NHL in-progress games. Price drift detection (>20% from discovery) blocks all other sports.
3. **One trade per ticker** — duplicate position block in _execute_decision checks `existing_side == new_side`.
4. **NHL playoff veto** — Apr 17 to Sep. Favorites win 80% in playoffs, edge disappears.
5. **Sport-specific Kelly** — NHL gets 3x the position size of NBA because 45% win rate vs 34%.

## Bug Fixes Applied This Session
1. **Cache race condition** — WS was overwriting REST prices with None. Fixed: only update if value is not None.
2. **Position sync** — Kalshi returns negative `position_fp` for NO positions. Fixed parsing.
3. **Duplicate trades** — Agent re-bought same ticker on every poll. Fixed: check if already holding.
4. **Depth gate blocking all trades** — MIN_DEPTH_CONTRACTS was 5, orderbook depth was 0. Set to 0 (disabled).
5. **Slippage eating NBA edge** — 1.5c slippage made NBA edge negative after fees. Reduced to 0.5c.
6. **Bankroll locked to starting** — max_bankroll capped bets at session start amount forever. Removed; MAX_BET_DOLLARS handles it.
7. **Auto-shutdown** — Disabled for 24/7 mode. Market re-discovery every 2 hours instead.
8. **Mid-game trading** — Added pre-event only rule + price drift detection for non-ESPN sports.
9. **Discovery price persistence** — Saved to disk so restarts don't lose the baseline.
10. **Consecutive loss halt** — Raised from 3 to 6 (3 triggers during normal 34% win rate variance).
11. **WebSocket JSON parse** — Added try/catch so malformed messages don't crash the feed.
12. **One-sided orderbook** — Skip instead of inferring extreme prices.
13. **NBA game winners not reaching evaluator** — Cache had best_bid=None due to WS/REST race. Fixed cache protection.
14. **Auto-profit-take too tight** — Raised from 200% to 400% to let winners run.
15. **Market poll too slow** — Reduced from 60s to 30s.

## Backtest Results ($300 start, $100 max bet, recent data only 2025+)
- **$300 → $107,148 over 13 months**
- 1,856 trades, 40.5% win rate
- Max drawdown: 42.2%
- Bankroll never dropped below $300
- $100K milestone: January 2026 (month 13)
- Best months: Dec (+$27K), Oct (+$19K), Nov (+$16K)
- Only losing month: April (-$413)
- Weather carries Jan-Mar, NFL TD dominates Sep-Oct, NHL peaks Nov-Dec

## Night 1 Results (April 6)
- 7 unique markets traded
- DEN NO: Won (Portland upset). With bugs: sold early at 33c for $0.58. Without bugs: would have been +$13.80 held to settlement.
- CLE NO: Won (Memphis upset). Similar — early sell destroyed profit.
- Actual P&L distorted by duplicate bug + early sells
- Without bugs: ~+$42 profit on $24 wagered (43% win rate, 3W/4L)
- Led to hold-to-settlement fix and duplicate block fix

## Night 2 Results (April 7)
- UCL Bayern: Lost $25 across 4 duplicate trades caused by 3 mid-session restarts
- NBA + NHL: Multiple pre-game trades placed legitimately, awaiting settlement
- Led to discovery price persistence fix

## VPS Details
- **Provider:** DigitalOcean
- **Plan:** $6/mo, 1 vCPU, 1GB RAM, NYC3
- **IP:** 159.65.177.244
- **SSH Key:** C:\Users\dcho0\.ssh\digitalocean_edgerunner
- **Service:** systemd `edgerunner.service` runs `python -u runner.py --now`
- **Auto-restart:** On crash (systemd RestartSec=30) + on session end (runner.py)

## GitHub
- **Repo:** github.com/d-anielcode/edgerunner
- **Branch:** main
- **Latest commit:** `bea5172` — Persist discovery prices to disk

## Dataset
- **TrevorJS/kalshi-trades** on HuggingFace: 154M trades, Jun 2021 - Jan 2026
- Downloaded to `data/trevorjs/` locally (not on VPS, not needed for live trading)
- All edge tables calibrated from this dataset

## File Structure (Key Files)
- `signals/rules.py` — THE BRAIN: 12 edge tables, 12 sport params, all trading logic
- `execution/risk.py` — Kelly sizing + $100 max bet cap
- `execution/risk_gates.py` — 5-gate risk system (drawdown, edge, liquidity, concentration, position limit)
- `execution/position_monitor.py` — Hold-to-settlement for game winners, trailing stops for props
- `execution/order_manager.py` — Trade execution, position sync from Kalshi, DRY_RUN support
- `config/markets.py` — 23 ticker patterns, sport identification, GAME_WINNER_PATTERNS
- `config/settings.py` — All env vars: MAX_BET_DOLLARS, ENABLE_NBA_POLLER, DRY_RUN, etc.
- `data/discovery_cache.py` — Persists discovery prices for mid-game detection
- `data/espn_scores.py` — NBA + NHL live scores (no soccer/tennis/UFC feeds)
- `data/cache.py` — In-memory state, protected against None overwrites
- `main.py` — Orchestrator: market discovery, signal evaluation, 2h re-discovery loop
- `runner.py` — 24/7 wrapper with ESPN schedule checking and auto-restart
- `deploy/setup.sh` — VPS setup script (systemd service creation)

## Known Limitations
1. **ESPN only covers NBA + NHL** — UCL/EPL/WTA/UFC/WNBA/Weather use price drift detection instead (20% threshold)
2. **Discovery price contamination on restart** — If restarted during live games, mid-game price becomes the baseline. NEVER restart during games.
3. **Backtest assumes hold-to-settlement** — No intra-game price data in dataset. Live agent matches this now.
4. **$100 bet cap limits compounding** — Growth becomes linear above ~$1K bankroll. Intentional for risk control.
5. **February-March are thin months** — Only weather trades. Expect slow growth.

## User Goals
- **Target:** $100K by end of 2026
- **Starting capital:** ~$314 (deposited $200 on top of $114)
- **Strategy:** Let it run 24/7 autonomously, monitor via Discord alerts
- **Risk tolerance:** Moderate — chose $100 max bet over more aggressive options
