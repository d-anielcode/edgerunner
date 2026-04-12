# Product Requirements

## Core Problem
Retail prediction market traders bleed capital due to emotional bias, slow reaction times, and poor bankroll management. They cannot monitor fast-resolving markets 24/7, miss mispriced lines when real-world events break, and lack the engineering skills to build automated execution systems.

## Primary User Story
"As an ambitious retail trader with limited time, I want an autonomous AI agent that monitors prediction markets 24/7, detects mispriced odds using real-world data, and executes mathematically sized trades on my behalf — so I can capitalize on high-EV opportunities without manual intervention or emotional interference."

## Must-Have Features (P0 — All required for MVP)

### 1. Asynchronous WebSocket Data Ingestion
- Persistent WebSocket connection to Kalshi live order book
- Concurrent polling of NBA data APIs (nba_api + BallDontLie REST)
- In-memory cache with Order Flow Imbalance (OFI) calculation
- Auto-reconnect with exponential backoff (max 60s)
- 30-second watchdog timeout for stale connections
- Zero blocking calls

### 2. LLM Edge Validator (Claude API Integration)
- Claude Haiku via Anthropic SDK with strict tool use
- System prompt cached (>4,096 tokens, 90%+ cache hit rate)
- Returns structured JSON: action, market_id, probabilities, kelly_fraction, rationale
- Rejects signals where edge < 5% or spread > 3 cents
- Average response time < 2 seconds
- Monthly API cost < $45

### 3. Dynamic Kelly Criterion Sizing Engine
- Standard Kelly formula with fractional multiplier (default 0.20x)
- Kalshi fee formula deducted from expected payout
- 1.5 cent slippage buffer
- Max position cap: 5% of bankroll
- Max concurrent positions: 10
- No trades within 5 minutes of market close
- Returns kelly_fraction = 0.0 (PASS) when edge is negative after fees

### 4. Execution & Supabase Logging Protocol
- Kalshi REST API orders with RSA-PSS authentication
- Fixed-point dollar strings for prices
- Handles rejections, partial fills, and 429 rate limits
- Async logging to Supabase (non-blocking)
- Records: timestamp, market, side, price, quantity, kelly_fraction, reasoning, latency
- Paper/live mode via TRADING_MODE env var

### 5. Headless Telegram Alert Pipeline
- Trade execution alerts with structured template
- Error alerts for API failures and disconnects
- Daily summary (trades, P&L, CLV, latency, API cost)
- Fires within 500ms of trade execution
- Non-blocking — alert failure never blocks trading loop

### 6. Post-Session Debrief (AI Analysis)
- End-of-session Claude API call analyzing day's trades
- Evaluates: edge accuracy, Kelly appropriateness, winning/losing patterns
- Sends debrief to Telegram
- ~1 call/day, negligible cost

## NOT in MVP (Explicitly Excluded)
- React Native mobile app or any web UI
- Multi-tenant SaaS infrastructure (Stripe, user auth)
- Cross-exchange arbitrage (Kalshi + Polymarket)
- Automated bankroll withdrawals
- Custom machine learning models
- Premium data feeds (SportsData.io, OpticOdds)
- Polymarket integration (US legal risk)

## Success Metrics

### Phase 1 — Paper Trading (Weeks 1-4)
| Metric | Target |
|--------|--------|
| Closing Line Value (CLV) | Consistently better odds than closing line |
| Execution Latency | < 1 second trigger-to-order |
| Brier Score | < 0.20 (better than random 0.25) |
| System Uptime | Full sessions without crashing |
| API Cost | < $45/month Claude API |

### Phase 2 — Live Trading (Months 2-3)
| Metric | Target |
|--------|--------|
| Real vs Simulated Slippage | Live P&L within 15% of paper P&L |
| Maximum Drawdown | < 30% of peak bankroll |
| Monthly ROI | Positive (realistic: 2-5%) |
| Win Rate | > 52% on resolved trades |

## Constraints
- **Budget:** $50/month total (~$45 Claude API, rest free)
- **Timeline:** Weeks 1-2 paper trading, Weeks 3-4 validation, Month 2 live
- **Security:** All keys in .env, .gitignore enforced, pre-commit secret scanning
- **Exchange:** Kalshi only (CFTC-regulated, legal for US)
- **Architecture:** Pure asyncio, no blocking calls
- **Quality:** Build it right, not fast. A broken bot loses real money.
