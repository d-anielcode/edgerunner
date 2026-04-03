# System Memory & Context

<!--
AGENTS: Update this file after every major milestone, structural change, or resolved bug.
DO NOT delete historical context if it is still relevant. Compress older completed items.
-->

## Active Phase & Goal
**Current Phase:** Phase 1 — Architecture & Paper Trading (Weeks 1-2)
**Current Task:** ALL 17 MODULES COMPLETE. Ready for integration testing.
**Next Steps:**
1. Set up real Kalshi demo API credentials (RSA key pair)
2. Set up real Anthropic API key
3. Set up Supabase project and create tables
4. Set up Telegram bot
5. Run end-to-end integration test with `python main.py`
6. Begin paper trading phase (500+ trades target)

## Architectural Decisions
- 2026-04-02 - Chose Kalshi as sole exchange (CFTC-regulated, legal for US, best NBA liquidity). No Polymarket due to US restrictions and VPN/ToS risk.
- 2026-04-02 - Chose Claude Haiku (via Anthropic API) over local models. Better reasoning quality for nuanced sports context. ~$45/month with prompt caching.
- 2026-04-02 - Chose raw Anthropic SDK with strict tool use over Claude Agent SDK. Agent SDK is designed for file-editing agents, not trading bots. Raw API gives precise control.
- 2026-04-02 - Chose `asyncio` as hard requirement. No blocking calls. WebSocket for Kalshi orderbook, polling for NBA data.
- 2026-04-02 - Chose fractional Kelly (0.20x) with 5% max position cap. Full Kelly is too aggressive for noisy LLM probability estimates.
- 2026-04-02 - BallDontLie webhooks are too expensive ($499.99/mo for useful tier). Using REST polling + nba_api package instead.
- 2026-04-02 - Kalshi demo environment (`demo-api.kalshi.co`) for all paper trading. Separate API keys from production.
- 2026-04-02 - Paper/live mode controlled by single `TRADING_MODE` env var. Default is `paper`.

## Known Issues & Quirks
- Kalshi WebSocket is READ-ONLY — cannot place orders via WebSocket, must use REST.
- Kalshi prices use fixed-point dollar strings ("0.6500"), not floats or integer cents.
- Claude Haiku prompt caching requires minimum 4,096 tokens in system prompt.
- Kalshi Basic tier rate limits: 20 reads/sec, 10 writes/sec. Must handle 429s with backoff.
- BallDontLie free tier: only 100 webhook deliveries/month (unusable). REST polling only.
- Supabase free tier: 500MB database, 50,000 rows. Sufficient for paper trading phase.

## Completed Phases
- [x] Part 1 — Deep Research (Gemini + Claude deep dive)
- [x] Part 2 — PRD (`PRD-EdgeRunner-MVP.md`)
- [x] Part 3 — Technical Design (`TechDesign-EdgeRunner-MVP.md`)
- [x] Part 4 — Agent configuration (AGENTS.md, MEMORY.md, agent_docs/, CLAUDE.md)
- [x] Project scaffold and dependency installation (Python 3.14, all deps installed)
- [x] Config module (settings.py with mode switching + startup banner, markets.py with NBA ticker patterns)
- [x] Storage module (async Supabase singleton client, Pydantic V2 models with Decimal + validators)
- [x] Data ingestion (cache with OFI + queue msgs, Kalshi WS with RSA-PSS auth, NBA poller with nba_api executor)
- [x] Signal analysis (TradeDecision schema, 4K+ system prompt with caching, async Claude caller with circuit breaker + budget tracking)
- [x] Smart money module (Polymarket leaderboard + position tracking, live API verified)
- [x] Execution (Kelly engine with 6 safety checks, Kalshi REST client with RSA-PSS, order manager with full lifecycle)
- [x] Alerts (Telegram pipeline with trade/error/summary/startup/shutdown alerts)
- [x] Resilience (circuit breakers for Kalshi + Supabase, retry decorators with exponential backoff)
- [x] Main orchestrator (asyncio.gather with 5 concurrent tasks, Ctrl+C graceful shutdown)
- [ ] Paper trading validation (500+ trades, CLV analysis)
- [ ] Live deployment ($100 bankroll)
