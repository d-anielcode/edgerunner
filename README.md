# EdgeRunner

An autonomous trading agent for CFTC-regulated prediction markets. EdgeRunner identifies and exploits structural pricing inefficiencies across 12 sports and event categories on the Kalshi exchange, executing trades 24/7 with zero human intervention.

## What It Does

EdgeRunner buys underpriced contracts on prediction market outcomes where the crowd systematically misprices probabilities. The strategy is grounded in the **favorite-longshot bias** — a well-documented behavioral finance anomaly where market participants overpay for likely outcomes and underpay for unlikely ones.

The agent:
- Monitors live orderbooks across **12 market categories** (NBA, NHL, EPL, UFC, weather, and more)
- Evaluates each market using empirically-calibrated edge tables derived from **154 million historical trades**
- Sizes positions using a modified **Kelly Criterion** with sport-specific parameters
- Executes trades autonomously via Kalshi's REST and WebSocket APIs
- Manages risk through a 5-gate system (drawdown circuit breaker, fee-adjusted edge, liquidity, concentration, position limits)

## Architecture

```
Market Discovery --> Orderbook Polling --> Signal Evaluation --> Risk Gates --> Order Execution
       |                                                                           |
  Re-discovery (2h)                                                Position Monitor + Discord Alerts
```

**Key components:**
- `signals/rules.py` — Rules-based evaluator with per-price Kelly sizing and cross-market companion signals
- `execution/risk_gates.py` — 5-gate risk management system
- `execution/position_monitor.py` — Trailing stop-loss with sport-aware hold-to-settlement logic
- `data/espn_scores.py` — Real-time ESPN integration for live game state
- `runner.py` — 24/7 process manager with auto-restart and ESPN schedule detection

## Research & Data

The strategy was developed through extensive quantitative analysis:
- **154M trades** from the TrevorJS/kalshi-trades HuggingFace dataset (Jun 2021 - Jan 2026)
- **33,000+ sportsbook games** cross-validated against 10 years of NBA/NHL moneyline data
- Per-cent price analysis, order flow microstructure, volume-edge relationships
- Seasonal pattern detection, cross-market divergence signals, home/away splits

## Tech Stack

- **Python 3.12** with asyncio for concurrent market monitoring
- **DuckDB** for high-performance analytical queries on Parquet datasets
- **Kalshi API** (REST + WebSocket) with RSA-PSS authentication
- **ESPN API** for real-time scores and schedule data
- **Supabase** for trade logging and decision tracking
- **Discord Webhooks** for real-time trade alerts and daily recaps
- **DigitalOcean VPS** for 24/7 autonomous operation
- **systemd** service with auto-restart and crash recovery

## Deployment

The agent runs autonomously on a DigitalOcean droplet in NYC for low latency to Kalshi servers. Deployment is handled via `deploy/sync.sh` which transfers only production code -- no datasets, tests, or research scripts.

```bash
bash deploy/sync.sh
ssh root@YOUR_IP 'systemctl restart edgerunner'
ssh root@YOUR_IP 'journalctl -u edgerunner -f'
```

## Disclaimer

This project is for educational and research purposes. Trading on prediction markets involves substantial risk of loss. Past performance does not guarantee future results.
