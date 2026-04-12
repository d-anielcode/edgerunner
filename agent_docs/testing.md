# Testing Strategy

## Frameworks
- **Unit Tests:** `pytest` + `pytest-asyncio` (for async function testing)
- **Integration Tests:** Standalone module execution (`python -m [module.path]`)
- **Manual Verification:** Kalshi demo environment with mock funds
- **Pre-commit Hooks:** Secret scanning (reject staged files containing API key patterns)

## Rules & Requirements
- **Coverage:** All P0 features must have standalone verification scripts.
- **Before Commit:** Always run `python -m py_compile [file.py]` for syntax check. Run pre-commit hook for secret scanning.
- **Failures:** NEVER skip tests or mock out assertions to make things pass without operator approval. If an Agent breaks something, the Agent must fix it.
- **Async Testing:** Use `pytest-asyncio` with `@pytest.mark.asyncio` for async functions. Never use blocking test patterns.

## Module-Level Testing
Each module must be testable standalone before integration:

```bash
# Test config loads correctly
python -c "from config.settings import *; print(f'Mode: {TRADING_MODE}')"

# Test Supabase connection
python -c "import asyncio; from storage.supabase_client import get_client; asyncio.run(get_client())"

# Test Kalshi WebSocket (streams orderbook data)
python -m data.feeds

# Test NBA poller (fetches current data)
python -m data.nba_poller

# Test Claude integration (returns TradeDecision)
python -m signals.analyzer

# Test Kelly math (correct sizing calculations)
python -m execution.risk

# Test Telegram (sends test message)
python -m alerts.telegram
```

## Pre-Commit Hook
```bash
#!/bin/bash
# .git/hooks/pre-commit — scans for secrets in staged files

PATTERNS="sk-ant""-|ANTHROPIC_API_KEY""=sk|BEGIN RSA PRIVATE"" KEY|supabase_anon""_key|TELEGRAM_BOT""_TOKEN="

if git diff --cached --diff-filter=ACMR | grep -iE "$PATTERNS"; then
    echo "ERROR: Potential secret detected in staged files!"
    echo "Remove the secret from the file and use .env instead."
    exit 1
fi

echo "Pre-commit: No secrets detected. OK."
exit 0
```

Install with:
```bash
cp hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Verification Loop
After building each module:
1. **Compile check:** `python -m py_compile [file.py]`
2. **Standalone run:** `python -m [module.path]` — verify output in terminal
3. **Color check:** Verify `rich` output uses correct colors (blue/yellow/green/red)
4. **Security check:** `git diff --cached` — verify no secrets staged
5. **Commit:** Only after all checks pass

## Integration Testing (Phase 1 Complete)
When all modules are built, run the full agent in paper trading mode:
```bash
TRADING_MODE=paper python main.py
```

Verify:
- [ ] WebSocket connects and streams orderbook data
- [ ] NBA poller fetches data on schedule
- [ ] Claude API returns structured TradeDecision JSON
- [ ] Kelly sizing produces reasonable bet amounts
- [ ] Orders route to Kalshi demo API (not production)
- [ ] Supabase logs all trades with full detail
- [ ] Telegram alerts arrive on phone
- [ ] Circuit breaker triggers on simulated API failure
- [ ] Watchdog detects stale data and reconnects
- [ ] Agent runs for 1+ hour without crashing

## Paper Trading Validation (Weeks 3-4)
After 500+ paper trades are logged:
```sql
-- Run these in Supabase SQL editor

-- 1. Daily P&L
SELECT date, realized_pnl, total_trades, win_rate FROM daily_pnl ORDER BY date DESC;

-- 2. Average execution latency
SELECT DATE(filled_at), AVG(execution_latency_ms) FROM trades GROUP BY 1 ORDER BY 1 DESC;

-- 3. CLV delta (the key metric)
SELECT t.kalshi_ticker, t.price, m.closing_price, (m.closing_price - t.price) as clv_delta
FROM trades t JOIN markets m ON t.market_id = m.id WHERE m.resolved_at IS NOT NULL;

-- 4. Brier score trend
SELECT DATE(scored_at), AVG(brier_score), COUNT(*) FROM brier_scores GROUP BY 1 ORDER BY 1 DESC;
```

**Pass criteria:**
- Positive CLV across trade history
- Brier score < 0.20
- System ran full sessions without crashing
- Claude API cost < $45/month
