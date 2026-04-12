# CLAUDE.md — Claude Code Configuration for EdgeRunner

## Project Context
**App:** EdgeRunner
**Stack:** Python 3.10+ (asyncio), Anthropic SDK, Kalshi API, Supabase, Telegram, rich, Pydantic
**Stage:** MVP Development — Phase 1 (Paper Trading)
**User Level:** Vibe-coder (AI writes all code, operator learns architecture)

## Directives
1. **Master Plan:** Always read `AGENTS.md` first. It contains the current phase, build order, and coding conventions.
2. **Documentation:** Refer to `agent_docs/` for tech stack details, code patterns, testing guides, and product requirements.
3. **Plan-First:** Propose a brief plan and wait for approval before coding.
4. **Incremental Build:** Build one module at a time. Test standalone. Commit. Then move to the next.
5. **Pre-Commit:** Run secret scanning hook before commits. Fix failures.
6. **Explain Everything:** The operator is learning. Explain architectural decisions in plain English alongside code.
7. **Communication:** Be concise. Ask ONE clarifying question when needed. No apologies — just fix.

## Commands
- `pip install -r requirements.txt` — Install dependencies
- `python main.py` — Run the agent
- `python -m [module.path]` — Test a single module (e.g., `python -m data.feeds`)
- `python -c "from config.settings import *; print(TRADING_MODE)"` — Verify config
- `python -m py_compile [file.py]` — Syntax check

## Security Rules (Non-Negotiable)
- NEVER print, log, or commit API keys, tokens, or private key contents
- NEVER change `TRADING_MODE` from `paper` to `live` without explicit operator confirmation
- NEVER modify risk parameters (Kelly fraction, position caps) without operator approval
- ALWAYS verify `.env` is in `.gitignore` before any commit
- ALWAYS use `.env` for secrets — never hardcode

## Architecture Rules
- ZERO blocking calls in the async loop (no `time.sleep()`, no `requests.get()`)
- All external data validated via Pydantic models
- All functions have type hints and docstrings
- Config loaded from `.env` via `config/settings.py` only
- Errors in non-critical paths (Telegram, logging) never crash the trading loop
