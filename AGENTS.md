# AGENTS.md — Master Plan for EdgeRunner

## Project Overview & Stack
**App:** EdgeRunner
**Overview:** An autonomous AI trading agent that day-trades NBA/sports prop markets on Kalshi. It uses Claude Haiku as the reasoning engine to detect mispriced odds by comparing market prices against real-world data (injuries, stats, lineup changes), sizes bets via fractional Kelly Criterion, and executes trades autonomously via Kalshi's REST API. The operator walks away and receives Telegram alerts. No frontend, no UI — terminal + Telegram only.
**Stack:** Python 3.10+ (asyncio), Anthropic SDK (Claude Haiku, structured outputs + prompt caching), Kalshi API (RSA-PSS auth, REST + WebSocket), Supabase (PostgreSQL, async client), Telegram Bot API, rich (terminal UI), Pydantic (schema validation), aiobreaker (circuit breaker), tenacity (retry logic), nba_api + BallDontLie REST (sports data)
**Critical Constraints:**
- Pure asyncio — zero blocking calls (`time.sleep()`, `requests.get()`) in the trading loop
- All API keys in `.env` only, `.gitignore` enforced before first code commit
- `TRADING_MODE=paper` by default — never accidentally trade live
- Kalshi only (no Polymarket) — CFTC-regulated, legal for US residents
- Budget ceiling: $50/month total ($45 Claude API, rest free tiers)
- Max 5% bankroll per trade, fractional Kelly 0.20x

## How I Should Think
1. **Understand Intent First**: Before answering, identify what the user actually needs
2. **Ask If Unsure**: If critical information is missing, ask before proceeding
3. **Plan Before Coding**: Propose a plan, ask for approval, then implement
4. **Verify After Changes**: Run tests/linters or manual checks after each change
5. **Explain Trade-offs**: When recommending something, mention alternatives
6. **Explain the Why**: The operator is learning — explain architectural decisions in plain English alongside code

## Setup & Commands
Execute these commands for standard development workflows. Do not invent new package manager commands.
- **Setup:** `pip install -r requirements.txt`
- **Development:** `python main.py`
- **Test single module:** `python -m [module.path]` (e.g., `python -m data.feeds`)
- **Test all connections:** `python -c "from config.settings import *; print(TRADING_MODE)"`
- **Linting:** `python -m py_compile [file.py]` (basic syntax check)
- **Type checking:** `mypy [file.py]` (if mypy installed)

## Protected Areas
Do NOT modify these areas without explicit human approval:
- **Environment:** `.env` file contents — never log, print, or commit API keys
- **Private Keys:** `keys/` directory — RSA-PSS keys are never committed or displayed
- **Trading Mode:** `TRADING_MODE` env var — never change from `paper` to `live` without explicit operator confirmation
- **Risk Parameters:** `MAX_POSITION_PCT`, `FRACTIONAL_KELLY`, `MAX_CONCURRENT_POSITIONS` — operator must approve changes
- **Database Schema:** Existing Supabase table schemas — propose migrations, don't execute unilaterally
- **Git Hooks:** Pre-commit secret scanning hooks

## Coding Conventions
- **Formatting:** PEP 8 style. 4-space indentation. Max line length 120 characters.
- **Naming:** `snake_case` for files, functions, and variables. `PascalCase` for classes and Pydantic models. `UPPER_SNAKE_CASE` for constants and env vars.
- **Architecture:** Each folder has a single responsibility (data, signals, execution, storage, alerts, resilience). No circular imports. Config loaded from `.env` via `config/settings.py` only.
- **Type Hints:** All function parameters and return types must be typed. Use Pydantic models for all data structures crossing module boundaries.
- **Error Handling:** Explicit error types. No bare `except:` clauses. All exceptions logged to terminal (via `rich`) and Supabase. Errors in non-critical paths (Telegram, logging) must never crash the trading loop.
- **Async:** All I/O operations must use `async/await`. Use `aiohttp` or `httpx` for HTTP calls, `websockets` for WebSocket connections, `supabase` async client for database operations.
- **Docstrings:** Every function must have a docstring explaining what it does and why in plain English.
- **Constants:** All magic numbers (Kelly fraction, thresholds, timeouts) come from `.env` or `config/settings.py`. Never hardcoded inline.

## Engineering Constraints

### Type Safety (No Compromises)
- All function parameters and returns must be typed
- Use Pydantic for runtime validation of all external data (API responses, Claude outputs, WebSocket messages)
- No `Any` types — use `Unknown` with type guards or specific types

### Architectural Sovereignty
- `main.py` handles task orchestration ONLY
- All business logic goes in `signals/`, `execution/`, or `data/`
- No database calls from `main.py` — those go through `storage/`
- No direct API calls from `signals/` — those go through `execution/` or `data/`

### Library Governance
- Check existing `requirements.txt` before suggesting new dependencies
- Prefer stdlib (`asyncio`, `json`, `os`) over third-party when equivalent
- No deprecated patterns (use `aiohttp` not `requests` in async code)

### The "No Apologies" Rule
- Do NOT apologize for errors — fix them immediately
- Do NOT generate filler text before providing solutions
- If context is missing, ask ONE specific clarifying question

### Workflow Discipline
- Pre-commit hooks must pass before commits
- If verification fails, fix issues before continuing
- Test each module in isolation before integrating

## Agent Behaviors
These rules apply across all AI coding assistants:
1. **Plan Before Execution:** ALWAYS propose a brief step-by-step plan before changing more than one file.
2. **Build in Order:** Follow the build order in the Tech Design (config → storage → data → signals → execution → alerts → resilience → main).
3. **One Module at a Time:** Build one module, test it, commit it. Then move to the next.
4. **Context Compaction:** Write states to `MEMORY.md` instead of filling context history during long sessions.
5. **Iterative Verification:** Run the module standalone after each change. Fix errors before proceeding (See `REVIEW-CHECKLIST.md`).
6. **Security First:** Before any commit, verify no secrets are staged (`git diff --cached`). Run pre-commit hook.

## What NOT To Do
- Do NOT delete files without explicit confirmation
- Do NOT modify Supabase table schemas without operator approval
- Do NOT add features not in the current phase (no UI, no Polymarket, no custom ML)
- Do NOT skip testing for "simple" changes
- Do NOT bypass failing tests or pre-commit hooks
- Do NOT use deprecated or synchronous libraries in the async loop
- Do NOT hardcode API keys, tokens, or secrets anywhere
- Do NOT change `TRADING_MODE` from `paper` to `live` without explicit operator command
- Do NOT use `time.sleep()` — use `asyncio.sleep()` if delays are needed
- Do NOT use `requests` library — use `aiohttp` or `httpx` for async HTTP

## Build Order (Phase 1)
| Step | Module | File | Depends On |
|------|--------|------|------------|
| 1 | Config loader | `config/settings.py` | Nothing |
| 2 | Market config | `config/markets.py` | Config |
| 3 | Supabase client | `storage/supabase_client.py` | Config |
| 4 | DB models | `storage/models.py` | Nothing |
| 5 | Kalshi WebSocket | `data/feeds.py` | Config |
| 6 | NBA poller | `data/nba_poller.py` | Config |
| 7 | In-memory cache | `data/cache.py` | Feeds + Poller |
| 8 | Output schemas | `signals/schemas.py` | Nothing |
| 9 | System prompt | `signals/prompts.py` | Schemas |
| 10 | Claude analyzer | `signals/analyzer.py` | Schemas + Prompts + Config |
| 11 | Kelly engine | `execution/risk.py` | Config |
| 12 | Kalshi REST client | `execution/kalshi_client.py` | Config |
| 13 | Order manager | `execution/order_manager.py` | Kalshi Client + Risk |
| 14 | Telegram alerts | `alerts/telegram.py` | Config |
| 15 | Circuit breaker | `resilience/circuit_breaker.py` | Nothing |
| 16 | Retry logic | `resilience/retry.py` | Nothing |
| 17 | Main orchestrator | `main.py` | All above |
