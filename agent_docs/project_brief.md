# Project Brief

- **Product Vision:** EdgeRunner eliminates human limitations of slow reaction times and emotional bias by autonomously executing mathematically sized trades on mispriced prediction market odds 24/7.
- **Target Audience:** Phase 1 — solo operator (the developer). Phase 2 — ambitious retail traders and sports bettors who understand EV but lack engineering skills to build automated systems.
- **Core Value:** Find the edge (mispriced odds), protect the bankroll (Kelly sizing), execute instantly (async architecture).

## Conventions

### Naming
- **Files:** `snake_case.py`
- **Functions/variables:** `snake_case`
- **Classes/Pydantic models:** `PascalCase`
- **Constants/env vars:** `UPPER_SNAKE_CASE`

### File Structure
- One responsibility per folder: `data/` ingests, `signals/` reasons, `execution/` trades, `storage/` logs, `alerts/` notifies, `resilience/` recovers
- Each folder has an `__init__.py`
- Tests (when added) live in a separate `tests/` directory mirroring the source structure

### Docstrings
- Every function has a docstring explaining WHAT it does and WHY in plain English
- The operator is learning architecture — explanations are part of the deliverable

## Key Principles
1. **Every line of code serves one of two purposes:** finding the edge or protecting the bankroll. If it doesn't, cut it.
2. **Prove before you build:** No UI, no SaaS, no consumer features until paper trading proves positive CLV over 500+ trades.
3. **Optimal over familiar:** Use the best tool for the job (asyncio, WebSockets) even if there's a learning curve.
4. **Security is non-negotiable:** `.env` in `.gitignore` before any code exists. Pre-commit hooks scan for secrets.
5. **One module at a time:** Build, test standalone, commit. Then move to the next.

## Quality Gates
- All functions typed (parameters + return types)
- Pydantic validates all external data
- No bare `except:` — all exceptions typed and logged
- Module runs standalone without crashing before integration
- Pre-commit hook passes (no secrets staged)
- `rich` terminal output follows color convention (blue/yellow/green/red)

## Key Commands
- **Run agent:** `python main.py`
- **Test module:** `python -m [module.path]`
- **Check config:** `python -c "from config.settings import *; print(TRADING_MODE)"`
- **Install deps:** `pip install -r requirements.txt`
- **Freeze deps:** `pip freeze > requirements.txt`

## Update Cadence
- Update `MEMORY.md` after every completed module or architectural decision
- Update `AGENTS.md` if build order or conventions change
- Update `requirements.txt` after adding any new dependency
