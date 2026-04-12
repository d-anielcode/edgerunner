# Artifact Review Checklist

> **AGENTS:** Do not mark a feature or task as "Complete" until you verify these checks manually or via automated test runs. Provide terminal logs or test results as proof.
> **HUMANS:** Use this checklist before merging Agent-generated code.

## Code Quality & Safety
- [ ] All functions have type hints (parameters + return types).
- [ ] Pydantic models validate all external data (API responses, Claude outputs, WebSocket messages).
- [ ] No bare `except:` clauses — all exceptions are typed and logged.
- [ ] Protected files/directories (`.env`, `keys/`, risk parameters) were NOT modified without permission.
- [ ] No existing, unrelated tests were deleted or skipped.
- [ ] Module is self-contained and doesn't break established architecture boundaries.
- [ ] No blocking calls (`time.sleep()`, `requests.get()`) in the async trading loop.
- [ ] No hardcoded secrets, API keys, or magic numbers.

## Security
- [ ] `.env` is in `.gitignore` and was NOT staged for commit.
- [ ] `keys/` directory is in `.gitignore` and was NOT staged.
- [ ] No API keys, tokens, or private key contents appear in any committed file.
- [ ] Pre-commit secret scanning hook passes.
- [ ] `TRADING_MODE` is still set to `paper` (unless operator explicitly approved `live`).

## Execution & Testing
- [ ] Module runs standalone without crashing: `python -m [module.path]`
- [ ] All imports resolve correctly.
- [ ] Type check passes (if mypy is configured).
- [ ] Related unit tests pass (if tests exist for this module).
- [ ] Terminal output uses `rich` with correct color coding (blue/yellow/green/red).

## Artifact Handoff
- [ ] The `MEMORY.md` file was updated with any new architectural decisions made during this task.
- [ ] Any new dependencies were added to `requirements.txt` via `pip freeze`.
- [ ] Docstrings explain the "why" in plain English for every new function.
