# Risk Architecture: Unitized NAV Drawdown + Bayesian Prior Capping

## Problem

Two critical risk management flaws in the live EdgeRunner agent:

1. **Deposit inflation corrupts drawdown circuit breakers.** The current absolute-dollar HWM in `hwm_cache.py` rises when deposits are added, making percentage drawdown calculations erroneously severe. A $50 deposit on a $55 bankroll inflates the HWM by 90%, potentially triggering false circuit breaker halts.

2. **Bayesian priors can lock in.** The Beta-Binomial priors in `bayesian_cache.py` have no cap on effective sample size. With thousands of 2025 historical observations, alpha+beta can pool at values that make the posterior nearly immovable by new live data. A regime shift (e.g., institutional MMs compressing FLB) takes hundreds of events to register.

## Solution

### Component 1: UnitizedRiskManager

**New file:** `data/unitized_risk.py`

Replaces `data/hwm_cache.py`. Implements NAV-per-share accounting — the institutional standard for deposit-adjusted performance tracking (validated by Gemini Deep Research, Prompt #5).

**Core concept:** The trading account is treated as a fund. Deposits mint new shares at current NAV. Withdrawals redeem shares. The NAV per share reflects pure trading performance, completely insulated from cash flows.

**Class: `UnitizedRiskManager`**

State:
- `shares` (Decimal) — total shares outstanding
- `hwm_nav` (Decimal) — highest NAV per share ever achieved  
- `nav` (Decimal) — current NAV per share

Methods:
- `__init__(state_file: str, initial_equity: Decimal)` — loads from JSON or initializes with `initial_equity / 1.00` = N shares at $1.00 NAV
- `process_cash_flow(amount: Decimal, current_equity: Decimal)` — mints shares (deposit) or redeems shares (withdrawal) at current NAV. Includes bankruptcy reset if equity <= $0.01 (prevents ZeroDivisionError)
- `update_from_trading(current_equity: Decimal)` — called each monitoring cycle. Recomputes NAV = equity / shares. Updates HWM if new peak.
- `get_drawdown() -> Decimal` — returns `(hwm_nav - nav) / hwm_nav`
- `get_kelly_multiplier() -> Decimal` — tiered circuit breakers:
  - DD >= 40% -> 0.00 (halt)
  - DD >= 25% -> 0.25
  - DD >= 15% -> 0.50
  - Otherwise -> 1.00
- `save_state()` / `load_state()` — atomic JSON persistence. All Decimal values serialized as strings to preserve precision.

**Arithmetic:** All calculations use `decimal.Decimal` with `getcontext().prec = 28`. No Python `float` anywhere in this module.

**State file:** `data/risk_state.json`

```json
{
    "shares": "100.000000",
    "hwm_nav": "1.370000",
    "nav": "1.370000"
}
```

**Bankruptcy reset:** If `current_equity <= 0.01`, reset shares to 0, NAV to 1.00, HWM to 1.00. Next deposit starts a fresh fund.

### Component 2: Integration — Full Removal of Old HWM Code

Every reference to the old absolute-dollar HWM system must be removed to prevent conflicts. Here is the complete inventory:

**`data/hwm_cache.py` — DELETE entirely:**
- `load_hwm()` and `save_hwm()` functions are fully replaced by `UnitizedRiskManager`
- File `data/hwm_state.json` becomes orphaned (can be left on disk, just never read)

**`execution/risk_gates.py` — remove all HWM internals:**
- Remove `self._high_water_mark` field from `__init__` (line 101)
- Remove `self._hwm_callback = None` (line 106)
- Remove `set_hwm_callback()` method (lines 108-110)
- Remove `update_after_trade()` method that updates `_high_water_mark` and calls `_hwm_callback` (lines 126-134)
- Remove tiered drawdown computation inside `check_all()` that uses `self._high_water_mark` (lines 153-175) — this logic moves to `UnitizedRiskManager.get_kelly_multiplier()`
- Remove `"high_water_mark"` from the summary dict (line 388)
- `RiskGates.__init__` signature changes: remove `persistent_hwm` and `starting_bankroll` parameters that fed the old HWM
- The drawdown gate in `check_all()` is replaced with a call to the UnitizedRiskManager passed in (or queried externally by main.py before calling check_all)

**`main.py` — remove all old HWM references:**
- Remove `from data.hwm_cache import load_hwm, save_hwm` (line 1080)
- Remove `persistent_hwm = load_hwm()` (line 1082)
- Remove `save_hwm(starting_portfolio)` calls (lines 1085, 1091)
- Remove `self._risk_gates.set_hwm_callback(save_hwm)` (line 1098)
- Remove the inline HWM update block (lines 843-846):
  ```python
  # DELETE THIS ENTIRE BLOCK:
  if self._risk_gates and portfolio_val > self._risk_gates._high_water_mark:
      self._risk_gates._high_water_mark = portfolio_val
      from data.hwm_cache import save_hwm
      save_hwm(portfolio_val)
  ```
- Replace with: `self._risk_mgr.update_from_trading(portfolio_val)`

**`main.py` — add new UnitizedRiskManager integration:**
- `from data.unitized_risk import UnitizedRiskManager`
- Initialize `self._risk_mgr = UnitizedRiskManager("data/risk_state.json", starting_portfolio)` in `EdgeRunner.__init__`
- Each monitoring cycle: `self._risk_mgr.update_from_trading(portfolio_value)`
- Before order execution: use `self._risk_mgr.get_kelly_multiplier()` for tiered drawdown scaling (replaces `gate_result.kelly_multiplier`)
- Deposit detection: compare broker balance vs expected. If unexplained delta > $1 and no recent trade, treat as deposit and call `process_cash_flow(delta, pre_deposit_equity)`

**`deploy/sync.sh` — add new file:**
- Add `data/unitized_risk.py` to the data module sync list
- Remove `data/hwm_cache.py` from the sync list (no longer needed on VPS)

**Migration path:** On first run, if `risk_state.json` doesn't exist, initialize with current portfolio value at $1.00 NAV. Old `hwm_state.json` is orphaned — never read again.

### Component 3: Bayesian Prior Strength Capping

**`data/bayesian_cache.py` changes:**

After each `update_outcome()` call, enforce a hard cap on the concentration parameter kappa = alpha + beta. Cap at 20 effective pseudo-observations.

```python
CAP_MAX = 20.0

def _cap_prior(alpha: float, beta: float) -> tuple[float, float]:
    kappa = alpha + beta
    if kappa > CAP_MAX:
        mu = alpha / kappa
        alpha = max(mu * CAP_MAX, 1.0)      # Floor at 1.0 prevents bathtub distribution
        beta = max((1.0 - mu) * CAP_MAX, 1.0)
    return alpha, beta
```

Applied immediately after the standard Bayesian update and event decay, before saving state. This ensures:
- No bucket retains more than ~20 effective observations of memory
- 2025 historical data sets the initial mean (mu) but can't dominate
- 5-10 new live results can meaningfully shift the posterior
- Bathtub distributions (alpha < 1 or beta < 1) are prevented

No other files change for this component.

## Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `data/unitized_risk.py` | **New** | UnitizedRiskManager class |
| `data/hwm_cache.py` | **Deleted** | Fully replaced by unitized_risk.py — all imports removed |
| `deploy/sync.sh` | Modified | Add unitized_risk.py, remove hwm_cache.py from sync |
| `execution/risk_gates.py` | Modified | Remove HWM tracking, delegate to UnitizedRiskManager |
| `main.py` | Modified | Initialize UnitizedRiskManager, deposit detection, Kelly multiplier source |
| `data/bayesian_cache.py` | Modified | Add `_cap_prior()` after each update |

## Testing

- Syntax check all modified files via `python -m py_compile`
- Verify UnitizedRiskManager math: deposit scenario from research (310 -> 55 -> deposit 100 -> NAV stays 0.55, DD stays 82.25%)
- Verify prior capping: initialize Beta(700, 300), cap should reduce to Beta(14, 6), preserving mean 0.70
- Deploy to VPS, verify agent starts and circuit breakers work correctly

## Success Criteria

1. Deposits no longer inflate HWM or trigger false circuit breakers
2. Bayesian posteriors respond to live data within 10-20 events, not 300+
3. Agent continues to trade normally — no regressions in signal generation or order execution
4. All state survives agent restarts via JSON persistence
