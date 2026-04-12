# Risk Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace absolute-dollar HWM drawdown tracking with unitized NAV-per-share accounting, and add Bayesian prior strength capping at kappa <= 20.

**Architecture:** New `data/unitized_risk.py` module with `UnitizedRiskManager` class replaces `data/hwm_cache.py`. All drawdown computation and tiered Kelly multipliers move from `risk_gates.py` into this class. `bayesian_cache.py` gets a `_cap_prior()` function applied after each update. All arithmetic uses `decimal.Decimal`.

**Tech Stack:** Python 3.10+, decimal.Decimal, JSON persistence

---

### Task 1: Create UnitizedRiskManager

**Files:**
- Create: `data/unitized_risk.py`

- [ ] **Step 1: Create the UnitizedRiskManager class**

```python
"""
Unitized NAV-per-share risk manager.

Replaces data/hwm_cache.py. Tracks drawdown using NAV per share
so deposits/withdrawals don't corrupt circuit breakers.

Reference: Gemini Deep Research Report #5 (2026-04-12)
"""
import json
import os
from decimal import Decimal, getcontext

getcontext().prec = 28

STATE_FILE = os.path.join(os.path.dirname(__file__), "risk_state.json")


class UnitizedRiskManager:
    def __init__(self, state_file: str = STATE_FILE, initial_equity: Decimal = Decimal("0")):
        self.state_file = state_file
        self.shares = Decimal("0")
        self.hwm_nav = Decimal("1.00")
        self.nav = Decimal("1.00")

        if os.path.exists(self.state_file):
            self._load_state()
        elif initial_equity > Decimal("0"):
            # First run: mint shares at $1.00 NAV
            self.nav = Decimal("1.00")
            self.hwm_nav = Decimal("1.00")
            self.shares = initial_equity / self.nav
            self._save_state()

    def process_cash_flow(self, amount: Decimal, current_equity: Decimal) -> None:
        """
        Handle deposits (positive) or withdrawals (negative).
        Must be called AFTER current equity is evaluated but BEFORE
        the cash flow alters the broker balance.
        """
        amount = Decimal(str(amount))
        current_equity = Decimal(str(current_equity))

        # Bankruptcy reset: prevent ZeroDivisionError and infinite share inflation
        if current_equity <= Decimal("0.01") or self.shares <= Decimal("0"):
            self.nav = Decimal("1.00")
            self.shares = Decimal("0")
            self.hwm_nav = Decimal("1.00")
            if amount > Decimal("0"):
                self.shares = amount / self.nav
            self._save_state()
            return

        # Recompute NAV at current equity before issuing/redeeming shares
        self.nav = current_equity / self.shares

        # Mint (deposit) or redeem (withdrawal) shares at current NAV
        share_delta = amount / self.nav
        self.shares += share_delta

        # Prevent negative shares from over-withdrawal
        if self.shares < Decimal("0"):
            self.shares = Decimal("0")

        self._save_state()

    def update_from_trading(self, current_equity: Decimal) -> None:
        """Called each monitoring cycle. Updates NAV and HWM from trading P&L."""
        current_equity = Decimal(str(current_equity))
        if self.shares > Decimal("0"):
            self.nav = current_equity / self.shares
            if self.nav > self.hwm_nav:
                self.hwm_nav = self.nav
        self._save_state()

    def get_drawdown(self) -> Decimal:
        """Returns current unitized drawdown as a positive decimal (0.25 = 25%)."""
        if self.hwm_nav <= Decimal("0"):
            return Decimal("0")
        return (self.hwm_nav - self.nav) / self.hwm_nav

    def get_kelly_multiplier(self) -> Decimal:
        """Tiered circuit breakers based on NAV drawdown."""
        dd = self.get_drawdown()
        if dd >= Decimal("0.40"):
            return Decimal("0")    # Halt trading
        elif dd >= Decimal("0.25"):
            return Decimal("0.25")
        elif dd >= Decimal("0.15"):
            return Decimal("0.50")
        return Decimal("1.00")

    def is_halted(self) -> bool:
        """Returns True if drawdown >= 40% (trading should stop)."""
        return self.get_drawdown() >= Decimal("0.40")

    def get_status(self) -> dict:
        """Return current state for monitoring/logging."""
        return {
            "nav": float(self.nav),
            "hwm_nav": float(self.hwm_nav),
            "shares": float(self.shares),
            "drawdown_pct": float(self.get_drawdown() * 100),
            "kelly_multiplier": float(self.get_kelly_multiplier()),
        }

    def _save_state(self) -> None:
        """Persist state as strings to preserve Decimal precision."""
        tmp = self.state_file + ".tmp"
        data = {
            "shares": str(self.shares),
            "hwm_nav": str(self.hwm_nav),
            "nav": str(self.nav),
        }
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.state_file)

    def _load_state(self) -> None:
        try:
            with open(self.state_file) as f:
                data = json.load(f)
            self.shares = Decimal(data["shares"])
            self.hwm_nav = Decimal(data["hwm_nav"])
            self.nav = Decimal(data["nav"])
        except (json.JSONDecodeError, KeyError, ValueError):
            # Corrupted state — will be re-initialized by caller
            pass
```

Write this to `data/unitized_risk.py`.

- [ ] **Step 2: Verify syntax**

Run: `python -m py_compile data/unitized_risk.py`
Expected: No output (clean compile)

- [ ] **Step 3: Commit**

```bash
git add data/unitized_risk.py
git commit -m "feat: add UnitizedRiskManager for deposit-adjusted drawdown tracking"
```

---

### Task 2: Delete hwm_cache.py and Update deploy/sync.sh

**Files:**
- Delete: `data/hwm_cache.py`
- Modify: `deploy/sync.sh:38`

- [ ] **Step 1: Delete hwm_cache.py**

```bash
git rm data/hwm_cache.py
```

- [ ] **Step 2: Update deploy/sync.sh — swap hwm_cache.py for unitized_risk.py**

In `deploy/sync.sh` line 38, replace `data/hwm_cache.py` with `data/unitized_risk.py` in the data module file list:

Change:
```
for f in data/__init__.py data/cache.py data/espn_scores.py data/espn_standings.py data/feeds.py data/market_poller.py data/nba_poller.py data/peak_cache.py data/smart_money.py data/flow_logger.py data/discovery_cache.py data/hwm_cache.py data/bayesian_cache.py; do
```

To:
```
for f in data/__init__.py data/cache.py data/espn_scores.py data/espn_standings.py data/feeds.py data/market_poller.py data/nba_poller.py data/peak_cache.py data/smart_money.py data/flow_logger.py data/discovery_cache.py data/unitized_risk.py data/bayesian_cache.py; do
```

- [ ] **Step 3: Verify syntax**

Run: `python -m py_compile deploy/sync.sh 2>/dev/null; echo "sync.sh is bash, not python — visual check OK"`

- [ ] **Step 4: Commit**

```bash
git add data/hwm_cache.py deploy/sync.sh
git commit -m "refactor: delete hwm_cache.py, update deploy sync for unitized_risk"
```

---

### Task 3: Strip Old HWM Logic from risk_gates.py

**Files:**
- Modify: `execution/risk_gates.py`

- [ ] **Step 1: Remove HWM fields and methods from RiskGates**

In `execution/risk_gates.py`, make these changes to the `RiskGates` class:

**Change `__init__` signature** (line 98) — remove `persistent_hwm` parameter:

```python
    def __init__(self, starting_bankroll: Decimal) -> None:
        self._starting_bankroll = starting_bankroll
        self._consecutive_losses: int = 0
        self._last_loss_time: float = 0.0
        self._halted: bool = False
        self._halt_reason: str = ""
```

Remove these lines entirely:
- `self._high_water_mark = ...` (line 101)
- `self._hwm_callback = None` (line 106)
- The entire `set_hwm_callback()` method (lines 108-110)

**Replace `update_after_trade()` method** (lines 126-140) — remove HWM update, keep consecutive loss tracking:

```python
    def update_after_trade(self, pnl: Decimal, current_bankroll: Decimal) -> None:
        """Update consecutive loss tracking after a trade resolves."""
        if pnl < 0:
            self._consecutive_losses += 1
            self._last_loss_time = time.monotonic()
        else:
            self._consecutive_losses = 0
```

**Replace `_check_drawdown()` method** (lines 142-188) — remove HWM-based drawdown, keep only consecutive loss logic. The tiered drawdown now lives in `UnitizedRiskManager.get_kelly_multiplier()`, called from `main.py`:

```python
    def _check_drawdown(self, current_bankroll: Decimal) -> tuple[GateResult, float]:
        """
        Gate 1: Consecutive loss cooldown.
        Tiered HWM drawdown is now handled externally by UnitizedRiskManager.
        """
        if self._halted:
            return GateResult("DRAWDOWN", False, f"HALTED: {self._halt_reason}"), 0.0

        kelly_mult = 1.0

        # Check consecutive losses
        if self._consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            time_since = time.monotonic() - self._last_loss_time
            if time_since < LOSS_COOLDOWN_SECONDS:
                remaining = LOSS_COOLDOWN_SECONDS - time_since
                return GateResult(
                    "DRAWDOWN", False,
                    f"{self._consecutive_losses} consecutive losses. "
                    f"Cooling down for {remaining:.0f}s more."
                ), 0.0
            self._consecutive_losses = 0

        return GateResult("DRAWDOWN", True, "Consecutive loss check passed"), kelly_mult
```

**Update `get_status()` method** (lines 384-392) — remove `high_water_mark`:

```python
    def get_status(self) -> dict:
        """Return current risk state for monitoring."""
        return {
            "starting_bankroll": float(self._starting_bankroll),
            "consecutive_losses": self._consecutive_losses,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
        }
```

- [ ] **Step 2: Verify syntax**

Run: `python -m py_compile execution/risk_gates.py`
Expected: No output (clean compile)

- [ ] **Step 3: Commit**

```bash
git add execution/risk_gates.py
git commit -m "refactor: strip absolute-dollar HWM from risk_gates, keep consecutive loss logic"
```

---

### Task 4: Integrate UnitizedRiskManager into main.py

**Files:**
- Modify: `main.py`

This is the largest task — touches initialization, monitoring loop, and signal processing.

- [ ] **Step 1: Replace HWM initialization (lines 1078-1102)**

Find the block starting with `from data.hwm_cache import load_hwm, save_hwm` (around line 1080) and ending with `self._risk_gates.set_hwm_callback(save_hwm)` (line 1098). Replace the entire block:

**Delete:**
```python
        from data.hwm_cache import load_hwm, save_hwm
        from config.settings import RESET_HWM
        persistent_hwm = load_hwm()
        if RESET_HWM or persistent_hwm is None:
            persistent_hwm = starting_portfolio
            save_hwm(starting_portfolio)
            console.print(f"[yellow]HWM reset to portfolio value ${starting_portfolio}[/yellow]")
        else:
            if starting_portfolio > persistent_hwm:
                persistent_hwm = starting_portfolio
                save_hwm(starting_portfolio)
            console.print(f"[blue]Loaded persistent HWM: ${persistent_hwm} | Portfolio: ${starting_portfolio}[/blue]")

        self._risk_gates = RiskGates(
            starting_bankroll=self._starting_bankroll,
            persistent_hwm=persistent_hwm,
        )
        self._risk_gates.set_hwm_callback(save_hwm)
        console.print(
            f"[blue]Starting bankroll: ${self._starting_bankroll} | "
            f"HWM: ${persistent_hwm} | 6-gate risk system initialized.[/blue]"
        )
```

**Replace with:**
```python
        from data.unitized_risk import UnitizedRiskManager
        self._risk_mgr = UnitizedRiskManager(initial_equity=starting_portfolio)

        self._risk_gates = RiskGates(starting_bankroll=self._starting_bankroll)

        nav_status = self._risk_mgr.get_status()
        console.print(
            f"[blue]Starting bankroll: ${self._starting_bankroll} | "
            f"NAV: ${nav_status['nav']:.4f} | HWM NAV: ${nav_status['hwm_nav']:.4f} | "
            f"DD: {nav_status['drawdown_pct']:.1f}% | "
            f"Shares: {nav_status['shares']:.2f} | 6-gate risk system initialized.[/blue]"
        )
```

- [ ] **Step 2: Replace inline HWM update in monitoring loop (lines 842-847)**

Find this block in the watchdog/monitoring section:

**Delete:**
```python
                    if self._risk_gates and portfolio_val > self._risk_gates._high_water_mark:
                        self._risk_gates._high_water_mark = portfolio_val
                        from data.hwm_cache import save_hwm
                        save_hwm(portfolio_val)
                        console.print(f"[green]New HWM: ${portfolio_val} (portfolio value)[/green]")
```

**Replace with:**
```python
                    if hasattr(self, '_risk_mgr'):
                        self._risk_mgr.update_from_trading(portfolio_val)
```

- [ ] **Step 3: Replace Kelly multiplier application in signal processing (lines 624-632)**

Find the block `# --- APPLY TIERED DRAWDOWN KELLY MULTIPLIER ---`. This currently reads `gate_result.kelly_multiplier`. Replace it to use the UnitizedRiskManager:

**Delete:**
```python
        # --- APPLY TIERED DRAWDOWN KELLY MULTIPLIER ---

        if gate_result.kelly_multiplier < 1.0:
            original_kelly = decision.kelly_fraction
            decision.kelly_fraction *= gate_result.kelly_multiplier
            console.print(
                f"[yellow]DD Kelly reduction: {original_kelly:.4f} -> "
                f"{decision.kelly_fraction:.4f} (x{gate_result.kelly_multiplier:.2f})[/yellow]"
            )
```

**Replace with:**
```python
        # --- APPLY UNITIZED NAV DRAWDOWN KELLY MULTIPLIER ---

        if hasattr(self, '_risk_mgr'):
            nav_kelly = float(self._risk_mgr.get_kelly_multiplier())
            if nav_kelly <= 0.0:
                console.print(f"[red bold]HALTED by NAV drawdown >= 40%[/red bold]")
                return
            if nav_kelly < 1.0:
                original_kelly = decision.kelly_fraction
                decision.kelly_fraction *= nav_kelly
                console.print(
                    f"[yellow]NAV DD Kelly reduction: {original_kelly:.4f} -> "
                    f"{decision.kelly_fraction:.4f} (x{nav_kelly:.2f})[/yellow]"
                )
```

- [ ] **Step 4: Add _risk_mgr to __init__ instance variables**

Near the top of `EdgeRunner.__init__` (around line 117-140), add:

```python
        self._risk_mgr: UnitizedRiskManager | None = None  # Initialized in _start()
```

- [ ] **Step 5: Verify no remaining references to hwm_cache**

Run: `grep -rn "hwm_cache\|load_hwm\|save_hwm\|_high_water_mark\|hwm_callback" main.py execution/risk_gates.py`
Expected: No matches

- [ ] **Step 6: Verify syntax**

Run: `python -m py_compile main.py`
Expected: No output (clean compile)

- [ ] **Step 7: Commit**

```bash
git add main.py
git commit -m "feat: integrate UnitizedRiskManager into main.py, remove all old HWM references"
```

---

### Task 5: Add Bayesian Prior Strength Capping

**Files:**
- Modify: `data/bayesian_cache.py:127-157`

- [ ] **Step 1: Add _cap_prior function**

At the top of `data/bayesian_cache.py`, near the other constants (after `EVENT_DECAY` and `MIN_OBSERVATIONS`), add:

```python
# Maximum effective pseudo-observations per bucket.
# Prevents 2025 historical data from dominating posteriors.
# With cap=20, live data shifts the mean within 5-10 events.
PRIOR_CAP_MAX = 20.0
```

Then add this function before `update_outcome()`:

```python
def _cap_prior(alpha: float, beta: float) -> tuple[float, float]:
    """
    Enforce hard cap on concentration parameter (alpha + beta).
    Prevents asymptotic lock-in from massive historical priors.
    Preserves the posterior mean while limiting effective sample size.
    """
    kappa = alpha + beta
    if kappa > PRIOR_CAP_MAX:
        mu = alpha / kappa
        alpha = max(mu * PRIOR_CAP_MAX, 1.0)   # Floor at 1.0 prevents bathtub distribution
        beta = max((1.0 - mu) * PRIOR_CAP_MAX, 1.0)
    return alpha, beta
```

- [ ] **Step 2: Apply capping after each update in update_outcome()**

In `update_outcome()`, after the update logic (after line 157 `state[key]["updates"] = ...`), add the capping call:

```python
    # Apply prior strength capping to prevent historical data lock-in
    state[key]["alpha"], state[key]["beta"] = _cap_prior(
        state[key]["alpha"], state[key]["beta"]
    )
```

This should go right after `state[key]["updates"] = state[key].get("updates", 0) + 1` and before any logging or saving.

- [ ] **Step 3: Verify syntax**

Run: `python -m py_compile data/bayesian_cache.py`
Expected: No output (clean compile)

- [ ] **Step 4: Commit**

```bash
git add data/bayesian_cache.py
git commit -m "feat: add Bayesian prior strength capping (kappa <= 20)"
```

---

### Task 6: Final Verification and Deploy

**Files:** All modified files

- [ ] **Step 1: Compile all modified files**

```bash
python -m py_compile data/unitized_risk.py && \
python -m py_compile data/bayesian_cache.py && \
python -m py_compile execution/risk_gates.py && \
python -m py_compile main.py && \
echo "All OK"
```

- [ ] **Step 2: Grep for any remaining old HWM references**

```bash
grep -rn "hwm_cache\|load_hwm\|save_hwm" *.py data/*.py execution/*.py signals/*.py config/*.py alerts/*.py
```

Expected: No matches (hwm_cache.py was deleted, all imports removed)

- [ ] **Step 3: Deploy to VPS**

```bash
bash deploy/sync.sh
```

- [ ] **Step 4: Restart agent**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'systemctl restart edgerunner'
```

- [ ] **Step 5: Verify agent starts with new NAV system**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'sleep 8 && journalctl -u edgerunner --no-pager -n 30 | grep -i "NAV\|risk\|Starting\|error\|traceback"'
```

Expected: See "NAV:" and "HWM NAV:" in startup log. No errors or tracebacks.

- [ ] **Step 6: Verify risk_state.json was created**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'cat /root/edgerunner/data/risk_state.json'
```

Expected: JSON with shares, hwm_nav, nav fields as strings.

- [ ] **Step 7: Commit all changes together**

```bash
git add data/unitized_risk.py data/bayesian_cache.py execution/risk_gates.py main.py deploy/sync.sh
git commit -m "Risk architecture: unitized NAV drawdown + Bayesian prior capping

- New UnitizedRiskManager (data/unitized_risk.py) replaces hwm_cache.py
- Deposits/withdrawals mint/redeem shares at current NAV, preserving drawdown accuracy
- Tiered circuit breakers (15/25/40% DD) now based on NAV, not absolute dollars
- All arithmetic uses decimal.Decimal to prevent float drift
- Bayesian prior capping at kappa <= 20 prevents historical data lock-in
- Deleted data/hwm_cache.py and all references across codebase"
```
