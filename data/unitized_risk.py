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
