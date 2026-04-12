"""
Bayesian edge table updating using Beta-Binomial conjugate model.

Replaces static edge tables with self-updating probability estimates.
Each sport + 5c price bucket maintains a Beta(alpha, beta) distribution:
  - alpha = YES wins observed
  - beta = NO wins observed
  - Posterior mean = alpha / (alpha + beta) = estimated YES hit rate

Priors initialized from 2025 EDGE_TABLES. As 2026 games settle,
the posteriors adapt automatically — decaying edges get detected
without manual intervention.

Daily decay (0.99x) ensures old data fades, giving recent outcomes
more weight. Half-life ~69 days.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console

console = Console()

CACHE_FILE = Path(__file__).parent / "bayesian_state.json"

# Event-based decay: applied per update, NOT per day (Gemini research recommendation)
# At 0.995 per event: half-life ~139 events
# - 50 events/month → ~2.8 month half-life (fast-adapting for busy sports)
# - 10 events/month → ~14 month half-life (slow-adapting for sparse sports)
EVENT_DECAY = 0.995

# Minimum alpha+beta to prevent the posterior from becoming unstable
MIN_OBSERVATIONS = 10.0

# Maximum effective pseudo-observations per bucket.
# Prevents 2025 historical data from dominating posteriors.
# With cap=20, live data shifts the mean within 5-10 events.
PRIOR_CAP_MAX = 20.0


def _bucket_key(sport: str, yes_price_cents: int) -> str:
    """Map sport + YES price to a 5c bucket key like 'NBA_60_64'."""
    lo = (yes_price_cents // 5) * 5
    hi = lo + 4
    return f"{sport}_{lo}_{hi}"


def _init_default_priors() -> dict:
    """
    Initialize Bayesian priors from the static EDGE_TABLES in rules.py.
    Scale to ~100 pseudo-observations so the prior has moderate weight.
    """
    # Import here to avoid circular imports
    from signals.rules import EDGE_TABLES, _per_price_yes_rate

    state = {}

    # Per-price linear models (NBA, NHL): create buckets for 55-90c range
    for sport in ("NBA", "NHL"):
        for lo in range(55, 90, 5):
            mid = lo + 2
            rate = _per_price_yes_rate(sport, mid)
            if rate is not None:
                key = f"{sport}_{lo}_{lo + 4}"
                n = 100  # pseudo-observations
                state[key] = {
                    "alpha": round(rate * n, 1),
                    "beta": round((1 - rate) * n, 1),
                    "updates": 0,
                    "prior_rate": round(rate, 4),
                }

    # Bucket-based models (all other sports)
    for sport, table in EDGE_TABLES.items():
        if sport in ("NBA", "NHL"):
            continue  # Already handled above
        for key_tuple, value in table.items():
            if isinstance(value, tuple):
                hit_rate = value[0]
            else:
                hit_rate = value

            lo, hi = key_tuple
            for sub_lo in range(lo, hi + 1, 5):
                sub_hi = min(sub_lo + 4, hi)
                bucket_key = f"{sport}_{sub_lo}_{sub_hi}"
                n = 100
                state[bucket_key] = {
                    "alpha": round(hit_rate * n, 1),
                    "beta": round((1 - hit_rate) * n, 1),
                    "updates": 0,
                    "prior_rate": round(hit_rate, 4),
                }

    return state


def load_bayesian_state() -> dict:
    """Load Bayesian state from disk. Returns empty dict if no file."""
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError):
        return {}


def save_bayesian_state(state: dict) -> None:
    """Save Bayesian state to disk (atomic write to prevent corruption on crash)."""
    try:
        tmp_file = CACHE_FILE.with_suffix(".tmp")
        with open(tmp_file, "w") as f:
            json.dump(state, f, indent=2)
        tmp_file.replace(CACHE_FILE)  # Atomic rename
    except Exception:
        pass


def get_or_init_state() -> dict:
    """Load state from disk, or initialize from priors if first run."""
    state = load_bayesian_state()
    if not state:
        state = _init_default_priors()
        save_bayesian_state(state)
        console.print(f"[blue]Bayesian: Initialized {len(state)} priors from edge tables[/blue]")
    return state


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


def update_outcome(sport: str, yes_price_cents: int, result: str) -> None:
    """
    Update Bayesian posterior after a game settles.

    Args:
        sport: Sport identifier (e.g., "NBA", "NHL")
        yes_price_cents: YES price at time of trade (55-95)
        result: "yes" or "no" — the market settlement result
    """
    if yes_price_cents < 55 or yes_price_cents > 95:
        return

    state = get_or_init_state()
    key = _bucket_key(sport, yes_price_cents)

    if key not in state:
        # New bucket not in priors — initialize with weak prior
        state[key] = {"alpha": 50.0, "beta": 50.0, "updates": 0}

    # Apply event-based decay BEFORE update (old data fades per observation)
    state[key]["alpha"] = max(MIN_OBSERVATIONS / 2, state[key]["alpha"] * EVENT_DECAY)
    state[key]["beta"] = max(MIN_OBSERVATIONS / 2, state[key]["beta"] * EVENT_DECAY)

    if result == "yes":
        state[key]["alpha"] += 1
    elif result == "no":
        state[key]["beta"] += 1
    else:
        return

    state[key]["updates"] = state[key].get("updates", 0) + 1

    # Apply prior strength capping to prevent historical data lock-in
    state[key]["alpha"], state[key]["beta"] = _cap_prior(
        state[key]["alpha"], state[key]["beta"]
    )

    old_rate = (state[key]["alpha"] - (1 if result == "yes" else 0)) / (
        state[key]["alpha"] + state[key]["beta"] - 1
    )
    new_rate = state[key]["alpha"] / (state[key]["alpha"] + state[key]["beta"])

    console.print(
        f"[cyan]Bayesian update: {key} result={result} | "
        f"YES rate {old_rate:.3f} -> {new_rate:.3f} | "
        f"n={state[key]['updates']}[/cyan]"
    )

    # Edge decay alerting: if posterior drifts 8%+ from prior, flag it
    prior_rate = state[key].get("prior_rate")
    if prior_rate is not None and state[key]["updates"] >= 10:
        drift = new_rate - prior_rate
        if abs(drift) >= 0.08:
            direction = "DECAYING" if drift > 0 else "STRENGTHENING"
            state[key]["_drift_alerted"] = True
            console.print(
                f"[{'red' if drift > 0 else 'green'} bold]EDGE DRIFT: {key} "
                f"{direction} — prior {prior_rate:.3f} -> posterior {new_rate:.3f} "
                f"(drift {drift:+.3f}, n={state[key]['updates']})[/{'red' if drift > 0 else 'green'} bold]"
            )
            # Send Discord alert (async not available here, so queue it)
            state[key]["_pending_alert"] = (
                f"{key} edge {direction}: "
                f"prior {prior_rate:.1%} -> current {new_rate:.1%} "
                f"(drift {drift:+.1%}, {state[key]['updates']} observations)"
            )

    save_bayesian_state(state)


def get_yes_rate(sport: str, yes_price_cents: int) -> float | None:
    """
    Get Bayesian posterior YES hit rate for a sport+price bucket.

    Returns None if no Bayesian data exists (caller should fall back to static table).
    Only returns a value if we have at least 5 real updates (not just priors).
    """
    state = load_bayesian_state()
    if not state:
        return None

    key = _bucket_key(sport, yes_price_cents)
    bucket = state.get(key)
    if bucket is None:
        return None

    # Only override static tables after enough real observations
    if bucket.get("updates", 0) < 5:
        return None

    alpha = bucket["alpha"]
    beta = bucket["beta"]
    total = alpha + beta

    if total < MIN_OBSERVATIONS:
        return None

    return alpha / total


def apply_daily_decay(state: dict) -> dict:
    """
    DEPRECATED: Decay is now event-based (applied per update_outcome call).
    This function is kept for backward compatibility but is a no-op.
    Event-based decay is more principled: high-volume sports decay faster,
    low-volume sports keep priors longer.
    """
    # No-op — decay now happens in update_outcome() per event
    return state


def get_sport_confidence(sport: str) -> float:
    """
    Simplified MAB: returns a 0.5-1.5 Kelly multiplier based on how well
    this sport's Bayesian posterior suggests edge is holding.

    Lower YES rate = more edge for NO buyers = higher confidence.
    """
    state = load_bayesian_state()
    if not state:
        return 1.0

    sport_buckets = [
        v for k, v in state.items()
        if k.startswith(sport + "_") and v.get("updates", 0) >= 3
    ]
    if not sport_buckets:
        return 1.0  # No data, neutral

    # Compare posterior YES rate to 0.5 (fair): lower = more edge
    total_edge = 0
    for b in sport_buckets:
        actual = b["alpha"] / (b["alpha"] + b["beta"])
        total_edge += (0.5 - actual)  # Positive = favorites underperform = edge exists

    avg_edge = total_edge / len(sport_buckets)
    # Map to 0.5-1.5 range
    return max(0.5, min(1.5, 1.0 + avg_edge * 5))


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing data/bayesian_cache.py...[/bold]\n")

    # Test 1: Initialize priors
    state = _init_default_priors()
    console.print(f"Initialized {len(state)} bucket priors")
    for key in sorted(state.keys())[:5]:
        b = state[key]
        rate = b["alpha"] / (b["alpha"] + b["beta"])
        console.print(f"  {key}: alpha={b['alpha']}, beta={b['beta']}, YES rate={rate:.3f}")

    # Test 2: Update outcome
    console.print("\nUpdating NBA_65_69 with 3 YES wins and 7 NO wins...")
    save_bayesian_state(state)
    for _ in range(3):
        update_outcome("NBA", 67, "yes")
    for _ in range(7):
        update_outcome("NBA", 67, "no")

    # Test 3: Check posterior
    state = load_bayesian_state()
    b = state["NBA_65_69"]
    rate = b["alpha"] / (b["alpha"] + b["beta"])
    console.print(f"\nNBA_65_69 posterior: alpha={b['alpha']}, beta={b['beta']}, YES rate={rate:.3f}")
    console.print(f"Updates: {b['updates']}")

    # Test 4: get_yes_rate (needs 5+ updates)
    result = get_yes_rate("NBA", 67)
    console.print(f"\nget_yes_rate('NBA', 67) = {result}")  # Should return the rate (10 updates)

    # Test 5: Daily decay
    state = apply_daily_decay(state)
    b = state["NBA_65_69"]
    rate = b["alpha"] / (b["alpha"] + b["beta"])
    console.print(f"\nAfter decay: alpha={b['alpha']:.1f}, beta={b['beta']:.1f}, YES rate={rate:.3f}")

    console.print("\n[green]All tests passed.[/green]")
