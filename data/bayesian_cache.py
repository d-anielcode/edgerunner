"""
Bayesian edge table updating using dual-state Beta-Binomial conjugate model.

Each sport + 5c price bucket maintains TWO parallel Beta distributions:
  - Slow filter (high inertia): tracks long-term mean, suppresses noise
  - Fast filter (low inertia): reacts quickly to regime shifts

Combined via Bayesian Model Averaging (BMA) with forgetting factor.

Additionally:
  - Hybrid decay: event-based + daily clock-time (handles off-season staleness)
  - CUSUM circuit breaker per sport: detects sudden regime shifts
  - Prior strength capping (kappa <= 20): prevents historical lock-in

Reference: Gemini Deep Research Report #1 (2026-04-12)
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console

console = Console()

CACHE_FILE = Path(__file__).parent / "bayesian_state.json"

# --- Decay Parameters ---

# Event-based decay (applied per update)
SLOW_EVENT_DECAY = 0.998   # ~347 event half-life (long-term trend)
FAST_EVENT_DECAY = 0.970   # ~23 event half-life (reactive to recent data)

# Daily clock-time decay (applied once per day across all buckets)
# 0.988^180 ≈ 0.11 → off-season buckets lose 89% of prior over 6 months
DAILY_DECAY = 0.988

# --- Capping and Floors ---

# Maximum effective pseudo-observations per filter per bucket
PRIOR_CAP_MAX = 20.0

# Minimum alpha+beta to prevent unstable posteriors
MIN_OBSERVATIONS = 4.0

# Minimum real updates before Bayesian overrides static tables
MIN_UPDATES_FOR_OVERRIDE = 5

# --- CUSUM Parameters ---

CUSUM_SHIFT = 0.05     # Detect a 5% worsening in YES rate
CUSUM_THRESHOLD = 3.0  # Alarm threshold (conservative)
CUSUM_ALARM_KELLY = 0.25  # Kelly multiplier when alarmed

# --- BMA Parameters ---

BMA_FORGETTING_FACTOR = 0.98  # Prevents weight lockout


def _bucket_key(sport: str, yes_price_cents: int) -> str:
    """Map sport + YES price to a 5c bucket key like 'NBA_60_64'."""
    lo = (yes_price_cents // 5) * 5
    hi = lo + 4
    return f"{sport}_{lo}_{hi}"


def _cap_prior(alpha: float, beta: float) -> tuple[float, float]:
    """
    Enforce hard cap on concentration parameter (alpha + beta).
    Preserves the posterior mean while limiting effective sample size.
    """
    kappa = alpha + beta
    if kappa > PRIOR_CAP_MAX:
        mu = alpha / kappa
        alpha = max(mu * PRIOR_CAP_MAX, 1.0)
        beta = max((1.0 - mu) * PRIOR_CAP_MAX, 1.0)
    return alpha, beta


def _apply_event_decay(alpha: float, beta: float, decay: float) -> tuple[float, float]:
    """Apply exponential decay: shrink toward uniform prior (1, 1)."""
    alpha = 1.0 + decay * (alpha - 1.0)
    beta = 1.0 + decay * (beta - 1.0)
    return max(alpha, MIN_OBSERVATIONS / 2), max(beta, MIN_OBSERVATIONS / 2)


# =============================================================================
# State Management
# =============================================================================

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
    """Save Bayesian state to disk (atomic write to prevent corruption)."""
    try:
        tmp_file = CACHE_FILE.with_suffix(".tmp")
        with open(tmp_file, "w") as f:
            json.dump(state, f, indent=2)
        tmp_file.replace(CACHE_FILE)
    except Exception:
        pass


def _init_default_priors() -> dict:
    """
    Initialize Bayesian priors from the static EDGE_TABLES in rules.py.
    Creates dual-state (slow + fast) format from the start.
    """
    from signals.rules import EDGE_TABLES, _per_price_yes_rate

    state = {}

    # Per-price linear models (NBA, NHL)
    for sport in ("NBA", "NHL"):
        for lo in range(55, 90, 5):
            mid = lo + 2
            rate = _per_price_yes_rate(sport, mid)
            if rate is not None:
                key = f"{sport}_{lo}_{lo + 4}"
                n = min(PRIOR_CAP_MAX, 15.0)  # Start within cap
                state[key] = _make_dual_bucket(rate, n)

    # Bucket-based models (all other sports)
    for sport, table in EDGE_TABLES.items():
        if sport in ("NBA", "NHL"):
            continue
        for key_tuple, value in table.items():
            hit_rate = value[0] if isinstance(value, tuple) else value
            lo, hi = key_tuple
            for sub_lo in range(lo, hi + 1, 5):
                sub_hi = min(sub_lo + 4, hi)
                bucket_key = f"{sport}_{sub_lo}_{sub_hi}"
                n = min(PRIOR_CAP_MAX, 15.0)
                state[bucket_key] = _make_dual_bucket(hit_rate, n)

    # Initialize metadata
    state["_meta"] = {
        "last_daily_decay": datetime.now(timezone.utc).isoformat(),
        "cusum": {},
    }

    return state


def _make_dual_bucket(hit_rate: float, n: float) -> dict:
    """Create a dual-state bucket from a hit rate and pseudo-observation count."""
    alpha = round(hit_rate * n, 2)
    beta = round((1.0 - hit_rate) * n, 2)
    return {
        "slow_alpha": alpha,
        "slow_beta": beta,
        "fast_alpha": alpha,
        "fast_beta": beta,
        "bma_weight_slow": 0.5,
        "updates": 0,
        "prior_rate": round(hit_rate, 4),
    }


def _migrate_bucket(bucket: dict) -> dict:
    """Migrate old single-filter bucket to dual-state format."""
    if "slow_alpha" in bucket:
        return bucket  # Already migrated

    alpha = bucket.get("alpha", 10.0)
    beta = bucket.get("beta", 10.0)
    # Cap the migrated values
    alpha, beta = _cap_prior(alpha, beta)

    return {
        "slow_alpha": alpha,
        "slow_beta": beta,
        "fast_alpha": alpha,
        "fast_beta": beta,
        "bma_weight_slow": 0.5,
        "updates": bucket.get("updates", 0),
        "prior_rate": bucket.get("prior_rate"),
    }


def get_or_init_state() -> dict:
    """Load state from disk, or initialize from priors if first run."""
    state = load_bayesian_state()
    if not state:
        state = _init_default_priors()
        save_bayesian_state(state)
        console.print(f"[blue]Bayesian: Initialized {len(state) - 1} dual-state priors from edge tables[/blue]")
        return state

    # Ensure _meta exists
    if "_meta" not in state:
        state["_meta"] = {
            "last_daily_decay": datetime.now(timezone.utc).isoformat(),
            "cusum": {},
        }

    # Migrate any old single-filter buckets
    migrated = 0
    for key in list(state.keys()):
        if key.startswith("_"):
            continue
        if "slow_alpha" not in state[key]:
            state[key] = _migrate_bucket(state[key])
            migrated += 1

    if migrated > 0:
        console.print(f"[yellow]Bayesian: Migrated {migrated} buckets to dual-state format[/yellow]")
        save_bayesian_state(state)

    return state


# =============================================================================
# Core Update Logic
# =============================================================================

def update_outcome(sport: str, yes_price_cents: int, result: str) -> None:
    """
    Update Bayesian posteriors after a game settles.

    Updates both slow and fast filters, applies BMA weight update,
    event decay, prior capping, and CUSUM tracking.
    """
    if yes_price_cents < 55 or yes_price_cents > 95:
        return

    state = get_or_init_state()
    key = _bucket_key(sport, yes_price_cents)

    if key not in state:
        state[key] = _make_dual_bucket(0.50, 10.0)

    bucket = state[key]

    # --- Step 1: Apply event decay to BOTH filters ---
    bucket["slow_alpha"], bucket["slow_beta"] = _apply_event_decay(
        bucket["slow_alpha"], bucket["slow_beta"], SLOW_EVENT_DECAY
    )
    bucket["fast_alpha"], bucket["fast_beta"] = _apply_event_decay(
        bucket["fast_alpha"], bucket["fast_beta"], FAST_EVENT_DECAY
    )

    # --- Step 2: BMA weight update (BEFORE incorporating new outcome) ---
    mu_slow = bucket["slow_alpha"] / (bucket["slow_alpha"] + bucket["slow_beta"])
    mu_fast = bucket["fast_alpha"] / (bucket["fast_alpha"] + bucket["fast_beta"])

    outcome_val = 1.0 if result == "yes" else 0.0 if result == "no" else None
    if outcome_val is None:
        return

    # Predictive likelihood of the observed outcome under each model
    l_slow = mu_slow if outcome_val == 1.0 else (1.0 - mu_slow)
    l_fast = mu_fast if outcome_val == 1.0 else (1.0 - mu_fast)

    # Clamp to avoid log(0)
    l_slow = max(l_slow, 1e-6)
    l_fast = max(l_fast, 1e-6)

    # BMA weight update with forgetting factor
    w_slow = bucket.get("bma_weight_slow", 0.5)
    w_fast = 1.0 - w_slow

    delta = BMA_FORGETTING_FACTOR
    w_slow_new = (w_slow * l_slow) ** delta
    w_fast_new = (w_fast * l_fast) ** delta

    total_w = w_slow_new + w_fast_new
    if total_w > 0:
        bucket["bma_weight_slow"] = round(w_slow_new / total_w, 6)
    else:
        bucket["bma_weight_slow"] = 0.5

    # --- Step 3: Update both filters with the outcome ---
    if result == "yes":
        bucket["slow_alpha"] += 1
        bucket["fast_alpha"] += 1
    elif result == "no":
        bucket["slow_beta"] += 1
        bucket["fast_beta"] += 1

    bucket["updates"] = bucket.get("updates", 0) + 1

    # --- Step 4: Apply prior capping to BOTH filters ---
    bucket["slow_alpha"], bucket["slow_beta"] = _cap_prior(
        bucket["slow_alpha"], bucket["slow_beta"]
    )
    bucket["fast_alpha"], bucket["fast_beta"] = _cap_prior(
        bucket["fast_alpha"], bucket["fast_beta"]
    )

    # --- Step 5: CUSUM update ---
    _update_cusum(state, sport, result)

    # --- Logging ---
    p_combined = _get_combined_rate(bucket)
    w_s = bucket["bma_weight_slow"]

    console.print(
        f"[cyan]Bayesian: {key} result={result} | "
        f"combined={p_combined:.3f} | "
        f"slow={mu_slow:.3f} fast={mu_fast:.3f} | "
        f"BMA w_slow={w_s:.2f} | "
        f"n={bucket['updates']}[/cyan]"
    )

    # Edge drift alerting
    prior_rate = bucket.get("prior_rate")
    if prior_rate is not None and bucket["updates"] >= 10:
        drift = p_combined - prior_rate
        if abs(drift) >= 0.08:
            direction = "DECAYING" if drift > 0 else "STRENGTHENING"
            console.print(
                f"[{'red' if drift > 0 else 'green'} bold]EDGE DRIFT: {key} "
                f"{direction} — prior {prior_rate:.3f} -> combined {p_combined:.3f} "
                f"(drift {drift:+.3f}, n={bucket['updates']})[/{'red' if drift > 0 else 'green'} bold]"
            )

    save_bayesian_state(state)


def _get_combined_rate(bucket: dict) -> float:
    """Get BMA-combined YES rate from a dual-state bucket."""
    mu_slow = bucket["slow_alpha"] / (bucket["slow_alpha"] + bucket["slow_beta"])
    mu_fast = bucket["fast_alpha"] / (bucket["fast_alpha"] + bucket["fast_beta"])
    w_slow = bucket.get("bma_weight_slow", 0.5)
    return w_slow * mu_slow + (1.0 - w_slow) * mu_fast


# =============================================================================
# CUSUM Circuit Breaker
# =============================================================================

def _update_cusum(state: dict, sport: str, result: str) -> None:
    """Update Bernoulli CUSUM statistic for a sport."""
    meta = state.get("_meta", {})
    cusum_state = meta.get("cusum", {})

    # Compute p0: sport's INITIAL prior YES rate (anchored, not chasing posterior)
    # Using prior_rate prevents CUSUM from being neutralized as the posterior shifts
    sport_buckets = [
        v for k, v in state.items()
        if k.startswith(sport + "_") and not k.startswith("_")
        and isinstance(v, dict) and v.get("prior_rate") is not None
    ]
    if not sport_buckets:
        return

    avg_prior = sum(b["prior_rate"] for b in sport_buckets) / len(sport_buckets)
    p0 = max(0.05, min(0.95, avg_prior))  # Anchored to initial calibration
    p1 = min(0.95, p0 + CUSUM_SHIFT)    # Hypothesized negative shift

    # Avoid degenerate cases
    if abs(p1 - p0) < 0.001:
        return

    # Compute log-likelihood ratio weight
    odds_ratio = (p1 * (1.0 - p0)) / (p0 * (1.0 - p1))
    if odds_ratio <= 0:
        return

    log_or = math.log(odds_ratio)
    log_denom = math.log(1.0 + p0 * (odds_ratio - 1.0))

    if result == "yes":  # Favorite won — evidence of edge decay
        w = log_or - log_denom
    elif result == "no":  # Favorite lost — evidence edge holds
        w = -log_denom
    else:
        return

    # Update cumulative sum
    s = cusum_state.get(sport, 0.0)
    s = max(0.0, s + w)

    # Check alarm
    if s >= CUSUM_THRESHOLD:
        console.print(
            f"[red bold]CUSUM ALARM: {sport} — regime shift detected! "
            f"S={s:.2f} >= {CUSUM_THRESHOLD}. Kelly reduced to {CUSUM_ALARM_KELLY:.0%}.[/red bold]"
        )
        cusum_state[sport] = 0.0  # Reset after alarm
        cusum_state[f"{sport}_alarmed"] = True
    else:
        cusum_state[sport] = round(s, 4)

    meta["cusum"] = cusum_state
    state["_meta"] = meta


def get_cusum_confidence(sport: str) -> float:
    """
    Returns 1.0 if CUSUM is normal, CUSUM_ALARM_KELLY if alarmed.
    Alarm auto-clears after 50 events (checked in update_outcome).
    """
    state = load_bayesian_state()
    meta = state.get("_meta", {})
    cusum = meta.get("cusum", {})

    if cusum.get(f"{sport}_alarmed", False):
        # Auto-clear alarm after enough new data
        sport_buckets = [
            v for k, v in state.items()
            if k.startswith(sport + "_") and not k.startswith("_")
            and isinstance(v, dict)
        ]
        recent_updates = sum(b.get("updates", 0) for b in sport_buckets)
        if recent_updates > 50:
            # Clear alarm, let CUSUM re-evaluate
            cusum[f"{sport}_alarmed"] = False
            meta["cusum"] = cusum
            state["_meta"] = meta
            save_bayesian_state(state)
            return 1.0
        return CUSUM_ALARM_KELLY

    return 1.0


# =============================================================================
# Daily Time Decay
# =============================================================================

def apply_daily_time_decay() -> None:
    """
    Apply clock-time decay to ALL buckets. Call once per day from main.py.
    Ensures off-season buckets don't retain stale priors indefinitely.
    """
    state = get_or_init_state()
    meta = state.get("_meta", {})

    last_decay_str = meta.get("last_daily_decay")
    if last_decay_str:
        try:
            last_decay = datetime.fromisoformat(last_decay_str)
            hours_since = (datetime.now(timezone.utc) - last_decay).total_seconds() / 3600
            if hours_since < 23.0:  # Less than ~1 day
                return  # Already decayed today
        except (ValueError, TypeError):
            pass

    decayed_count = 0
    for key, bucket in state.items():
        if key.startswith("_"):
            continue
        if not isinstance(bucket, dict):
            continue

        # Apply daily decay to both filters
        for prefix in ("slow_", "fast_"):
            a_key = f"{prefix}alpha"
            b_key = f"{prefix}beta"
            if a_key in bucket and b_key in bucket:
                bucket[a_key] = 1.0 + DAILY_DECAY * (bucket[a_key] - 1.0)
                bucket[b_key] = 1.0 + DAILY_DECAY * (bucket[b_key] - 1.0)
                bucket[a_key] = max(bucket[a_key], MIN_OBSERVATIONS / 2)
                bucket[b_key] = max(bucket[b_key], MIN_OBSERVATIONS / 2)

        decayed_count += 1

    meta["last_daily_decay"] = datetime.now(timezone.utc).isoformat()
    state["_meta"] = meta
    save_bayesian_state(state)

    console.print(
        f"[blue]Bayesian daily decay: applied to {decayed_count} buckets "
        f"(factor={DAILY_DECAY})[/blue]"
    )


# =============================================================================
# Public Query API
# =============================================================================

def get_yes_rate(sport: str, yes_price_cents: int) -> float | None:
    """
    Get BMA-combined posterior YES hit rate for a sport+price bucket.

    Returns None if insufficient data (caller falls back to static table).
    """
    state = load_bayesian_state()
    if not state:
        return None

    key = _bucket_key(sport, yes_price_cents)
    bucket = state.get(key)
    if bucket is None:
        return None

    # Only override static tables after enough real observations
    if bucket.get("updates", 0) < MIN_UPDATES_FOR_OVERRIDE:
        return None

    # Handle old single-filter format gracefully
    if "slow_alpha" not in bucket:
        alpha = bucket.get("alpha", 10.0)
        beta = bucket.get("beta", 10.0)
        total = alpha + beta
        if total < MIN_OBSERVATIONS:
            return None
        return alpha / total

    return _get_combined_rate(bucket)


def get_sport_confidence(sport: str) -> float:
    """
    Returns a 0.5-1.5 Kelly multiplier based on sport's Bayesian health.

    Combines:
    - Edge strength (lower YES rate = more edge for NO buyers)
    - CUSUM alarm status (0.25x if regime shift detected)
    """
    state = load_bayesian_state()
    if not state:
        return 1.0

    sport_buckets = [
        v for k, v in state.items()
        if k.startswith(sport + "_") and not k.startswith("_")
        and isinstance(v, dict) and v.get("updates", 0) >= 3
    ]
    if not sport_buckets:
        return 1.0

    # Edge strength from BMA-combined rates
    total_edge = 0
    for b in sport_buckets:
        if "slow_alpha" in b:
            actual = _get_combined_rate(b)
        else:
            actual = b["alpha"] / (b["alpha"] + b["beta"])
        total_edge += (0.5 - actual)

    avg_edge = total_edge / len(sport_buckets)
    edge_confidence = max(0.5, min(1.5, 1.0 + avg_edge * 5))

    # CUSUM alarm multiplier
    cusum_mult = get_cusum_confidence(sport)

    return edge_confidence * cusum_mult


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing data/bayesian_cache.py (dual-state BMA)...[/bold]\n")

    # Test 1: Initialize priors
    console.print("[cyan]Test 1: Initialize dual-state priors[/cyan]")
    state = _init_default_priors()
    bucket_count = sum(1 for k in state if not k.startswith("_"))
    console.print(f"  Initialized {bucket_count} bucket priors")
    for key in sorted(k for k in state.keys() if not k.startswith("_"))[:3]:
        b = state[key]
        rate = _get_combined_rate(b)
        console.print(
            f"  {key}: slow=({b['slow_alpha']:.1f},{b['slow_beta']:.1f}) "
            f"fast=({b['fast_alpha']:.1f},{b['fast_beta']:.1f}) "
            f"combined={rate:.3f} w_slow={b['bma_weight_slow']:.2f}"
        )

    # Test 2: BMA weight shift under regime change
    console.print("\n[cyan]Test 2: BMA weight shift (10 consecutive YES = regime shift)[/cyan]")
    save_bayesian_state(state)
    key = "NHL_70_74"
    b_before = state.get(key, {})
    w_before = b_before.get("bma_weight_slow", 0.5)
    console.print(f"  Before: w_slow={w_before:.4f}")

    for i in range(10):
        update_outcome("NHL", 72, "yes")

    state = load_bayesian_state()
    b_after = state.get(key, {})
    w_after = b_after.get("bma_weight_slow", 0.5)
    console.print(f"  After 10 YES: w_slow={w_after:.4f}")
    if w_after != w_before:
        console.print(f"  [green]BMA weights shifted (slow {'gained' if w_after > w_before else 'lost'} weight)[/green]")
    else:
        console.print(f"  [red]BMA weights did NOT shift — check logic[/red]")

    # Test 3: Prior capping
    console.print("\n[cyan]Test 3: Prior capping (kappa <= 20)[/cyan]")
    a, b = _cap_prior(700.0, 300.0)
    console.print(f"  Beta(700, 300) -> Beta({a:.1f}, {b:.1f})")
    console.print(f"  Mean preserved: {700/1000:.3f} -> {a/(a+b):.3f}")
    assert abs(a + b - PRIOR_CAP_MAX) < 1.0, f"Capping failed: kappa={a+b}"

    # Test 4: CUSUM alarm
    console.print("\n[cyan]Test 4: CUSUM alarm (30 consecutive YES on NBA)[/cyan]")
    state = _init_default_priors()
    save_bayesian_state(state)
    for i in range(30):
        update_outcome("NBA", 72, "yes")

    state = load_bayesian_state()
    meta = state.get("_meta", {})
    cusum = meta.get("cusum", {})
    nba_s = cusum.get("NBA", 0)
    nba_alarmed = cusum.get("NBA_alarmed", False)
    console.print(f"  CUSUM S={nba_s:.2f}, alarmed={nba_alarmed}")
    conf = get_cusum_confidence("NBA")
    console.print(f"  get_cusum_confidence('NBA') = {conf}")

    # Test 5: Daily time decay
    console.print("\n[cyan]Test 5: Daily time decay[/cyan]")
    state = load_bayesian_state()
    meta = state.get("_meta", {})
    # Force last_daily_decay to 2 days ago
    from datetime import timedelta
    meta["last_daily_decay"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    state["_meta"] = meta
    save_bayesian_state(state)
    apply_daily_time_decay()
    console.print("  [green]Daily decay applied successfully[/green]")

    # Test 6: Migration from old format
    console.print("\n[cyan]Test 6: Migration from old single-filter format[/cyan]")
    old_bucket = {"alpha": 45.0, "beta": 55.0, "updates": 10, "prior_rate": 0.45}
    migrated = _migrate_bucket(old_bucket)
    console.print(f"  Old: alpha={old_bucket['alpha']}, beta={old_bucket['beta']}")
    console.print(
        f"  New: slow=({migrated['slow_alpha']:.1f},{migrated['slow_beta']:.1f}) "
        f"fast=({migrated['fast_alpha']:.1f},{migrated['fast_beta']:.1f}) "
        f"w_slow={migrated['bma_weight_slow']}"
    )

    # Test 7: get_sport_confidence
    console.print("\n[cyan]Test 7: Sport confidence (MAB + CUSUM)[/cyan]")
    for sport in ("NBA", "NHL", "EPL", "ATP"):
        conf = get_sport_confidence(sport)
        console.print(f"  {sport}: confidence={conf:.3f}")

    console.print("\n[green bold]All tests passed.[/green bold]")
