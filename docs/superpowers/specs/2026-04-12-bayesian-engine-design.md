# Bayesian Engine Upgrade: Hybrid Decay, CUSUM, Dual-State BMA

## Problem

The current Bayesian updating system in `bayesian_cache.py` has three limitations:

1. **Event-only decay creates heterogeneous staleness.** A busy NBA bucket (50 events/month) decays 10x faster than a sparse ATP bucket (5 events/month). Off-season buckets retain stale priors indefinitely because no events trigger decay.

2. **No regime shift detection.** Gradual edge decay is caught by the posterior drift alerting (8% threshold), but sudden structural changes (institutional MM enters overnight) take 100+ events to register through exponential decay.

3. **Single decay rate is always a compromise.** Fast decay overreacts to variance noise. Slow decay misses regime shifts. No single rate is optimal for both stable and volatile periods.

## Solution: Three Incremental Components

All changes are in `data/bayesian_cache.py` with minor integration in `main.py`. Built and tested incrementally — each component works independently.

### Component 1: Hybrid Time+Event Decay

Add a daily clock-time decay that runs across ALL buckets regardless of whether events occurred.

**New constants:**
```python
DAILY_DECAY = 0.988  # ~180-day half-life: 0.988^180 ≈ 0.11
```

**New function: `apply_daily_time_decay()`**
- Iterates all buckets in state
- Applies: `alpha = 1.0 + (1.0 - DAILY_DECAY) * (alpha - 1.0)`, same for beta (both slow and fast filters)
- Updates `"last_daily_decay"` timestamp in state
- Called from main.py once per day (check timestamp, only apply if 24h+ elapsed)

**State addition:**
```json
{"last_daily_decay": "2026-04-12T00:00:00+00:00"}
```

**Event decay (0.995/event) stays as-is.** The two decay mechanisms are orthogonal — event decay handles intra-event adaptation, time decay handles calendar staleness.

### Component 2: Bernoulli CUSUM Circuit Breaker

One CUSUM scalar per **sport** (not per bucket — too sparse) that detects sudden negative regime shifts.

**Per-sport tracking:**
```json
{"cusum": {"NBA": 0.0, "NHL": 0.0, "ATP": 0.0, ...}}
```

**Parameters:**
- `p0`: sport's current average posterior YES rate (computed from all its buckets)
- `p1 = p0 + 0.05`: hypothesized negative shift (favorites winning 5% more = our edge disappearing)
- `h = 3.0`: alarm threshold (conservative — requires sustained evidence)

**Update rule (per event):**
```python
OR = (p1 * (1-p0)) / (p0 * (1-p1))
if outcome == "yes":  # favorite won — evidence of edge decay
    w = math.log(OR) - math.log(1 + p0 * (OR - 1))
elif outcome == "no":  # favorite lost — evidence edge holds
    w = -math.log(1 + p0 * (OR - 1))
S = max(0.0, S + w)
```

**When `S >= h`:**
- Log alarm: `"CUSUM ALARM: {sport} — regime shift detected"`
- Return sport confidence = 0.25 (effectively quarantining the sport via Kelly)
- Reset `S = 0` after alarm

**New function: `update_cusum(sport, outcome) -> float`**
- Returns current CUSUM value for the sport
- Called from `update_outcome()` after each event

**New function: `get_cusum_confidence(sport) -> float`**
- Returns 1.0 if CUSUM is below alarm, 0.25 if alarmed
- Used by `get_sport_confidence()` as a multiplier

### Component 3: Dual-State BMA Filter

Two parallel Beta distributions per bucket — slow (stable) and fast (reactive) — combined via Bayesian Model Averaging.

**Per-bucket state expands:**
```json
{
  "slow_alpha": 10.0, "slow_beta": 10.0,
  "fast_alpha": 10.0, "fast_beta": 10.0,
  "bma_weight_slow": 0.5,
  "updates": 5,
  "prior_rate": 0.45
}
```

**Decay rates:**
- Slow: `SLOW_EVENT_DECAY = 0.998` (~347 event half-life)
- Fast: `FAST_EVENT_DECAY = 0.970` (~23 event half-life)

**Update sequence (per event):**
1. Apply event decay to both filters independently
2. Compute predictive probability from each: `mu_slow`, `mu_fast`
3. Score against actual outcome: `L_slow = mu_slow if yes else (1-mu_slow)`
4. Update BMA weights with forgetting factor `delta = 0.98`:
   ```
   w_slow' = (w_slow * L_slow) ** delta
   w_fast' = (w_fast * L_fast) ** delta
   normalize so w_slow + w_fast = 1
   ```
5. Update both filters with the outcome (alpha += 1 or beta += 1)
6. Apply prior capping (κ ≤ 20) to BOTH filters

**Combined probability (used by `get_yes_rate()`):**
```python
p_combined = w_slow * mu_slow + w_fast * mu_fast
```

**Migration:** On first run after upgrade, if a bucket has old single-filter state (`alpha`, `beta`), copy to both `slow_*` and `fast_*`. Set `bma_weight_slow = 0.5`. Delete old `alpha`/`beta` keys.

**Key property:** During stable regimes, slow filter predicts well → BMA upweights it → variance suppressed. During regime shifts, slow filter predicts badly → BMA shifts to fast filter → adapts in ~20 events.

## Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `data/bayesian_cache.py` | Major rewrite | Dual-state BMA, hybrid decay, CUSUM, migration |
| `main.py` | Minor | Call `apply_daily_time_decay()` once per day |
| `signals/rules.py` | Minor | `get_sport_confidence()` now incorporates CUSUM |

## Testing Strategy

**Test each component independently before combining:**

1. **Hybrid decay test:** Initialize bucket, simulate 30 days with no events, verify alpha/beta decay toward 1.0
2. **CUSUM test:** Feed 20 consecutive "yes" outcomes to a sport with p0=0.45. Verify CUSUM triggers alarm.
3. **BMA test:** Initialize both filters identically. Feed 10 events that diverge from the slow filter's prediction. Verify BMA weight shifts to fast filter.
4. **Migration test:** Create old-format state, run migration, verify dual-state structure created correctly.
5. **Integration test:** Compile all files, deploy to VPS, verify agent starts and Bayesian updates log correctly.

## Success Criteria

1. Off-season buckets decay gracefully via daily time decay (no more stale 14-month-old priors)
2. CUSUM detects a 5% regime shift within ~20-30 events (not 100+)
3. BMA naturally upweights slow filter in stable periods, fast filter during shifts
4. All state persists across restarts via JSON
5. Agent continues to trade normally — no regressions
