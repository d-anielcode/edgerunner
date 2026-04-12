# Critical Agent Fixes — Live Hotfix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 5 critical issues in the live EdgeRunner agent: mid-game re-entry churn, unresearched markets, correlated MLB Total exposure, 90c+ longshot entries, and WTA re-enablement.

**Architecture:** All changes are in existing files. No new files created. Changes to `signals/rules.py` (market filtering), `execution/position_monitor.py` (disable re-entry), `execution/risk_gates.py` (tighten per-game cap), `config/markets.py` (remove unresearched prefixes). Deploy via `deploy/sync.sh` and restart systemd service.

**Tech Stack:** Python 3.10+, Kalshi API, asyncio

---

### Task 1: Disable Re-Entry Logic

The re-entry system in `position_monitor.py` causes mid-game churn. The UCL RMA-BMU match on Apr 7 had 4 round-trips in 30 min, burning $8.43 in fees. The re-entry bypasses pre-game safety checks because it pushes orderbook updates directly to the cache.

**Files:**
- Modify: `execution/position_monitor.py:549-550` (disable call)
- Modify: `execution/position_monitor.py:478-486` (disable exit recording)

- [ ] **Step 1: Comment out re-entry call in `_check_cycle`**

In `execution/position_monitor.py` around line 549-550, comment out the re-entry check:

```python
        # 3. Check exited positions for re-entry opportunities
        # DISABLED: Re-entry bypasses pre-game safety checks and causes
        # mid-game churn with double fees (see UCL RMA-BMU 2026-04-07).
        # await self._check_reentry_opportunities()
```

- [ ] **Step 2: Comment out exit recording**

In `execution/position_monitor.py` around line 478-486, comment out the exit recording that feeds re-entry:

```python
        if result is not None:
            # DISABLED: Re-entry logic disabled — no need to track exits.
            # self._exited_positions[position.kalshi_ticker] = {
            #     "side": position.side,
            #     "entry_price": float(position.avg_price),
            #     "exit_price": float(current_price),
            #     "exit_reason": reason,
            #     "exit_time": time.monotonic(),
            # }
            pass
```

- [ ] **Step 3: Verify syntax**

Run: `python -m py_compile execution/position_monitor.py`
Expected: No output (clean compile)

---

### Task 2: Remove Unresearched Markets from SPORT_PARAMS

CBA, LOL, Ligue 1, and ATP Challenger have zero backtest validation or OOS data. They should not be traded with real money.

**Files:**
- Modify: `signals/rules.py:148-155` (remove edge tables)
- Modify: `signals/rules.py:206-209` (remove SPORT_PARAMS)
- Modify: `signals/rules.py:309` (remove from LOW_EDGE_SPORTS)

- [ ] **Step 1: Comment out unresearched edge tables**

In `signals/rules.py` around lines 148-155, comment out CBA, LIGUE1, LOL, ATPCH edge tables:

```python
    # DISABLED: No backtest validation or OOS data for these markets.
    # "MLBTOTAL": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    # "CBA":      {(55, 65): 0.500, (66, 75): 0.550, (76, 85): 0.620},
    # "LIGUE1":   {(55, 65): 0.480, (66, 75): 0.500, (76, 85): 0.550},
    # "LOL":      {(55, 65): 0.500, (66, 75): 0.520, (76, 85): 0.550},
    # "ATPCH":    {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
```

**Note:** MLBTOTAL is also disabled here pending Gemini research on whether FLB applies to over/under totals.

- [ ] **Step 2: Comment out corresponding SPORT_PARAMS**

In `signals/rules.py` around lines 203-209, comment out the matching SPORT_PARAMS:

```python
    # DISABLED: Unresearched markets removed from live trading.
    # "MLBTOTAL":{"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.05},
    # "CBA":     {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    # "LIGUE1":  {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    # "LOL":     {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    # "ATPCH":   {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
```

- [ ] **Step 3: Remove from LOW_EDGE_SPORTS tuple**

In `signals/rules.py` line 309, remove the disabled sports:

```python
        LOW_EDGE_SPORTS = ("WEATHER", "CPI", "NFLTD", "NFLGW", "NFLTT")
```

(Removed: MLBTOTAL, CBA, LIGUE1, LOL, ATPCH)

- [ ] **Step 4: Verify syntax**

Run: `python -m py_compile signals/rules.py`
Expected: No output (clean compile)

---

### Task 3: Re-Disable WTA

Research explicitly dropped WTA at -10% ROI. It was re-enabled with a comment "RE-ENABLED: 150% PT at 76-90c, Sharpe 0.183" but 0.183 Sharpe is very weak and there's no OOS validation.

**Files:**
- Modify: `signals/rules.py:200` (comment out WTA SPORT_PARAMS)

- [ ] **Step 1: Comment out WTA SPORT_PARAMS**

In `signals/rules.py` line 200:

```python
    # DISABLED: -10% ROI in research. 0.183 Sharpe insufficient for live trading.
    # "WTA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
```

Also comment out the WTA edge table entry (find `"WTA": EDGE_TABLE_WTA` in the EDGE_TABLES dict around line 146) — change to:

```python
    # "WTA": EDGE_TABLE_WTA,  # DISABLED: -10% ROI
```

- [ ] **Step 2: Verify syntax**

Run: `python -m py_compile signals/rules.py`
Expected: No output (clean compile)

---

### Task 4: Lower MAX_YES_PRICE for LOW_EDGE_SPORTS to 90c

The agent is buying NO at 94-95c YES prices (5-6c NO), which are lottery tickets with ~95% loss probability. The LOW_EDGE_SPORTS override sets max_price to 95c, which is too high.

**Files:**
- Modify: `signals/rules.py:311` (change max_price for remaining LOW_EDGE_SPORTS)

- [ ] **Step 1: Cap LOW_EDGE_SPORTS at 90c instead of 95c**

In `signals/rules.py` line 311:

```python
        max_price = Decimal("0.90") if sport in LOW_EDGE_SPORTS else MAX_YES_PRICE
```

Changed from `0.95` to `0.90`. This blocks entries where NO costs less than 10c (lottery tickets).

- [ ] **Step 2: Verify syntax**

Run: `python -m py_compile signals/rules.py`
Expected: No output (clean compile)

---

### Task 5: Tighten Per-Game Position Limit

`MAX_PER_GAME = 3` is too high for over/under totals where each line is correlated. Also, batch execution places multiple orders within seconds, before any register in the position check.

**Files:**
- Modify: `execution/risk_gates.py:50` (reduce MAX_PER_GAME)
- Modify: `main.py:563-580` (add batch-aware duplicate check)

- [ ] **Step 1: Reduce MAX_PER_GAME from 3 to 2**

In `execution/risk_gates.py` line 50:

```python
MAX_PER_GAME: int = 2
```

- [ ] **Step 2: Add pending-order tracking to prevent batch race condition**

In `main.py`, find the section around line 563-580 where `_extract_game_id` and correlated positions are checked. Add tracking of orders placed in the current cycle. Find the `_place_order` or signal processing method and add a set to track game IDs of orders placed this cycle:

In `__init__` (around line 117), add:

```python
        self._pending_game_ids: set[str] = set()  # Track game IDs of orders placed this cycle
```

In the signal processing section (around line 563), before the existing correlated check, add:

```python
        # Check pending orders from THIS cycle (race condition prevention)
        if game_id and game_id in self._pending_game_ids:
            console.print(f"[yellow]SKIP {ticker}: already placed order on game {game_id} this cycle.[/yellow]")
            return
```

After a successful order placement (find the line where the order succeeds), add:

```python
        if game_id:
            self._pending_game_ids.add(game_id)
```

At the start of each polling cycle (find the main loop iteration), clear it:

```python
        self._pending_game_ids.clear()
```

- [ ] **Step 3: Verify syntax**

Run: `python -m py_compile main.py && python -m py_compile execution/risk_gates.py`
Expected: No output (clean compile)

---

### Task 6: Deploy and Restart

- [ ] **Step 1: Syntax-check all modified files**

```bash
python -m py_compile signals/rules.py && \
python -m py_compile execution/position_monitor.py && \
python -m py_compile execution/risk_gates.py && \
python -m py_compile main.py && \
echo "All OK"
```

- [ ] **Step 2: Deploy to VPS**

```bash
bash deploy/sync.sh
```

- [ ] **Step 3: Restart the agent**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'systemctl restart edgerunner'
```

- [ ] **Step 4: Verify agent starts cleanly**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'sleep 5 && journalctl -u edgerunner --no-pager -n 20'
```

Expected: Agent starts, discovers markets, no import errors or crashes.

- [ ] **Step 5: Verify disabled markets are not being polled**

```bash
ssh -i ~/.ssh/digitalocean_edgerunner root@159.65.177.244 'sleep 30 && journalctl -u edgerunner --no-pager -n 100 | grep -i "CBA\|LOL\|LIGUE\|ATPCH\|MLBTOTAL\|WTA"'
```

Expected: No matches (these markets should not appear in logs).

- [ ] **Step 6: Commit changes**

```bash
git add signals/rules.py execution/position_monitor.py execution/risk_gates.py main.py
git commit -m "Hotfix: disable re-entry, remove unresearched markets, cap per-game positions

- Disable mid-game re-entry logic (caused $8+ fee churn on single games)
- Remove CBA, LOL, Ligue 1, ATP Challenger, MLB Total from live trading (no OOS validation)
- Re-disable WTA (-10% ROI in research)
- Cap LOW_EDGE_SPORTS max YES at 90c (was 95c — blocked 5c lottery ticket NO entries)
- Reduce MAX_PER_GAME from 3 to 2
- Add pending_game_ids tracking to prevent batch race condition on correlated positions"
```

---

### Existing Positions Note

The 25 currently open positions (including CBA, WTA, MLB Total) will continue to be monitored and will settle normally. The position monitor still tracks them. They just won't generate new entries. No manual intervention needed — they'll resolve on their own.
