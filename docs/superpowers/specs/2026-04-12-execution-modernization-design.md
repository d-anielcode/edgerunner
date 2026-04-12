# Execution Modernization: Min-Edge, Fractional Trading, API v2 Fields

## Problem

Three execution-layer issues are costing the agent money:

1. **min_edge floors are below break-even.** Gemini execution realism research (Prompt #3) calculated break-even edge at ~10.5% for a $0.30 contract after spread + ceil() fees + slippage. Six sports have min_edge at 0.05-0.08 — every trade on these is negative EV.

2. **$1.00 minimum bet floor rejects valid trades.** Kalshi launched fractional trading in March 2026 with `count_fp` fields accepting 2 decimal places. The $1 floor in `risk.py:249` was a workaround for integer contracts — now obsolete.

3. **Kalshi API uses deprecated integer fields.** `kalshi_client.py` sends `count` (integer) and `yes_price` (cents). These legacy fields were deprecated March 12, 2026 and will eventually break. Must migrate to `count_fp` (string) and `yes_price_dollars`/`no_price_dollars` (string).

## Changes

### Change 1: Raise min_edge to 0.10 floor

**File:** `signals/rules.py` SPORT_PARAMS dict

| Sport | Current | New | Reason |
|-------|---------|-----|--------|
| NFLTD | 0.05 | 0.10 | Below break-even |
| NHLSPREAD | 0.05 | 0.10 | Below break-even |
| UCL | 0.08 | 0.10 | Below break-even |
| WNBA | 0.08 | 0.10 | Below break-even |
| UFC | 0.08 | 0.10 | Below break-even |
| NCAAWB | 0.08 | 0.10 | Below break-even |
| ATP | 0.08 | 0.10 | Below break-even |
| NFLGW | 0.08 | 0.10 | Below break-even |
| NFLTT | 0.08 | 0.10 | Below break-even |
| NCAAMB | 0.08 | 0.10 | Marginal, raise to floor |

Sports already at 0.10+ (EPL, NHL, NBA, NBASPREAD, NFLSPREAD, MLB, LALIGA, CPI, CFB, WEATHER) are unchanged.

### Change 2: Lower minimum bet to $0.10, support fractional contracts

**File:** `execution/risk.py`

- Line 245: Change `contracts = int(bet_amount / exec_price)` to preserve 2 decimal places via `Decimal.quantize("0.01")` and convert to float for the order
- Line 249: Change minimum from `bet_amount < Decimal("1.00") or contracts < 1` to `bet_amount < Decimal("0.10")` — removes integer contract requirement
- The Kelly formula already produces the mathematically correct bet size; we were artificially truncating it

### Change 3: Migrate Kalshi API to v2 fields

**File:** `execution/kalshi_client.py` — `place_order()` method

Current (deprecated):
```python
order_data = {
    "ticker": ticker,
    "side": side,
    "action": action,
    "count": count,          # integer, deprecated
    "type": "limit",         # removed Feb 2026
    price_field: price_cents, # integer cents, deprecated
}
```

New (v2):
```python
order_data = {
    "ticker": ticker,
    "side": side,
    "action": action,
    "count_fp": f"{count:.2f}",              # string, 2 decimal places
    price_field_dollars: f"{price:.4f}",      # string, 4 decimal places
    "client_order_id": str(uuid.uuid4()),
}
```

- `place_order()` signature: `count: int` becomes `count: float`
- Price field names: `yes_price` → `yes_price_dollars`, `no_price` → `no_price_dollars`
- Remove `"type": "limit"` — no longer a valid field
- Remove cent conversion (`price_cents = int(price * 100)`) — send dollars directly

**Also update `order_manager.py`** if it passes integer contracts to `place_order()`.

## Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `signals/rules.py` | Modified | Raise min_edge to 0.10 for 10 sports |
| `execution/risk.py` | Modified | Lower min bet to $0.10, fractional contracts |
| `execution/kalshi_client.py` | Modified | Migrate to count_fp and _dollars fields |
| `execution/order_manager.py` | Modified | Pass float contracts instead of int |

## Testing

- Syntax check all modified files
- Verify no sport has min_edge below 0.10
- Verify order payload uses `count_fp` and `_dollars` fields (log output)
- Deploy to VPS, verify agent places orders successfully with new API fields

## Success Criteria

1. No sport accepts trades with less than 10% raw edge
2. Sub-$1 Kelly recommendations are no longer rejected
3. Kalshi API accepts orders with new field names
4. Agent continues to trade normally with no API rejections
