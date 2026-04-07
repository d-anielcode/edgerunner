"""
EdgeRunner v2 Backtest — Rules-Based Strategy on Real Kalshi Data.

Simulates the "fade the favorite" strategy on historical game winner markets
using actual opening trade prices from the Jon-Becker dataset.

Strategy: Buy NO on NBA game winners where YES > 60c.
Starting bankroll: $100. Profits compound daily.
"""

import duckdb
from decimal import Decimal
from datetime import datetime

con = duckdb.connect()
kp = "data/dataset/data/data/kalshi"

# Load finalized game winner markets
print("Loading markets and trades...")
markets = con.sql(f"""
    SELECT ticker, event_ticker, result, title, volume, created_time
    FROM '{kp}/markets/*.parquet'
    WHERE event_ticker LIKE 'KXNBAGAME%'
      AND status = 'finalized'
      AND result IN ('yes', 'no')
""").fetchdf()
con.register("gw_markets", markets)

# Get first trade per market (opening price)
opening_trades = con.sql(f"""
    WITH ranked AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time as trade_time,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM gw_markets)
    )
    SELECT r.*, m.result, m.title, m.volume,
           CAST(r.trade_time AS DATE) as trade_date
    FROM ranked r
    JOIN gw_markets m ON r.ticker = m.ticker
    WHERE r.rn = 1
    ORDER BY r.trade_time
""").fetchdf()

print(f"Game winner markets with trades: {len(opening_trades)}")
print(f"Date range: {opening_trades['trade_date'].min()} to {opening_trades['trade_date'].max()}")

# ============================================================
# BACKTEST SIMULATION
# ============================================================

STARTING_BANKROLL = 100.0
MIN_YES_PRICE = 60  # Only fade favorites (YES > 60c)
MAX_YES_PRICE = 95  # Skip extreme favorites (tiny NO payout)
MAX_DAILY_TRADES = 5  # Max trades per day
KELLY_FRACTION = 0.35  # 35% Kelly
MAX_POSITION_PCT = 0.15  # Max 15% per trade

# Empirical hit rates by bucket
HIT_RATES = {
    (61, 75): 0.593,
    (76, 95): 0.758,
}

bankroll = STARTING_BANKROLL
starting_daily = STARTING_BANKROLL
peak = STARTING_BANKROLL
max_dd = 0.0
total_trades = 0
wins = 0
losses = 0
daily_trades = 0
last_date = None
consecutive_losses = 0
max_streak = 0

# Daily tracking
daily_log = []
current_day_pnl = 0.0

print(f"\n{'='*70}")
print(f"BACKTEST: Fade Favorites (YES > {MIN_YES_PRICE}c)")
print(f"Starting bankroll: ${STARTING_BANKROLL}")
print(f"Kelly: {KELLY_FRACTION}x | Max position: {MAX_POSITION_PCT*100}%")
print(f"Max {MAX_DAILY_TRADES} trades per day")
print(f"{'='*70}\n")

for _, row in opening_trades.iterrows():
    trade_date = str(row["trade_date"])
    yes_price = int(row["yes_price"])
    no_price = int(row["no_price"])
    result = row["result"]

    # New day — log previous day and reset
    if trade_date != last_date:
        if last_date is not None:
            daily_log.append({
                "date": last_date,
                "trades": daily_trades,
                "pnl": round(current_day_pnl, 2),
                "bankroll": round(bankroll, 2),
            })
        daily_trades = 0
        current_day_pnl = 0.0
        starting_daily = bankroll  # Compound: new day starts with previous day's bankroll
        last_date = trade_date

    # Skip if max daily trades reached
    if daily_trades >= MAX_DAILY_TRADES:
        continue

    # Skip if bankroll too low
    if bankroll < 5.0:
        continue

    # STRATEGY FILTER: Only fade favorites
    if yes_price < MIN_YES_PRICE or yes_price > MAX_YES_PRICE:
        continue

    # Calculate edge and Kelly
    actual_yes_rate = 0.65  # default
    for (lo, hi), rate in HIT_RATES.items():
        if lo <= yes_price <= hi:
            actual_yes_rate = rate
            break

    market_prob = yes_price / 100.0
    edge = market_prob - actual_yes_rate

    if edge < 0.05:
        continue  # Edge too small

    no_cost = no_price / 100.0
    yes_cost = yes_price / 100.0

    # Kelly sizing
    b = yes_cost / no_cost if no_cost > 0 else 0
    p = 1 - actual_yes_rate  # prob NO wins
    q = actual_yes_rate
    kelly_raw = (b * p - q) / b if b > 0 else 0
    kelly_adj = max(0, min(kelly_raw * KELLY_FRACTION, MAX_POSITION_PCT))

    position_size = bankroll * kelly_adj
    contracts = max(1, int(position_size / no_cost)) if no_cost > 0 else 0
    if contracts == 0:
        continue

    cost = contracts * no_cost

    # Don't bet more than we have
    if cost > bankroll:
        contracts = max(1, int(bankroll / no_cost))
        cost = contracts * no_cost
    if cost > bankroll:
        continue

    # Fee: $0.07 * P * (1-P) per contract
    fee = 0.07 * no_cost * (1 - no_cost) * contracts

    # Execute
    if result == "no":
        # We win — NO resolves to $1
        profit = contracts * (1.0 - no_cost) - fee
        bankroll += profit
        current_day_pnl += profit
        wins += 1
        consecutive_losses = 0
    else:
        # We lose — NO resolves to $0
        bankroll -= cost + fee
        current_day_pnl -= (cost + fee)
        losses += 1
        consecutive_losses += 1
        if consecutive_losses > max_streak:
            max_streak = consecutive_losses

    total_trades += 1
    daily_trades += 1

    if bankroll > peak:
        peak = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd:
        max_dd = dd

# Log final day
if last_date:
    daily_log.append({
        "date": last_date,
        "trades": daily_trades,
        "pnl": round(current_day_pnl, 2),
        "bankroll": round(bankroll, 2),
    })

# ============================================================
# RESULTS
# ============================================================

pnl = bankroll - STARTING_BANKROLL
win_rate = wins / max(total_trades, 1) * 100
days_traded = len([d for d in daily_log if d["trades"] > 0])
profitable_days = len([d for d in daily_log if d["pnl"] > 0])
losing_days = len([d for d in daily_log if d["pnl"] < 0])

print(f"{'='*70}")
print(f"RESULTS")
print(f"{'='*70}")
print(f"Period: {opening_trades['trade_date'].min()} to {opening_trades['trade_date'].max()}")
print(f"Days traded: {days_traded}")
print(f"Total trades: {total_trades}")
print(f"Wins: {wins} ({win_rate:.1f}%)")
print(f"Losses: {losses}")
print(f"")
print(f"Starting bankroll: ${STARTING_BANKROLL:.2f}")
print(f"Final bankroll: ${bankroll:.2f}")
print(f"Total P&L: ${pnl:+.2f} ({pnl/STARTING_BANKROLL*100:+.1f}%)")
print(f"Peak bankroll: ${peak:.2f}")
print(f"Max drawdown: {max_dd*100:.1f}%")
print(f"Max losing streak: {max_streak}")
print(f"")
print(f"Profitable days: {profitable_days}/{days_traded} ({profitable_days/max(days_traded,1)*100:.0f}%)")
print(f"Losing days: {losing_days}/{days_traded}")
if days_traded > 0:
    print(f"Avg daily P&L: ${pnl/days_traded:+.2f}")
    print(f"Avg daily return: {pnl/days_traded/STARTING_BANKROLL*100:+.2f}%")

# Daily breakdown
print(f"\n{'='*70}")
print(f"DAILY LOG (first 20 days)")
print(f"{'='*70}")
print(f"{'Date':>12s} | {'Trades':>6s} | {'Day P&L':>8s} | {'Bankroll':>10s}")
print("-" * 45)
for d in daily_log[:20]:
    if d["trades"] > 0:
        color = "+" if d["pnl"] >= 0 else ""
        print(f"{d['date']:>12s} | {d['trades']:>6d} | ${color}{d['pnl']:>7.2f} | ${d['bankroll']:>9.2f}")

# Show all days for monthly summary
print(f"\n{'='*70}")
print(f"MONTHLY SUMMARY")
print(f"{'='*70}")
monthly = {}
for d in daily_log:
    month = d["date"][:7]
    if month not in monthly:
        monthly[month] = {"trades": 0, "pnl": 0.0, "days": 0}
    monthly[month]["trades"] += d["trades"]
    monthly[month]["pnl"] += d["pnl"]
    if d["trades"] > 0:
        monthly[month]["days"] += 1

for month, data in sorted(monthly.items()):
    if data["trades"] > 0:
        print(f"{month} | {data['days']:>3d} days | {data['trades']:>4d} trades | P&L: ${data['pnl']:+8.2f}")
