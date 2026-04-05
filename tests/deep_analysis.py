"""
Comprehensive strategy analysis using 6.5M NBA trades from the Jon-Becker dataset.
Tests fee-adjusted ROI, stacked filters, bankroll simulations, and drawdown analysis.
"""

import duckdb

con = duckdb.connect()
kp = "data/dataset/data/data/kalshi"

# Load markets
markets = con.sql(f"""
    SELECT ticker, event_ticker, result, title, volume
    FROM '{kp}/markets/*.parquet'
    WHERE event_ticker LIKE 'KXNBA%' AND status = 'finalized' AND result IN ('yes', 'no')
""").fetchdf()
con.register("nba_markets", markets)

print("=" * 70)
print("EDGERUNNER COMPREHENSIVE STRATEGY ANALYSIS")
print(f"Dataset: {len(markets)} finalized NBA markets")
print("=" * 70)

# ANALYSIS 1: Fee-adjusted ROI
print("\n### ANALYSIS 1: GAME WINNER NO ROI — AFTER KALSHI FEES ###")
fee_adj = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBAGAME%')
    ),
    o AS (SELECT ft.*, m.result FROM ft JOIN nba_markets m ON ft.ticker = m.ticker WHERE ft.rn = 1)
    SELECT
        CASE WHEN yes_price <= 25 THEN '01-25c'
             WHEN yes_price <= 40 THEN '26-40c'
             WHEN yes_price <= 60 THEN '41-60c'
             WHEN yes_price <= 75 THEN '61-75c'
             ELSE '76-99c' END as bucket,
        COUNT(*) as n,
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1) as no_roi_raw,
        ROUND(AVG(0.07 * (no_price/100.0) * (1 - no_price/100.0) * 100), 2) as avg_fee_cents,
        ROUND(
            (SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)
             - COUNT(*) * AVG(0.07 * (no_price/100.0) * (1 - no_price/100.0) * 100))
            / NULLIF(SUM(no_price),0) * 100
        , 1) as no_roi_after_fees
    FROM o GROUP BY 1 ORDER BY AVG(yes_price)
""").fetchdf()
print(fee_adj.to_string(index=False))

# ANALYSIS 2: Stacked filters
print("\n### ANALYSIS 2: STACKED FILTERS — COMBINING EDGES ###")
stacked = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBAGAME%')
    ),
    o AS (SELECT ft.*, m.result, m.volume FROM ft JOIN nba_markets m ON ft.ticker = m.ticker WHERE ft.rn = 1)
    SELECT
        CASE
            WHEN yes_price > 60 AND volume < 10000 THEN 'Fav + LowVol'
            WHEN yes_price > 60 AND volume >= 10000 THEN 'Fav + HighVol'
            WHEN yes_price <= 60 AND volume < 10000 THEN 'NonFav + LowVol'
            ELSE 'NonFav + HighVol'
        END as combo,
        COUNT(*) as n,
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1) as no_win_pct,
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1) as no_roi
    FROM o GROUP BY 1 ORDER BY no_roi DESC
""").fetchdf()
print(stacked.to_string(index=False))

# ANALYSIS 3: Strategy comparison
print("\n### ANALYSIS 3: STRATEGY COMPARISON ###")
rules = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBAGAME%')
    ),
    o AS (SELECT ft.*, m.result, m.volume FROM ft JOIN nba_markets m ON ft.ticker = m.ticker WHERE ft.rn = 1)
    SELECT 'All games (blind NO)' as strategy, COUNT(*) as n,
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1) as win_pct,
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1) as roi
    FROM o
    UNION ALL
    SELECT 'YES > 60c (fade fav)', COUNT(*),
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1),
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1)
    FROM o WHERE yes_price > 60
    UNION ALL
    SELECT 'YES > 60c + vol<50K', COUNT(*),
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1),
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1)
    FROM o WHERE yes_price > 60 AND volume < 50000
    UNION ALL
    SELECT 'YES > 70c + vol<10K', COUNT(*),
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1),
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1)
    FROM o WHERE yes_price > 70 AND volume < 10000
    UNION ALL
    SELECT 'YES 61-75c only', COUNT(*),
        ROUND(AVG(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)*100,1),
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1)
    FROM o WHERE yes_price BETWEEN 61 AND 75
""").fetchdf()
print(rules.to_string(index=False))

# ANALYSIS 4: TOTAL markets
print("\n### ANALYSIS 4: TOTAL (OVER/UNDER) MARKETS ###")
totals = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBATOTAL%')
    ),
    o AS (SELECT ft.*, m.result FROM ft JOIN nba_markets m ON ft.ticker = m.ticker WHERE ft.rn = 1)
    SELECT
        CASE WHEN yes_price <= 30 THEN '01-30c'
             WHEN yes_price <= 50 THEN '31-50c'
             WHEN yes_price <= 70 THEN '51-70c'
             ELSE '71-99c' END as bucket,
        COUNT(*) as n,
        ROUND(AVG(CASE WHEN result='yes' THEN 1.0 ELSE 0.0 END)*100,1) as yes_hit,
        ROUND(SUM(CASE WHEN result='yes' THEN (100.0-yes_price) ELSE -yes_price END)/NULLIF(SUM(yes_price),0)*100,1) as yes_roi,
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1) as no_roi
    FROM o GROUP BY 1 ORDER BY AVG(yes_price)
""").fetchdf()
print(totals.to_string(index=False))

# ANALYSIS 5: Full bankroll simulation
print("\n### ANALYSIS 5: BANKROLL SIMULATION ###")
print("Strategy: Buy NO on game winners, YES > 60c, max 3/day")
print("Starting: $40, Position size: 10% of bankroll")

trades_df = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBAGAME%')
    ),
    o AS (
        SELECT ft.*, m.result
        FROM ft JOIN nba_markets m ON ft.ticker = m.ticker
        WHERE ft.rn = 1 AND ft.yes_price > 60
    )
    SELECT ticker, yes_price, no_price, result, created_time
    FROM o ORDER BY created_time
""").fetchdf()

bankroll = 40.0
peak = 40.0
max_dd = 0.0
trades_taken = 0
wins = 0
losses = 0
daily_trades = 0
last_date = None
milestones = {}

for _, row in trades_df.iterrows():
    trade_date = str(row["created_time"])[:10]
    if trade_date != last_date:
        daily_trades = 0
        last_date = trade_date

    if daily_trades >= 3 or bankroll < 5:
        continue

    no_cost = row["no_price"] / 100.0
    if no_cost <= 0:
        continue

    contracts = max(1, int((bankroll * 0.10) / no_cost))
    cost = contracts * no_cost
    if cost > bankroll:
        contracts = max(1, int(bankroll / no_cost))
        cost = contracts * no_cost
    if cost > bankroll:
        continue

    fee = 0.07 * no_cost * (1 - no_cost) * contracts

    if row["result"] == "no":
        profit = contracts * (1.0 - no_cost) - fee
        bankroll += profit
        wins += 1
    else:
        bankroll -= cost + fee
        losses += 1

    trades_taken += 1
    daily_trades += 1

    if bankroll > peak:
        peak = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd:
        max_dd = dd

    for milestone in [10, 25, 50, 100, 150, 200, 250, 300]:
        if trades_taken == milestone and milestone not in milestones:
            milestones[milestone] = bankroll

print(f"Trades: {trades_taken} | Wins: {wins} ({wins/max(trades_taken,1)*100:.1f}%)")
print(f"Starting: $40.00 | Final: ${bankroll:.2f} | P&L: ${bankroll-40:.2f} ({(bankroll-40)/40*100:.1f}%)")
print(f"Peak: ${peak:.2f} | Max drawdown: {max_dd*100:.1f}%")
print("Milestones:")
for m, b in sorted(milestones.items()):
    print(f"  After {m} trades: ${b:.2f} ({(b-40)/40*100:+.1f}%)")

# ANALYSIS 6: Spread markets
print("\n### ANALYSIS 6: SPREAD MARKETS — BY YES PRICE ###")
spreads = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM nba_markets WHERE event_ticker LIKE 'KXNBASPREAD%')
    ),
    o AS (SELECT ft.*, m.result FROM ft JOIN nba_markets m ON ft.ticker = m.ticker WHERE ft.rn = 1)
    SELECT
        CASE WHEN yes_price <= 30 THEN '01-30c'
             WHEN yes_price <= 50 THEN '31-50c'
             WHEN yes_price <= 70 THEN '51-70c'
             ELSE '71-99c' END as bucket,
        COUNT(*) as n,
        ROUND(AVG(CASE WHEN result='yes' THEN 1.0 ELSE 0.0 END)*100,1) as yes_hit,
        ROUND(SUM(CASE WHEN result='no' THEN (100.0-no_price) ELSE -no_price END)/NULLIF(SUM(no_price),0)*100,1) as no_roi,
        ROUND(SUM(CASE WHEN result='yes' THEN (100.0-yes_price) ELSE -yes_price END)/NULLIF(SUM(yes_price),0)*100,1) as yes_roi
    FROM o GROUP BY 1 ORDER BY AVG(yes_price)
""").fetchdf()
print(spreads.to_string(index=False))

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)
