"""
Full-year projection backtest with sport-specific parameters.
Compares conservative, moderate, and aggressive configs.
"""
import duckdb

con = duckdb.connect()

mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

RATES = {
    "NBA": {"lo": 0.608, "hi": 0.719},
    "NHL": {"lo": 0.545, "hi": 0.563},
}

# Load all trades
all_trades = con.sql(f"""
    WITH game_markets AS (
        SELECT ticker, result, event_ticker,
               CASE WHEN event_ticker LIKE 'KXNBAGAME%' THEN 'NBA'
                    WHEN event_ticker LIKE 'KXNHLGAME%' THEN 'NHL' END as sport
        FROM '{mp}'
        WHERE (event_ticker LIKE 'KXNBAGAME%' OR event_ticker LIKE 'KXNHLGAME%')
              AND status = 'finalized' AND result IN ('yes','no')
    ),
    first_trades AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM game_markets)
    )
    SELECT ft.*, gm.result, gm.sport
    FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1
    ORDER BY ft.created_time
""").fetchdf()


def run_backtest(df, starting=100.0, nba_kelly=0.20, nhl_kelly=0.20,
                 nba_max_pos=0.08, nhl_max_pos=0.08,
                 nba_min_edge=0.08, nhl_min_edge=0.08,
                 nba_range=(61, 90), nhl_range=(61, 90),
                 max_daily=8):
    bankroll = starting
    peak = starting
    max_dd = 0.0
    trades = 0
    wins = 0
    daily_count = 0
    last_date = None
    daily_bankroll = {}
    sport_trades = {"NBA": 0, "NHL": 0}
    sport_wins = {"NBA": 0, "NHL": 0}
    min_bankroll = starting

    for _, row in df.iterrows():
        d = str(row["trade_date"])
        if d != last_date:
            if last_date:
                daily_bankroll[last_date] = bankroll
            daily_count = 0
            last_date = d
        if daily_count >= max_daily or bankroll < 2:
            continue

        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        sport = row["sport"]

        if sport == "NBA":
            kf, mp_, me = nba_kelly, nba_max_pos, nba_min_edge
            lo, hi = nba_range
        else:
            kf, mp_, me = nhl_kelly, nhl_max_pos, nhl_min_edge
            lo, hi = nhl_range

        if yes_p < lo or yes_p > hi:
            continue

        rates = RATES.get(sport, RATES["NBA"])
        actual_yes = rates["lo"] if yes_p <= 75 else rates["hi"]
        edge = (yes_p / 100.0) - actual_yes
        if edge < me:
            continue

        no_cost = no_p / 100.0
        yes_cost = yes_p / 100.0
        b = yes_cost / no_cost if no_cost > 0 else 0
        p = 1 - actual_yes
        q = actual_yes
        kr = (b * p - q) / b if b > 0 else 0
        ka = max(0, min(kr * kf, mp_))

        pos_size = bankroll * ka
        contracts = max(1, int(pos_size / no_cost)) if no_cost > 0 else 0
        if contracts == 0:
            continue
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
        if cost > bankroll:
            continue

        fee = 0.07 * no_cost * (1 - no_cost) * contracts

        if row["result"] == "no":
            bankroll += contracts * (1.0 - no_cost) - fee
            wins += 1
            sport_wins[sport] += 1
        else:
            bankroll -= cost + fee

        trades += 1
        daily_count += 1
        sport_trades[sport] += 1
        if bankroll > peak:
            peak = bankroll
        if bankroll < min_bankroll:
            min_bankroll = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    if last_date:
        daily_bankroll[last_date] = bankroll

    return {
        "trades": trades, "wins": wins,
        "wr": round(wins / max(trades, 1) * 100, 1),
        "final": round(bankroll, 2),
        "roi": round((bankroll - starting) / starting * 100, 1),
        "max_dd": round(max_dd * 100, 1),
        "min_br": round(min_bankroll, 2),
        "daily": daily_bankroll,
        "sport_trades": sport_trades,
        "sport_wins": sport_wins,
    }


# ================================================================
# PART 1: Same params vs sport-specific
# ================================================================
print("=" * 80)
print("PART 1: SHOULD NBA AND NHL HAVE DIFFERENT PARAMETERS?")
print("=" * 80)
print()
print("NHL has 45% win rate (vs NBA 34%). Bigger edge = can bet more aggressively.")
print()

configs = [
    ("Same: 0.20/0.20 Kelly",          0.20, 0.20, 0.08, 0.08, 0.08, 0.08),
    ("Split: NBA 0.15 / NHL 0.25",     0.15, 0.25, 0.08, 0.10, 0.08, 0.05),
    ("Split: NBA 0.15 / NHL 0.30",     0.15, 0.30, 0.08, 0.12, 0.08, 0.05),
    ("Split: NBA 0.20 / NHL 0.35",     0.20, 0.35, 0.08, 0.15, 0.08, 0.05),
    ("Conservative: NBA 0.10 / NHL 0.15", 0.10, 0.15, 0.05, 0.08, 0.08, 0.05),
    ("Aggressive: NBA 0.25 / NHL 0.40",  0.25, 0.40, 0.10, 0.15, 0.08, 0.05),
]

print(f"{'Config':40s} | {'Trades':>6s} | {'WR':>5s} | {'Final':>14s} | {'MaxDD':>6s} | {'Lowest':>8s}")
print("-" * 95)
for label, nba_k, nhl_k, nba_p, nhl_p, nba_e, nhl_e in configs:
    r = run_backtest(all_trades, starting=100,
                     nba_kelly=nba_k, nhl_kelly=nhl_k,
                     nba_max_pos=nba_p, nhl_max_pos=nhl_p,
                     nba_min_edge=nba_e, nhl_min_edge=nhl_e)
    if r["final"] > 1e8:
        final_s = f"${r['final']:>.1e}"
    else:
        final_s = f"${r['final']:>12,.2f}"
    print(f"{label:40s} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {final_s:>14s} | {r['max_dd']:>5.1f}% | ${r['min_br']:>7.2f}")


# ================================================================
# PART 2: Full year monthly progression
# ================================================================
print()
print("=" * 80)
print("PART 2: MONTHLY BANKROLL PROGRESSION ($100 start)")
print("=" * 80)
print()
print("Data: Apr 2025 - Jan 2026 (~10 months of games)")
print("Jul-Sep: only NHL preseason data available, thin activity")
print()

best_configs = [
    ("Conservative\n  NBA: 0.10 Kelly, 5% max\n  NHL: 0.15 Kelly, 8% max",
     0.10, 0.15, 0.05, 0.08, 0.08, 0.05),
    ("Moderate\n  NBA: 0.15 Kelly, 8% max\n  NHL: 0.25 Kelly, 10% max",
     0.15, 0.25, 0.08, 0.10, 0.08, 0.05),
    ("Aggressive\n  NBA: 0.20 Kelly, 8% max\n  NHL: 0.35 Kelly, 15% max",
     0.20, 0.35, 0.08, 0.15, 0.08, 0.05),
]

for label, nba_k, nhl_k, nba_p, nhl_p, nba_e, nhl_e in best_configs:
    r = run_backtest(all_trades, starting=100,
                     nba_kelly=nba_k, nhl_kelly=nhl_k,
                     nba_max_pos=nba_p, nhl_max_pos=nhl_p,
                     nba_min_edge=nba_e, nhl_min_edge=nhl_e)

    print(f"--- {label.split(chr(10))[0]} ---")
    for line in label.split("\n")[1:]:
        print(f"  {line.strip()}")

    nba_wr = r["sport_wins"].get("NBA", 0) / max(r["sport_trades"].get("NBA", 1), 1) * 100
    nhl_wr = r["sport_wins"].get("NHL", 0) / max(r["sport_trades"].get("NHL", 1), 1) * 100
    print(f"  Trades: {r['trades']} (NBA: {r['sport_trades'].get('NBA', 0)}, NHL: {r['sport_trades'].get('NHL', 0)})")
    print(f"  Win rates: NBA {nba_wr:.1f}% | NHL {nhl_wr:.1f}%")
    print(f"  Max drawdown: {r['max_dd']}% | Lowest balance: ${r['min_br']}")
    print()

    monthly = {}
    for date, br in sorted(r["daily"].items()):
        mo = date[:7]
        monthly[mo] = br

    max_val = max(monthly.values()) if monthly else 1
    for mo in sorted(monthly.keys()):
        bar_len = min(40, max(1, int(monthly[mo] / max_val * 40)))
        bar = "#" * bar_len
        print(f"    {mo}: ${monthly[mo]:>14,.2f}  {bar}")

    print(f"    FINAL: ${r['final']:>14,.2f}")

    # Annualize: data covers ~10 months, extrapolate
    months_in_data = len(monthly)
    if months_in_data > 0:
        monthly_growth = (r["final"] / 100) ** (1 / months_in_data)
        annual_proj = 100 * (monthly_growth ** 12)
        print(f"    12-month projection: ~${annual_proj:>14,.2f}")
    print()


# ================================================================
# PART 3: Aggressive vs conservative — the tradeoff
# ================================================================
print()
print("=" * 80)
print("PART 3: THE RISK/REWARD TRADEOFF")
print("=" * 80)
print()
print("More aggressive = more profit BUT bigger drawdowns.")
print("The question: can you stomach watching your bankroll drop 50%?")
print()

print(f"{'':30s} | {'Conservative':>14s} | {'Moderate':>14s} | {'Aggressive':>14s}")
print("-" * 80)

results = []
for label, nba_k, nhl_k, nba_p, nhl_p, nba_e, nhl_e in best_configs:
    r = run_backtest(all_trades, starting=100,
                     nba_kelly=nba_k, nhl_kelly=nhl_k,
                     nba_max_pos=nba_p, nhl_max_pos=nhl_p,
                     nba_min_edge=nba_e, nhl_min_edge=nhl_e)
    results.append(r)

labels = ["Conservative", "Moderate", "Aggressive"]
rows = [
    ("Final bankroll", [f"${r['final']:>12,.2f}" for r in results]),
    ("Total trades", [f"{r['trades']:>14d}" for r in results]),
    ("Win rate", [f"{r['wr']:>13.1f}%" for r in results]),
    ("Max drawdown", [f"{r['max_dd']:>13.1f}%" for r in results]),
    ("Lowest balance", [f"${r['min_br']:>12.2f}" for r in results]),
    ("Worst case: you see", [f"${r['min_br']:>12.2f}" for r in results]),
]

for row_label, values in rows:
    print(f"{row_label:30s} | {values[0]:>14s} | {values[1]:>14s} | {values[2]:>14s}")
