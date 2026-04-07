"""Check every sport's edge + calendar coverage to find what adds value."""
import duckdb

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

RATES = {
    "NBA": {"lo": 0.608, "hi": 0.719},
    "NHL": {"lo": 0.545, "hi": 0.563},
}

sports = [
    ("NBA", "KXNBAGAME%"),
    ("NHL", "KXNHLGAME%"),
    ("NFL", "KXNFLGAME%"),
    ("NCAA-FB", "KXNCAAFGAME%"),
    ("MLB", "KXMLBGAME%"),
    ("MMA/UFC", "KXMMA%"),
    ("March Mad", "KXMARMAD%"),
]

print("=" * 85)
print("EVERY SPORT: Fade-Favorites Edge + Calendar Coverage")
print("=" * 85)
print()

for sport, pattern in sports:
    month_data = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pattern}' AND status = 'finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price, t.no_price,
                   EXTRACT(MONTH FROM t.created_time) as mo,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
        )
        SELECT ft.mo,
               COUNT(*) as total,
               SUM(CASE WHEN gm.result='no' THEN 1 ELSE 0 END) as no_wins
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
        GROUP BY ft.mo ORDER BY ft.mo
    """).fetchall()

    overall = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pattern}' AND status = 'finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price, t.no_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
        )
        SELECT COUNT(*) as total,
               SUM(CASE WHEN gm.result='no' THEN 1 ELSE 0 END) as no_wins,
               ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as no_pct,
               ROUND(SUM(
                   CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                        ELSE -(100-ft.yes_price)/100.0 END
               ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
    """).fetchone()

    if not overall or overall[0] == 0:
        print(f"{sport:10s}: No data in 61-90c range")
        continue

    months_active = {int(r[0]) for r in month_data if r[1] >= 3}
    month_labels = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]
    month_str = ""
    for m in range(1, 13):
        month_str += month_labels[m - 1] if m in months_active else "-"

    roi = overall[3] if overall[3] else 0
    if roi > 15:
        tag = "** STRONG **"
    elif roi > 5:
        tag = "decent"
    elif roi > 0:
        tag = "marginal"
    else:
        tag = "LOSING"

    print(f"{sport:10s} | {overall[0]:>4d} bets | NO wins {overall[2]:>5.1f}% | ROI {roi:>+6.1f}% | [{month_str}] | {tag}")

print()
print("Month key: J F M A M J J A S O N D")
print()

# ================================================================
# Simulate: what does adding each sport do to combined profits?
# ================================================================
print()
print("=" * 85)
print("COMBINED BACKTEST: What does adding each sport contribute?")
print("=" * 85)
print()

# First get sport-specific hit rates for all sports
for sport_name, pattern in sports:
    rates = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pattern}' AND status = 'finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
        )
        SELECT
            CASE WHEN ft.yes_price BETWEEN 61 AND 75 THEN 'lo'
                 WHEN ft.yes_price BETWEEN 76 AND 90 THEN 'hi' END as bucket,
            ROUND(SUM(CASE WHEN gm.result='yes' THEN 1.0 ELSE 0.0 END)/COUNT(*), 3) as yes_rate
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
        GROUP BY bucket
    """).fetchall()
    r = {}
    for bucket, rate in rates:
        if bucket:
            r[bucket] = rate
    RATES[sport_name] = {"lo": r.get("lo", 0.65), "hi": r.get("hi", 0.75)}

print("Empirical YES hit rates by sport:")
for s, r in RATES.items():
    print(f"  {s:10s}: 61-75c = {r['lo']:.1%} YES hit | 76-90c = {r['hi']:.1%} YES hit")
print()

# Load ALL sports trades
all_df = con.sql(f"""
    WITH game_markets AS (
        SELECT ticker, result, event_ticker,
               CASE
                   WHEN event_ticker LIKE 'KXNBAGAME%' THEN 'NBA'
                   WHEN event_ticker LIKE 'KXNHLGAME%' THEN 'NHL'
                   WHEN event_ticker LIKE 'KXNFLGAME%' THEN 'NFL'
                   WHEN event_ticker LIKE 'KXNCAAFGAME%' THEN 'NCAA-FB'
                   WHEN event_ticker LIKE 'KXMLBGAME%' THEN 'MLB'
               END as sport
        FROM '{mp}'
        WHERE (event_ticker LIKE 'KXNBAGAME%' OR event_ticker LIKE 'KXNHLGAME%'
               OR event_ticker LIKE 'KXNFLGAME%' OR event_ticker LIKE 'KXNCAAFGAME%'
               OR event_ticker LIKE 'KXMLBGAME%')
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
    WHERE ft.rn = 1 AND gm.sport IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()


def run_combined(df, sport_list, starting=100, max_bet=100, max_daily=8):
    subset = df[df["sport"].isin(sport_list)]
    bankroll = starting
    peak = starting
    max_dd = 0.0
    trades = 0
    wins = 0
    daily_count = 0
    last_date = None
    monthly = {}
    sport_count = {s: 0 for s in sport_list}

    for _, row in subset.iterrows():
        d = str(row["trade_date"])
        if d != last_date:
            if last_date:
                monthly[last_date[:7]] = bankroll
            daily_count = 0
            last_date = d
        if daily_count >= max_daily or bankroll < 2:
            continue

        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        sport = row["sport"]
        if yes_p < 61 or yes_p > 90:
            continue

        rates = RATES.get(sport, {"lo": 0.65, "hi": 0.75})
        actual_yes = rates["lo"] if yes_p <= 75 else rates["hi"]
        edge = (yes_p / 100.0) - actual_yes
        if edge < 0.05:
            continue

        no_cost = no_p / 100.0
        yes_cost = yes_p / 100.0
        b = yes_cost / no_cost if no_cost > 0 else 0
        p = 1 - actual_yes
        q = actual_yes
        kr = (b * p - q) / b if b > 0 else 0

        # Sport-specific Kelly: NHL gets more, NBA/NFL moderate, MLB conservative
        kelly_map = {"NHL": 0.25, "NBA": 0.15, "NFL": 0.15, "NCAA-FB": 0.12, "MLB": 0.08}
        maxpos_map = {"NHL": 0.10, "NBA": 0.08, "NFL": 0.08, "NCAA-FB": 0.06, "MLB": 0.05}
        kf = kelly_map.get(sport, 0.10)
        mp_ = maxpos_map.get(sport, 0.05)

        ka = max(0, min(kr * kf, mp_))
        pos_size = min(bankroll * ka, max_bet)
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
        else:
            bankroll -= cost + fee

        trades += 1
        daily_count += 1
        sport_count[sport] = sport_count.get(sport, 0) + 1
        if bankroll > peak:
            peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    if last_date:
        monthly[last_date[:7]] = bankroll

    months_with_trades = set()
    for d in subset["trade_date"].astype(str):
        m = int(d[5:7])
        months_with_trades.add(m)

    return {
        "trades": trades, "wins": wins,
        "wr": round(wins / max(trades, 1) * 100, 1),
        "final": round(bankroll, 2),
        "max_dd": round(max_dd * 100, 1),
        "monthly": monthly,
        "sport_count": sport_count,
        "months_active": months_with_trades,
    }


combos = [
    ("NBA + NHL (current)", ["NBA", "NHL"]),
    ("+ NFL", ["NBA", "NHL", "NFL"]),
    ("+ NCAA-FB", ["NBA", "NHL", "NCAA-FB"]),
    ("+ MLB", ["NBA", "NHL", "MLB"]),
    ("+ NFL + NCAA-FB", ["NBA", "NHL", "NFL", "NCAA-FB"]),
    ("ALL 5 sports", ["NBA", "NHL", "NFL", "NCAA-FB", "MLB"]),
]

print(f"{'Combo':25s} | {'Trades':>6s} | {'WR':>5s} | {'Final':>12s} | {'MaxDD':>6s} | {'Months':>6s} | Sport breakdown")
print("-" * 110)

for label, sport_list in combos:
    r = run_combined(all_df, sport_list)
    months = len(r["months_active"])
    sc = ", ".join(f"{s}:{r['sport_count'].get(s,0)}" for s in sport_list)
    final_s = f"${r['final']:>10,.2f}" if r["final"] < 1e8 else f"${r['final']:>.2e}"
    print(f"{label:25s} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {final_s:>12s} | {r['max_dd']:>5.1f}% | {months:>4d}/12 | {sc}")

# Show monthly for the "ALL 5" combo
print()
print("Monthly: ALL 5 sports combined ($100 start, $100 max bet)")
r = run_combined(all_df, ["NBA", "NHL", "NFL", "NCAA-FB", "MLB"])
for mo in sorted(r["monthly"].keys()):
    val = r["monthly"][mo]
    bar_len = min(40, max(1, int(val / max(r["monthly"].values()) * 40)))
    print(f"  {mo}: ${val:>10,.2f}  {'#' * bar_len}")
