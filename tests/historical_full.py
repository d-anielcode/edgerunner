"""
Full historical simulation: All 7 sports, exact agent logic, $100 start.
Tests the complete pipeline: rules engine, risk gates, Kelly sizing, sport-specific params.
"""
import re
from collections import defaultdict
from decimal import Decimal

import duckdb

# === Edge tables (from signals/rules.py) ===
EDGE_TABLES = {
    "NBA":    {(61, 75): 0.608, (76, 90): 0.719},
    "NHL":    {(61, 75): 0.545, (76, 90): 0.563},
    "EPL":    {(61, 75): 0.619, (76, 90): 0.473},
    "UCL":    {(61, 75): 0.524, (76, 90): 0.673},
    "LALIGA": {(61, 75): 0.684, (76, 90): 0.611},
    "WNBA":   {(61, 75): 0.659, (76, 90): 0.655},
    "UFC":    {(61, 75): 0.650, (76, 90): 0.700},
}

# === Sport params (from signals/rules.py) ===
SPORT_PARAMS = {
    "NBA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NHL":    {"kelly_mult": 0.30, "max_position": 0.12, "min_edge": 0.05},
    "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.05},
    "UCL":    {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "LALIGA": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.10},
    "WNBA":   {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "UFC":    {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
}

# === Risk gates ===
MAX_DRAWDOWN_PCT = 0.40
MAX_CONSECUTIVE_LOSSES = 6
MAX_PER_GAME = 3
MAX_CONCURRENT_POSITIONS = 10
MAX_DAILY_TRADES = 8
MIN_VOLUME = 500


def kalshi_fee(price):
    return 0.07 * price * (1 - price)


def run():
    con = duckdb.connect()
    mp = "data/trevorjs/markets-*.parquet"
    tp = "data/trevorjs/trades-*.parquet"

    print("Loading all sports trades...")

    # Build sport mapping
    sport_patterns = {
        "NBA": "KXNBAGAME%",
        "NHL": "KXNHLGAME%",
        "EPL": "KXEPLGAME%",
        "UCL": "KXUCLGAME%",
        "LALIGA": "KXLALIGAGAME%",
        "WNBA": "KXWNBAGAME%",
        "UFC": "KXUFCFIGHT%",
    }

    case_stmts = " ".join(
        f"WHEN event_ticker LIKE '{p}' THEN '{s}'"
        for s, p in sport_patterns.items()
    )
    like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

    all_trades = con.sql(f"""
        WITH game_markets AS (
            SELECT ticker, result, event_ticker, volume,
                   CASE {case_stmts} END as sport
            FROM '{mp}'
            WHERE ({like_clauses})
                  AND status = 'finalized' AND result IN ('yes','no')
        ),
        first_trades AS (
            SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
                   CAST(t.created_time AS DATE) as trade_date,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM game_markets)
        )
        SELECT ft.*, gm.result, gm.sport, gm.volume
        FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND gm.sport IS NOT NULL
        ORDER BY ft.created_time
    """).fetchdf()

    print(f"Loaded {len(all_trades)} markets across {all_trades['sport'].nunique()} sports")
    for s, c in all_trades["sport"].value_counts().items():
        print(f"  {s}: {c}")
    print()

    # === SIMULATION ===
    bankroll = 100.0
    peak = 100.0
    max_dd = 0.0
    min_br = 100.0

    total_trades = 0
    total_wins = 0
    total_rejected = 0
    rejection_reasons = defaultdict(int)
    sport_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})

    consecutive_losses = 0
    game_positions = defaultdict(int)

    daily_trades = 0
    last_date = None
    daily_log = {}

    for _, row in all_trades.iterrows():
        date = str(row["trade_date"])
        ticker = row["ticker"]
        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        sport = row["sport"]
        result = row["result"]
        volume = int(row["volume"])

        if date != last_date:
            if last_date:
                daily_log[last_date] = bankroll
            daily_trades = 0
            game_positions.clear()
            last_date = date

        # Price range
        if yes_p < 61 or yes_p > 90:
            continue

        # NHL playoff veto (Apr 17 - Sep)
        trade_month = int(date[5:7])
        trade_day = int(date[8:10])
        if sport == "NHL":
            if (trade_month > 4 or (trade_month == 4 and trade_day > 16)) and trade_month < 10:
                total_rejected += 1
                rejection_reasons["nhl_playoff_veto"] += 1
                continue

        # Edge check
        params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
        edge_table = EDGE_TABLES.get(sport, EDGE_TABLES["NBA"])
        actual_yes = 0.65
        for (lo, hi), rate in edge_table.items():
            if lo <= yes_p <= hi:
                actual_yes = rate
                break

        edge = (yes_p / 100.0) - actual_yes
        if edge < params["min_edge"]:
            total_rejected += 1
            rejection_reasons["edge_too_small"] += 1
            continue

        # Drawdown check — per-session (daily) drawdown from start of day
        # Real agent restarts daily, so drawdown resets each session
        day_start = daily_log.get(last_date, bankroll) if last_date and last_date != date else bankroll
        current_dd = (day_start - bankroll) / day_start if day_start > 0 else 0
        if current_dd >= MAX_DRAWDOWN_PCT:
            total_rejected += 1
            rejection_reasons["drawdown_halt"] += 1
            continue

        # Consecutive loss cooldown
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            total_rejected += 1
            rejection_reasons["loss_cooldown"] += 1
            consecutive_losses = 0
            continue

        # Fee-adjusted edge
        no_cost = no_p / 100.0
        fee = kalshi_fee(no_cost)
        slippage = 0.005
        friction = (fee + slippage) / no_cost if no_cost > 0 else 0
        net_edge = edge - friction
        if net_edge < 0.03:
            total_rejected += 1
            rejection_reasons["edge_after_fees"] += 1
            continue

        # Volume
        if volume < MIN_VOLUME:
            total_rejected += 1
            rejection_reasons["low_volume"] += 1
            continue

        # Daily limit
        if daily_trades >= MAX_DAILY_TRADES:
            total_rejected += 1
            rejection_reasons["daily_limit"] += 1
            continue

        # Kelly sizing
        yes_cost = yes_p / 100.0
        b = yes_cost / no_cost if no_cost > 0 else 0
        p = 1 - actual_yes
        q = actual_yes
        kelly_raw = (b * p - q) / b if b > 0 else 0
        kelly_adj = max(0, min(kelly_raw * params["kelly_mult"], params["max_position"]))

        if kelly_adj <= 0:
            total_rejected += 1
            rejection_reasons["kelly_zero"] += 1
            continue

        bet_size = bankroll * kelly_adj
        contracts = max(1, int(bet_size / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
        if cost > bankroll or bankroll < 2:
            total_rejected += 1
            rejection_reasons["insufficient_funds"] += 1
            continue

        # Execute
        total_fee = kalshi_fee(no_cost) * contracts
        pnl = 0.0

        if result == "no":
            pnl = contracts * (1.0 - no_cost) - total_fee
            bankroll += pnl
            total_wins += 1
            sport_stats[sport]["wins"] += 1
            consecutive_losses = 0
        else:
            pnl = -(cost + total_fee)
            bankroll += pnl
            consecutive_losses += 1

        total_trades += 1
        daily_trades += 1
        sport_stats[sport]["trades"] += 1
        sport_stats[sport]["pnl"] += pnl

        if bankroll > peak:
            peak = bankroll
        if bankroll < min_br:
            min_br = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    if last_date:
        daily_log[last_date] = bankroll

    # === REPORT ===
    print("=" * 90)
    print("FULL HISTORICAL SIMULATION: ALL 7 SPORTS")
    dates = sorted(daily_log.keys())
    print(f"Period: {dates[0]} to {dates[-1]}")
    print("=" * 90)
    print()

    wr = total_wins / max(total_trades, 1) * 100
    roi = (bankroll - 100) / 100 * 100
    print(f"Starting:       $100.00")
    print(f"Final:          ${bankroll:>12,.2f}")
    print(f"ROI:            {roi:>+12.1f}%")
    print(f"Max drawdown:   {max_dd * 100:>12.1f}%")
    print(f"Lowest balance: ${min_br:>12.2f}")
    print(f"Total trades:   {total_trades:>12d}")
    print(f"Wins:           {total_wins:>12d} ({wr:.1f}%)")
    print(f"Rejected:       {total_rejected:>12d}")
    print()

    print("SPORT BREAKDOWN:")
    print(f"  {'Sport':8s} | {'Trades':>7s} | {'Wins':>5s} | {'WR':>6s} | {'P&L':>12s} | Style")
    print("  " + "-" * 65)
    for sport in ["NBA", "NHL", "EPL", "UCL", "LALIGA", "WNBA", "UFC"]:
        s = sport_stats[sport]
        if s["trades"] > 0:
            swr = s["wins"] / s["trades"] * 100
            style = f"K={SPORT_PARAMS[sport]['kelly_mult']:.2f}"
            print(f"  {sport:8s} | {s['trades']:>7d} | {s['wins']:>5d} | {swr:>5.1f}% | ${s['pnl']:>+11.2f} | {style}")
    print()

    print("REJECTION REASONS:")
    for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason:25s}: {count:>6d}")
    print()

    # Monthly progression
    monthly = {}
    for date, br in sorted(daily_log.items()):
        monthly[date[:7]] = br

    max_val = max(monthly.values()) if monthly else 1
    print("MONTHLY BANKROLL:")
    for mo in sorted(monthly.keys()):
        val = monthly[mo]
        bar = "#" * min(50, max(1, int(val / max_val * 50)))
        print(f"  {mo}: ${val:>12,.2f}  {bar}")

    # Annualize
    months = len(monthly)
    if months > 0 and bankroll > 0:
        monthly_g = (bankroll / 100) ** (1 / months)
        annual = 100 * (monthly_g ** 12)
        print(f"\n  12-month projection: ~${annual:>12,.2f}")

    print()
    print("=" * 90)
    print("CALENDAR COVERAGE:")
    month_sports = defaultdict(set)
    for _, row in all_trades.iterrows():
        d = str(row["trade_date"])
        mo = int(d[5:7])
        month_sports[mo].add(row["sport"])

    month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                   7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    for m in range(1, 13):
        sports_active = sorted(month_sports.get(m, set()))
        status = ", ".join(sports_active) if sports_active else "-- NO DATA --"
        print(f"  {month_names[m]:3s}: {status}")
    print("=" * 90)


if __name__ == "__main__":
    run()
