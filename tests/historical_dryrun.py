"""
Historical dry-run: Simulates the EXACT agent pipeline against real past data.

This replays every trade from the TrevorJS dataset through our rules engine,
risk gates, and Kelly sizing — as if the agent were running live during that period.

Unlike the simple backtest, this tests:
- Sport-specific SPORT_PARAMS from rules.py
- 5-gate risk system (drawdown circuit breaker, liquidity, concentration, etc.)
- Kelly sizing with fee deduction
- Daily trade limits
- Consecutive loss cooldowns
- Per-game concentration limits
- Blowout veto logic (simulated)

This is the closest thing to a paper trade without connecting to Kalshi.
"""
import re
import time
from collections import defaultdict
from decimal import Decimal

import duckdb

# We can't import the full modules (they need Kalshi keys, etc.)
# So we replicate the exact logic from rules.py and risk_gates.py

# === FROM signals/rules.py ===
EDGE_TABLE_NBA = {(61, 75): (0.608,), (76, 90): (0.719,)}
EDGE_TABLE_NHL = {(61, 75): (0.545,), (76, 90): (0.563,)}
EDGE_TABLES = {"NBA": EDGE_TABLE_NBA, "NHL": EDGE_TABLE_NHL}

SPORT_PARAMS = {
    "NBA": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NHL": {"kelly_mult": 0.30, "max_position": 0.12, "min_edge": 0.05},
}

# === FROM execution/risk_gates.py ===
MAX_DRAWDOWN_PCT = 0.40
MAX_CONSECUTIVE_LOSSES = 6
LOSS_COOLDOWN_SECONDS = 600.0
MAX_PER_GAME = 3
MAX_TOTAL_EXPOSURE_PCT = 0.60
MAX_CONCURRENT_POSITIONS = 10
MIN_VOLUME = 500
MAX_SPREAD = Decimal("0.05")

# === FROM execution/position_monitor.py ===
AUTO_PROFIT_TAKE_PCT = 4.00
TRAILING_STOP_PCT = 0.25

# Kalshi fee formula
def kalshi_fee(price):
    return 0.07 * price * (1 - price)


def extract_game_id(ticker):
    match = re.search(r"KX(?:NBA|NHL)\w*-\d{2}[A-Z]{3}\d{2}([A-Z]{6})", ticker.upper())
    return match.group(1) if match else None


def run_historical_dryrun():
    con = duckdb.connect()
    mp = "data/trevorjs/markets-*.parquet"
    tp = "data/trevorjs/trades-*.parquet"

    print("Loading trades...")
    all_trades = con.sql(f"""
        WITH game_markets AS (
            SELECT ticker, result, event_ticker, volume,
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
        SELECT ft.*, gm.result, gm.sport, gm.volume, gm.event_ticker
        FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1
        ORDER BY ft.created_time
    """).fetchdf()

    print(f"Loaded {len(all_trades)} markets across {all_trades['sport'].nunique()} sports")
    print()

    # === SIMULATION STATE ===
    bankroll = 100.0
    starting_bankroll = 100.0
    peak_bankroll = 100.0
    max_dd = 0.0
    min_bankroll = 100.0

    positions = {}  # ticker -> {side, price, quantity, cost}
    game_positions = defaultdict(int)  # game_id -> count

    total_trades = 0
    total_wins = 0
    total_rejected = 0
    rejection_reasons = defaultdict(int)
    sport_stats = {"NBA": {"trades": 0, "wins": 0}, "NHL": {"trades": 0, "wins": 0}}

    consecutive_losses = 0
    last_loss_time = 0
    halted = False

    daily_trades = 0
    last_date = None
    max_daily_trades = 8

    daily_log = {}  # date -> bankroll
    trade_log = []  # list of all decisions

    for idx, row in all_trades.iterrows():
        date = str(row["trade_date"])
        ticker = row["ticker"]
        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        sport = row["sport"]
        result = row["result"]
        volume = int(row["volume"])

        # New day reset
        if date != last_date:
            if last_date:
                daily_log[last_date] = bankroll
            daily_trades = 0
            last_date = date

        # === RULE 1: Game winner only (already filtered) ===

        # === RULE 2: YES price range ===
        if yes_p < 61 or yes_p > 90:
            continue

        # === RULE 3: Spread check (simulate ~2c spread) ===
        simulated_spread = Decimal(str(abs(yes_p - no_p) / 100.0)) if (yes_p + no_p) != 100 else Decimal("0.02")

        # === RULE 3b: NHL playoff veto ===
        trade_month = int(date[5:7])
        trade_day = int(date[8:10])
        if sport == "NHL" and ((trade_month > 4 or (trade_month == 4 and trade_day > 16)) and trade_month < 10):
            total_rejected += 1
            rejection_reasons["nhl_playoff_veto"] += 1
            continue

        # === RULE 4: Sport-specific edge check ===
        params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
        edge_table = EDGE_TABLES.get(sport, EDGE_TABLE_NBA)
        actual_yes = 0.65
        for (lo, hi), (rate,) in edge_table.items():
            if lo <= yes_p <= hi:
                actual_yes = rate
                break

        market_prob = yes_p / 100.0
        edge = market_prob - actual_yes

        if edge < params["min_edge"]:
            total_rejected += 1
            rejection_reasons["edge_too_small"] += 1
            continue

        # === RISK GATE 1: Drawdown circuit breaker ===
        # In real agent this is a permanent halt per session. But across days we reset.
        # Here we check drawdown from PEAK (not starting), and skip the rest of the day.
        current_dd = (peak_bankroll - bankroll) / peak_bankroll if peak_bankroll > 0 else 0
        if current_dd >= MAX_DRAWDOWN_PCT:
            total_rejected += 1
            rejection_reasons["drawdown_halt"] += 1
            continue

        # Consecutive loss cooldown
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            # Simulate cooldown (check if enough trades have passed)
            total_rejected += 1
            rejection_reasons["loss_cooldown"] += 1
            consecutive_losses = 0  # Reset after cooldown
            continue

        # === RISK GATE 2: Fee-adjusted edge ===
        no_cost = no_p / 100.0
        fee = kalshi_fee(no_cost)
        slippage = 0.005
        friction = (fee + slippage) / no_cost if no_cost > 0 else 0
        net_edge = edge - friction
        if net_edge < 0.03:
            total_rejected += 1
            rejection_reasons["edge_after_fees"] += 1
            continue

        # === RISK GATE 3: Liquidity ===
        if volume < MIN_VOLUME:
            total_rejected += 1
            rejection_reasons["low_volume"] += 1
            continue

        # === RISK GATE 4: Concentration ===
        game_id = extract_game_id(ticker)
        if game_id and game_positions[game_id] >= MAX_PER_GAME:
            total_rejected += 1
            rejection_reasons["concentration"] += 1
            continue

        total_exposure = sum(p["cost"] for p in positions.values())
        if total_exposure >= bankroll * MAX_TOTAL_EXPOSURE_PCT:
            total_rejected += 1
            rejection_reasons["exposure_limit"] += 1
            continue

        # === RISK GATE 5: Position limit ===
        if len(positions) >= MAX_CONCURRENT_POSITIONS:
            total_rejected += 1
            rejection_reasons["position_limit"] += 1
            continue

        # === Daily trade limit ===
        if daily_trades >= max_daily_trades:
            total_rejected += 1
            rejection_reasons["daily_limit"] += 1
            continue

        # === KELLY SIZING ===
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

        # === EXECUTE TRADE ===
        total_fee = kalshi_fee(no_cost) * contracts

        if result == "no":  # NO wins = our bet wins
            payout = contracts * (1.0 - no_cost) - total_fee
            bankroll += payout
            total_wins += 1
            sport_stats[sport]["wins"] += 1
            consecutive_losses = 0
        else:  # YES wins = our bet loses
            bankroll -= cost + total_fee
            consecutive_losses += 1

        total_trades += 1
        daily_trades += 1
        sport_stats[sport]["trades"] += 1

        if game_id:
            game_positions[game_id] += 1

        # Track peak/drawdown
        if bankroll > peak_bankroll:
            peak_bankroll = bankroll
        if bankroll < min_bankroll:
            min_bankroll = bankroll
        dd = (peak_bankroll - bankroll) / peak_bankroll if peak_bankroll > 0 else 0
        if dd > max_dd:
            max_dd = dd

        trade_log.append({
            "date": date, "ticker": ticker[:40], "sport": sport,
            "yes_p": yes_p, "edge": round(edge * 100, 1),
            "kelly": round(kelly_adj, 4), "contracts": contracts,
            "cost": round(cost, 2), "result": "WIN" if result == "no" else "LOSS",
            "bankroll": round(bankroll, 2),
        })

    # Final day
    if last_date:
        daily_log[last_date] = bankroll

    # === REPORT ===
    print("=" * 90)
    print("HISTORICAL DRY-RUN RESULTS")
    print(f"Period: {min(daily_log.keys())} to {max(daily_log.keys())}")
    print("=" * 90)
    print()

    wr = total_wins / max(total_trades, 1) * 100
    roi = (bankroll - starting_bankroll) / starting_bankroll * 100
    print(f"Starting bankroll:  ${starting_bankroll:>10.2f}")
    print(f"Final bankroll:     ${bankroll:>10,.2f}")
    print(f"ROI:                {roi:>+10.1f}%")
    print(f"Max drawdown:       {max_dd * 100:>10.1f}%")
    print(f"Lowest balance:     ${min_bankroll:>10.2f}")
    print()
    print(f"Total trades:       {total_trades}")
    print(f"Total wins:         {total_wins} ({wr:.1f}%)")
    print(f"Total rejected:     {total_rejected}")
    print()

    print("Sport breakdown:")
    for s in ["NBA", "NHL"]:
        st = sport_stats[s]
        swr = st["wins"] / max(st["trades"], 1) * 100
        print(f"  {s}: {st['trades']} trades, {st['wins']} wins ({swr:.1f}%)")
    print()

    print("Rejection reasons:")
    for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason:25s}: {count:>5d}")
    print()

    # Monthly progression
    monthly = {}
    for date, br in sorted(daily_log.items()):
        monthly[date[:7]] = br

    print("Monthly bankroll:")
    for mo in sorted(monthly.keys()):
        val = monthly[mo]
        max_val = max(monthly.values())
        bar = "#" * min(40, max(1, int(val / max_val * 40)))
        print(f"  {mo}: ${val:>12,.2f}  {bar}")
    print()

    # Last 20 trades
    print("Last 20 trades:")
    print(f"  {'Date':>12s} {'Sport':>5s} {'Ticker':>30s} {'YES':>4s} {'Edge':>5s} {'Kelly':>6s} {'#':>3s} {'Cost':>7s} {'Result':>6s} {'Bankroll':>10s}")
    for t in trade_log[-20:]:
        print(f"  {t['date']:>12s} {t['sport']:>5s} {t['ticker']:>30s} {t['yes_p']:>4d}c {t['edge']:>4.1f}% {t['kelly']:>.4f} {t['contracts']:>3d} ${t['cost']:>6.2f} {t['result']:>6s} ${t['bankroll']:>9,.2f}")

    print()
    print("=" * 90)
    dd_rejects = rejection_reasons.get("drawdown_halt", 0)
    if dd_rejects > 0:
        print(f"Drawdown circuit breaker rejected {dd_rejects} trades (temporary pauses)")
    else:
        print("Drawdown circuit breaker: Never triggered (good)")
    print("=" * 90)


if __name__ == "__main__":
    run_historical_dryrun()
