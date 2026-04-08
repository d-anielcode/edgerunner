"""
10-year backtest using sportsbook moneyline data (2011-2021).
Converts American odds to Kalshi-equivalent prices and runs our exact strategy.
$100 start per year, hold to settlement, per-price Kelly.
"""
import json
from collections import defaultdict


def american_to_cents(ml):
    """Convert American moneyline to Kalshi-equivalent cents (0-100)."""
    if ml is None or ml == 0: return None
    ml = float(ml)
    if ml > 0:
        prob = 100 / (ml + 100)
    else:
        prob = abs(ml) / (abs(ml) + 100)
    return int(prob * 100)


def per_price_yes_rate(sport, yp):
    if sport == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def run_year(games, sport, start=100.0, kelly_scale=1.5, max_bet=200):
    bankroll = start
    peak = start
    max_dd = 0.0
    min_br = start
    trades = 0
    wins = 0
    monthly_br = {}

    for g in games:
        yp = g["fav_cents"]
        if yp < 61 or yp > 95: continue

        ay = per_price_yes_rate(sport, yp)
        if ay is None: continue

        edge = (yp / 100.0) - ay
        me = 0.05 if sport == "NHL" else 0.08
        if edge < me: continue

        nc = (100 - yp) / 100.0
        fee = 0.07 * nc * (1 - nc)
        if edge - (fee + 0.005) / nc < 0.03: continue

        b = (yp / 100) / nc if nc > 0 else 0
        kr = (b * (1 - ay) - ay) / b if b > 0 else 0
        ka = max(0, min(kr * 0.375 * kelly_scale / 1.5, 0.18 * kelly_scale / 1.5))
        if ka <= 0: continue

        # Away favorite boost
        if g.get("fav_location") == "away":
            ka = min(ka * 1.5, 0.27)

        bet = min(bankroll * ka, max_bet)
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > bankroll: continue

        tf = 0.07 * nc * (1 - nc) * contracts
        if not g["fav_won"]:  # Underdog won = our NO bet wins
            bankroll += contracts * (1.0 - nc) - tf
            wins += 1
        else:
            bankroll -= cost + tf

        trades += 1
        if bankroll > peak: peak = bankroll
        if bankroll < min_br: min_br = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

        # Track monthly
        month = str(g.get("date", 0))[:6]  # YYYYMM
        monthly_br[month] = bankroll

    wr = wins / max(trades, 1) * 100
    return {
        "final": bankroll, "trades": trades, "wins": wins, "wr": wr,
        "max_dd": max_dd * 100, "min_br": min_br, "monthly": monthly_br
    }


# Load SBR data
for sport, filename in [("NBA", "data/sportsbook/nba_archive_10Y.json"),
                          ("NHL", "data/sportsbook/nhl_archive_10Y.json")]:
    with open(filename) as f:
        raw = json.load(f)

    # Convert to our format
    all_games = []
    for game in raw:
        home_ml = game.get("home_close_ml") or game.get("home_open_ml")
        away_ml = game.get("away_close_ml") or game.get("away_open_ml")
        home_final = game.get("home_final")
        away_final = game.get("away_final")
        season = game.get("season")
        date = game.get("date")

        if home_ml is None or away_ml is None: continue
        if home_final is None or away_final is None: continue
        try:
            hs = int(home_final); as_ = int(away_final)
            hml = int(home_ml); aml = int(away_ml)
        except (ValueError, TypeError): continue
        if hs == as_: continue

        hc = american_to_cents(hml)
        ac = american_to_cents(aml)
        if hc is None or ac is None: continue

        home_won = hs > as_
        if hc > ac:
            fav_cents = hc; fav_won = home_won; fav_loc = "home"
        else:
            fav_cents = ac; fav_won = not home_won; fav_loc = "away"

        all_games.append({
            "fav_cents": fav_cents, "fav_won": fav_won,
            "fav_location": fav_loc, "season": season, "date": date,
        })

    # Group by season
    seasons = defaultdict(list)
    for g in all_games:
        seasons[g["season"]].append(g)

    print("=" * 90)
    print(f"{sport}: 10-YEAR SPORTSBOOK BACKTEST (2011-2021)")
    print(f"$100 fresh start each season, 1.5x Kelly, $200 max bet")
    print(f"Per-price model, away boost, hold to settlement")
    print("=" * 90)
    print()
    print(f"{'Season':>8s} | {'Games':>6s} | {'Trades':>6s} | {'Wins':>5s} | {'WR':>5s} | {'Final':>10s} | {'MaxDD':>6s} | {'Low':>8s}")
    print("-" * 75)

    total_final = 0
    total_trades = 0
    total_wins = 0
    winning_seasons = 0

    for season in sorted(seasons.keys()):
        games = seasons[season]
        r = run_year(games, sport)
        total_final += r["final"]
        total_trades += r["trades"]
        total_wins += r["wins"]
        if r["final"] > 100: winning_seasons += 1
        marker = " <<<" if r["final"] < 100 else ""
        print(f"  {season:>6d} | {len(games):>6d} | {r['trades']:>6d} | {r['wins']:>5d} | {r['wr']:>4.1f}% | ${r['final']:>8.2f} | {r['max_dd']:>5.1f}% | ${r['min_br']:>7.2f}{marker}")

    num_seasons = len(seasons)
    avg_final = total_final / num_seasons
    avg_wr = total_wins / max(total_trades, 1) * 100
    print("-" * 75)
    print(f"  {'AVG':>6s} | {'':>6s} | {total_trades//num_seasons:>6d} | {total_wins//num_seasons:>5d} | {avg_wr:>4.1f}% | ${avg_final:>8.2f} | {'':>6s} | {'':>8s}")
    print(f"\n  Winning seasons: {winning_seasons}/{num_seasons} ({winning_seasons/num_seasons*100:.0f}%)")
    print(f"  Average per-season return: ${avg_final - 100:+.2f} ({(avg_final-100):.1f}%)")

    # Also run continuous (one $100 across all 10 years)
    print(f"\n  CONTINUOUS RUN ($100 across all 10 years):")
    r_all = run_year(all_games, sport, start=100.0)
    print(f"    $100 -> ${r_all['final']:>,.2f} | {r_all['trades']} trades | {r_all['wr']:.1f}% WR | {r_all['max_dd']:.1f}% MaxDD")
    print()
