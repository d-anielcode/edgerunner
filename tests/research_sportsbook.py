"""
Validate Kalshi edge tables against 10+ years of sportsbook moneyline data.
Does the favorite-longshot bias exist in traditional sports betting too?
"""
import json
import csv
from collections import defaultdict


def american_to_implied(ml):
    """Convert American moneyline odds to implied probability (0-1)."""
    if ml is None or ml == 0:
        return None
    ml = float(ml)
    if ml > 0:
        return 100 / (ml + 100)
    else:
        return abs(ml) / (abs(ml) + 100)


def implied_to_kalshi_cents(prob):
    """Convert implied probability to Kalshi-equivalent cents (0-100)."""
    return int(prob * 100)


# ================================================================
# LOAD SBR DATA (2011-2021)
# ================================================================
print("=" * 85)
print("SPORTSBOOK MONEYLINE ANALYSIS: 10+ Years of Data")
print("Does the favorite-longshot bias exist at sportsbooks?")
print("=" * 85)

results = {}

for sport, filename in [("NBA", "data/sportsbook/nba_archive_10Y.json"),
                          ("NHL", "data/sportsbook/nhl_archive_10Y.json")]:
    with open(filename) as f:
        data = json.load(f)

    print(f"\n{'='*40}")
    print(f"{sport}: {len(data)} games (SBR 2011-2021)")
    print(f"{'='*40}")

    games = []
    for game in data:
        home_ml = game.get("home_close_ml") or game.get("home_open_ml")
        away_ml = game.get("away_close_ml") or game.get("away_open_ml")
        home_final = game.get("home_final")
        away_final = game.get("away_final")
        season = game.get("season")
        date = game.get("date")

        if home_ml is None or away_ml is None:
            continue
        if home_final is None or away_final is None:
            continue

        try:
            home_score = int(home_final)
            away_score = int(away_final)
            home_ml = int(home_ml)
            away_ml = int(away_ml)
        except (ValueError, TypeError):
            continue

        if home_score == away_score:
            continue  # Skip ties (NHL OT handled by sportsbook as winner)

        home_prob = american_to_implied(home_ml)
        away_prob = american_to_implied(away_ml)
        if home_prob is None or away_prob is None:
            continue

        home_won = home_score > away_score
        home_cents = implied_to_kalshi_cents(home_prob)
        away_cents = implied_to_kalshi_cents(away_prob)

        # The favorite is whoever has higher implied probability
        if home_prob > away_prob:
            fav_cents = home_cents
            fav_won = home_won
            fav_location = "home"
        else:
            fav_cents = away_cents
            fav_won = not home_won
            fav_location = "away"

        games.append({
            "fav_cents": fav_cents,
            "fav_won": fav_won,
            "fav_location": fav_location,
            "season": season,
            "date": date,
        })

    print(f"  Valid games with moneylines: {len(games)}")

    # Per-price analysis (same as our Kalshi analysis)
    print(f"\n  Per-price favorite win rate (Kalshi-equivalent cents):")
    print(f"  {'Price':>5s} | {'Games':>6s} | {'Fav Wins':>9s} | {'Fav Win%':>8s} | {'NO Win%':>8s} | {'NO ROI':>7s} | Our Kalshi Data")

    our_rates = {
        "NBA": {65: 0.460, 70: 0.460, 75: 0.440, 80: 0.420, 85: 0.400, 90: 0.380},
        "NHL": {65: 0.535, 70: 0.520, 75: 0.505, 80: 0.490, 85: 0.475, 90: 0.460},
    }

    for price in range(55, 91, 5):
        bucket = [g for g in games if price <= g["fav_cents"] < price + 5]
        if len(bucket) < 20:
            continue
        fav_wins = sum(1 for g in bucket if g["fav_won"])
        fav_pct = fav_wins / len(bucket) * 100
        no_pct = 100 - fav_pct

        # Calculate NO ROI (same as fading the favorite)
        total_cost = sum((100 - g["fav_cents"]) / 100 for g in bucket)
        total_pnl = sum(
            (1.0 - (100 - g["fav_cents"]) / 100) if not g["fav_won"]
            else -(100 - g["fav_cents"]) / 100
            for g in bucket
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0

        # Compare to our Kalshi rate
        kalshi_rate = our_rates.get(sport, {}).get(price + 2, None)
        kalshi_str = f"Kalshi: {kalshi_rate*100:.0f}% YES" if kalshi_rate else ""

        bar = "#" * max(0, int(roi / 5)) if roi > 0 else "-" * max(0, int(-roi / 5))
        print(f"  {price:>3d}-{price+4}c | {len(bucket):>6d} | {fav_wins:>9d} | {fav_pct:>7.1f}% | {no_pct:>7.1f}% | {roi:>+6.1f}% | {kalshi_str} {bar[:15]}")

    # Home vs Away favorite
    print(f"\n  Home vs Away favorite (fading):")
    for loc in ["home", "away"]:
        bucket = [g for g in games if g["fav_location"] == loc and 61 <= g["fav_cents"] <= 90]
        if len(bucket) < 20:
            continue
        fav_wins = sum(1 for g in bucket if g["fav_won"])
        no_pct = (1 - fav_wins / len(bucket)) * 100
        total_cost = sum((100 - g["fav_cents"]) / 100 for g in bucket)
        total_pnl = sum(
            (1.0 - (100 - g["fav_cents"]) / 100) if not g["fav_won"]
            else -(100 - g["fav_cents"]) / 100
            for g in bucket
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"    {loc.upper():5s} favorite: {len(bucket):>6d} games | NO wins {no_pct:>5.1f}% | NO ROI {roi:>+6.1f}%")

    # Playoff vs Regular Season
    print(f"\n  Playoff vs Regular Season (fading favorites 61-90c):")
    # SBR dates are YYYYMMDD floats. Playoffs are roughly Apr 15 - Jun for NBA, Apr - Jun for NHL
    for label, date_filter in [
        ("Regular season", lambda d: True),  # We'll split by month below
    ]:
        pass

    # By month
    print(f"\n  Monthly NO win rate (fading favorites 61-90c):")
    monthly = defaultdict(lambda: {"games": 0, "no_wins": 0, "cost": 0, "pnl": 0})
    for g in games:
        if g["fav_cents"] < 61 or g["fav_cents"] > 90:
            continue
        if g["date"] is None:
            continue
        month = int(str(int(g["date"]))[4:6])
        monthly[month]["games"] += 1
        nc = (100 - g["fav_cents"]) / 100
        monthly[month]["cost"] += nc
        if not g["fav_won"]:
            monthly[month]["no_wins"] += 1
            monthly[month]["pnl"] += 1.0 - nc
        else:
            monthly[month]["pnl"] -= nc

    mo_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    for m in sorted(monthly.keys()):
        s = monthly[m]
        if s["games"] < 10:
            continue
        no_pct = s["no_wins"] / s["games"] * 100
        roi = s["pnl"] / s["cost"] * 100 if s["cost"] > 0 else 0
        tag = " <-- APRIL" if m == 4 else (" <-- PLAYOFFS" if m in (5, 6) else "")
        print(f"    {mo_names[m]:3s}: {s['games']:>5d} games | NO wins {no_pct:>5.1f}% | ROI {roi:>+6.1f}%{tag}")

# ================================================================
# LOAD KAGGLE NBA DATA (2007-2025)
# ================================================================
print(f"\n\n{'='*85}")
print("KAGGLE NBA DATASET: 2007-2025 (17 seasons)")
print("='*85")

with open("data/sportsbook/nba_2008-2025.csv") as f:
    reader = csv.DictReader(f)
    kaggle_data = list(reader)

print(f"Total rows: {len(kaggle_data)}")

# Check which have moneylines
with_ml = [r for r in kaggle_data if r.get("moneyline_home") and r.get("moneyline_away")
           and r["moneyline_home"].strip() and r["moneyline_away"].strip()]
print(f"Rows with moneyline data: {len(with_ml)}")

if with_ml:
    # Analyze same way
    games = []
    for row in with_ml:
        try:
            home_ml = int(float(row["moneyline_home"]))
            away_ml = int(float(row["moneyline_away"]))
            home_score = int(row["score_home"])
            away_score = int(row["score_away"])
        except (ValueError, TypeError, KeyError):
            continue

        if home_score == away_score:
            continue

        home_prob = american_to_implied(home_ml)
        away_prob = american_to_implied(away_ml)
        if home_prob is None or away_prob is None:
            continue

        home_won = home_score > away_score
        home_cents = implied_to_kalshi_cents(home_prob)
        away_cents = implied_to_kalshi_cents(away_prob)

        if home_prob > away_prob:
            fav_cents = home_cents
            fav_won = home_won
            fav_location = "home"
        else:
            fav_cents = away_cents
            fav_won = not home_won
            fav_location = "away"

        is_playoff = row.get("playoffs", "0") == "1"

        games.append({
            "fav_cents": fav_cents,
            "fav_won": fav_won,
            "fav_location": fav_location,
            "is_playoff": is_playoff,
        })

    print(f"  Valid games: {len(games)}")
    print(f"  Regular season: {sum(1 for g in games if not g['is_playoff'])}")
    print(f"  Playoffs: {sum(1 for g in games if g['is_playoff'])}")

    # Per-price analysis
    print(f"\n  Per-price favorite win rate (17 seasons):")
    print(f"  {'Price':>5s} | {'Games':>6s} | {'Fav Win%':>8s} | {'NO Win%':>8s} | {'NO ROI':>7s}")

    for price in range(55, 96, 5):
        bucket = [g for g in games if price <= g["fav_cents"] < price + 5]
        if len(bucket) < 20:
            continue
        fav_wins = sum(1 for g in bucket if g["fav_won"])
        fav_pct = fav_wins / len(bucket) * 100
        no_pct = 100 - fav_pct
        total_cost = sum((100 - g["fav_cents"]) / 100 for g in bucket)
        total_pnl = sum(
            (1.0 - (100 - g["fav_cents"]) / 100) if not g["fav_won"]
            else -(100 - g["fav_cents"]) / 100
            for g in bucket
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        bar = "#" * max(0, int(roi / 5)) if roi > 0 else "-" * max(0, int(-roi / 5))
        print(f"  {price:>3d}-{price+4}c | {len(bucket):>6d} | {fav_pct:>7.1f}% | {no_pct:>7.1f}% | {roi:>+6.1f}% | {bar[:20]}")

    # Playoff vs Regular
    print(f"\n  Playoff vs Regular Season (61-90c):")
    for label, filt in [("Regular season", lambda g: not g["is_playoff"]),
                         ("Playoffs", lambda g: g["is_playoff"])]:
        bucket = [g for g in games if filt(g) and 61 <= g["fav_cents"] <= 90]
        if len(bucket) < 20:
            continue
        fav_wins = sum(1 for g in bucket if g["fav_won"])
        no_pct = (1 - fav_wins / len(bucket)) * 100
        total_cost = sum((100 - g["fav_cents"]) / 100 for g in bucket)
        total_pnl = sum(
            (1.0 - (100 - g["fav_cents"]) / 100) if not g["fav_won"]
            else -(100 - g["fav_cents"]) / 100
            for g in bucket
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"    {label:20s}: {len(bucket):>6d} games | NO wins {no_pct:>5.1f}% | ROI {roi:>+6.1f}%")

    # Home vs Away
    print(f"\n  Home vs Away favorite (61-90c):")
    for loc in ["home", "away"]:
        bucket = [g for g in games if g["fav_location"] == loc and 61 <= g["fav_cents"] <= 90]
        if len(bucket) < 20:
            continue
        fav_wins = sum(1 for g in bucket if g["fav_won"])
        no_pct = (1 - fav_wins / len(bucket)) * 100
        total_cost = sum((100 - g["fav_cents"]) / 100 for g in bucket)
        total_pnl = sum(
            (1.0 - (100 - g["fav_cents"]) / 100) if not g["fav_won"]
            else -(100 - g["fav_cents"]) / 100
            for g in bucket
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"    {loc.upper():5s} favorite: {len(bucket):>6d} games | NO wins {no_pct:>5.1f}% | ROI {roi:>+6.1f}%")

print("\n" + "=" * 85)
