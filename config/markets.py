"""
Kalshi sports market ticker definitions and helpers.

This is the single place that knows how Kalshi market tickers are structured.
All other modules use these helpers to categorize and filter markets.

Supported sports for "fade favorites" strategy:
  NBA:     KXNBAGAME-26APR03NOPSAC-NOP   (Oct-Jun)
  NHL:     KXNHLGAME-26APR03TORSEA-TOR   (Oct-Apr reg season)
  EPL:     KXEPLGAME-26JAN25ARSMUN-ARS   (Aug-May)
  UCL:     KXUCLGAME-26JAN28BENRMA-BEN   (Sep-Jun)
  La Liga: KXLALIGAGAME-26JAN18RSOBAR-RSO (Aug-May)
  WNBA:    KXWNBAGAME-25JUL15ATLCHI-ATL   (May-Oct)
  UFC:     KXUFCFIGHT-25NOV15PANTOP-PAN    (year-round)
"""

import re
from enum import Enum

from rich.console import Console
from rich.table import Table

console = Console()


class MarketCategory(str, Enum):
    """
    Categories of sports prediction markets on Kalshi.

    Why str + Enum? So the value can be stored directly in Supabase
    as a string without needing conversion.
    """

    PLAYER_POINTS = "player_points"
    PLAYER_REBOUNDS = "player_rebounds"
    PLAYER_ASSISTS = "player_assists"
    PLAYER_THREES = "player_threes"
    GAME_TOTAL = "game_total"
    GAME_SPREAD = "game_spread"
    GAME_WINNER = "game_winner"
    NHL_GAME_WINNER = "nhl_game_winner"
    EPL_GAME_WINNER = "epl_game_winner"
    UCL_GAME_WINNER = "ucl_game_winner"
    LALIGA_GAME_WINNER = "laliga_game_winner"
    WNBA_GAME_WINNER = "wnba_game_winner"
    UFC_FIGHT_WINNER = "ufc_fight_winner"
    NCAAMB_GAME_WINNER = "ncaamb_game_winner"
    NCAAWB_GAME_WINNER = "ncaawb_game_winner"
    WTA_MATCH_WINNER = "wta_match_winner"
    WEATHER_HIGH = "weather_high"
    CPI_INFLATION = "cpi_inflation"
    NFL_ANYTIME_TD = "nfl_anytime_td"
    NHL_SPREAD = "nhl_spread"
    NHL_FIRST_GOAL = "nhl_first_goal"
    NBA_SPREAD = "nba_spread"
    NBA_DOUBLE_DOUBLE = "nba_double_double"
    NFL_SPREAD = "nfl_spread"


# Prefixes for supported sports
KALSHI_NBA_PREFIX: str = "KXNBA"
KALSHI_NHL_PREFIX: str = "KXNHL"

# All sport prefixes we trade
SUPPORTED_SPORT_PREFIXES: list[str] = [
    KALSHI_NBA_PREFIX, KALSHI_NHL_PREFIX,
    "KXEPL", "KXUCL", "KXLALIGA", "KXWNBA", "KXUFC",
    "KXNCAAMB", "KXNCAAWB", "KXWTA",
    "KXHIGH", "CPI", "CPICORE", "CPICOREYOY",
    "KXNFLANYTD",
    "KXNHLSPREAD", "KXNHLFIRSTGOAL", "KXNBASPREAD", "KXNBA2D", "KXNFLSPREAD",
]

# Game winner ticker patterns we actively trade (fade favorites)
GAME_WINNER_PATTERNS: list[str] = [
    "KXNBAGAME", "KXNHLGAME",
    "KXEPLGAME", "KXUCLGAME", "KXLALIGAGAME",
    "KXWNBAGAME", "KXUFCFIGHT",
    "KXNCAAMBGAME", "KXNCAAWBGAME",
    "KXWTAMATCH",
    "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLA", "KXHIGHSF",
    "KXHIGHHOU", "KXHIGHDEN", "KXHIGHDC", "KXHIGHDAL",
    "CPI", "CPICORE", "CPICOREYOY",
    "KXNFLANYTD",
    "KXNHLSPREAD", "KXNHLFIRSTGOAL",
    "KXNBASPREAD", "KXNBA2D",
    "KXNFLSPREAD",
    "KXHIGHAUS", "KXHIGHPHIL",
]

# Regex patterns for matching tickers to categories.
MARKET_TICKER_PATTERNS: dict[MarketCategory, re.Pattern[str]] = {
    MarketCategory.PLAYER_POINTS: re.compile(r"KXNBA.*PTS", re.IGNORECASE),
    MarketCategory.PLAYER_REBOUNDS: re.compile(r"KXNBA.*REB", re.IGNORECASE),
    MarketCategory.PLAYER_ASSISTS: re.compile(r"KXNBA.*AST", re.IGNORECASE),
    MarketCategory.PLAYER_THREES: re.compile(r"KXNBA.*3PT", re.IGNORECASE),
    MarketCategory.GAME_TOTAL: re.compile(r"KXNBA.*TOTAL", re.IGNORECASE),
    MarketCategory.GAME_SPREAD: re.compile(r"KXNBA.*SPREAD", re.IGNORECASE),
    MarketCategory.GAME_WINNER: re.compile(r"KXNBA.*WIN", re.IGNORECASE),
    MarketCategory.NHL_GAME_WINNER: re.compile(r"KXNHLGAME", re.IGNORECASE),
    MarketCategory.EPL_GAME_WINNER: re.compile(r"KXEPLGAME", re.IGNORECASE),
    MarketCategory.UCL_GAME_WINNER: re.compile(r"KXUCLGAME", re.IGNORECASE),
    MarketCategory.LALIGA_GAME_WINNER: re.compile(r"KXLALIGAGAME", re.IGNORECASE),
    MarketCategory.WNBA_GAME_WINNER: re.compile(r"KXWNBAGAME", re.IGNORECASE),
    MarketCategory.UFC_FIGHT_WINNER: re.compile(r"KXUFCFIGHT", re.IGNORECASE),
    MarketCategory.NCAAMB_GAME_WINNER: re.compile(r"KXNCAAMBGAME", re.IGNORECASE),
    MarketCategory.NCAAWB_GAME_WINNER: re.compile(r"KXNCAAWBGAME", re.IGNORECASE),
    MarketCategory.WTA_MATCH_WINNER: re.compile(r"KXWTAMATCH", re.IGNORECASE),
    MarketCategory.WEATHER_HIGH: re.compile(r"KXHIGH", re.IGNORECASE),
    MarketCategory.CPI_INFLATION: re.compile(r"^CPI", re.IGNORECASE),
    MarketCategory.NFL_ANYTIME_TD: re.compile(r"KXNFLANYTD", re.IGNORECASE),
    MarketCategory.NHL_SPREAD: re.compile(r"KXNHLSPREAD", re.IGNORECASE),
    MarketCategory.NHL_FIRST_GOAL: re.compile(r"KXNHLFIRSTGOAL", re.IGNORECASE),
    MarketCategory.NBA_SPREAD: re.compile(r"KXNBASPREAD", re.IGNORECASE),
    MarketCategory.NBA_DOUBLE_DOUBLE: re.compile(r"KXNBA2D", re.IGNORECASE),
    MarketCategory.NFL_SPREAD: re.compile(r"KXNFLSPREAD", re.IGNORECASE),
}

# Which categories the agent actively trades in V1.
# Spread and total markets are live on Kalshi — enable them.
SUPPORTED_CATEGORIES: list[MarketCategory] = [
    MarketCategory.PLAYER_POINTS,
    MarketCategory.PLAYER_REBOUNDS,
    MarketCategory.PLAYER_ASSISTS,
    MarketCategory.GAME_SPREAD,
    MarketCategory.GAME_TOTAL,
]

# No trades within this many minutes of market close.
# Why? Late markets have thin liquidity and high slippage risk.
MIN_MINUTES_BEFORE_CLOSE: int = 5


def categorize_ticker(ticker: str) -> MarketCategory | None:
    """
    Match a Kalshi ticker string to a MarketCategory.

    Returns None if the ticker doesn't match any known pattern.
    This is intentionally defensive — unknown tickers are logged
    as warnings, not crashes.
    """
    for category, pattern in MARKET_TICKER_PATTERNS.items():
        if pattern.search(ticker):
            return category
    return None


def is_nba_market(ticker: str) -> bool:
    """Check if a ticker belongs to the Kalshi NBA market family."""
    return ticker.upper().startswith(KALSHI_NBA_PREFIX)


def is_nhl_market(ticker: str) -> bool:
    """Check if a ticker belongs to the Kalshi NHL market family."""
    return ticker.upper().startswith(KALSHI_NHL_PREFIX)


def is_sports_market(ticker: str) -> bool:
    """Check if a ticker belongs to any supported sport."""
    upper = ticker.upper()
    return any(upper.startswith(p) for p in SUPPORTED_SPORT_PREFIXES)


def is_game_winner(ticker: str) -> bool:
    """Check if a ticker is a game/fight winner market we trade."""
    upper = ticker.upper()
    return any(p in upper for p in GAME_WINNER_PATTERNS)


def get_sport(ticker: str) -> str | None:
    """Return the sport identifier for a ticker."""
    upper = ticker.upper()
    if "KXNBAGAME" in upper:
        return "NBA"
    if "KXNHLGAME" in upper:
        return "NHL"
    if "KXEPLGAME" in upper:
        return "EPL"
    if "KXUCLGAME" in upper:
        return "UCL"
    if "KXLALIGAGAME" in upper:
        return "LALIGA"
    if "KXWNBAGAME" in upper:
        return "WNBA"
    if "KXUFCFIGHT" in upper:
        return "UFC"
    if "KXNCAAMBGAME" in upper:
        return "NCAAMB"
    if "KXNCAAWBGAME" in upper:
        return "NCAAWB"
    if "KXWTAMATCH" in upper:
        return "WTA"
    if "KXHIGH" in upper:
        return "WEATHER"
    if upper.startswith("CPI"):
        return "CPI"
    if "KXNFLANYTD" in upper:
        return "NFLTD"
    if "KXNHLSPREAD" in upper:
        return "NHLSPREAD"
    if "KXNHLFIRSTGOAL" in upper:
        return "NHLFG"
    if "KXNBASPREAD" in upper:
        return "NBASPREAD"
    if "KXNBA2D" in upper:
        return "NBA2D"
    if "KXNFLSPREAD" in upper:
        return "NFLSPREAD"
    # Fallback to prefix matching
    if upper.startswith(KALSHI_NBA_PREFIX):
        return "NBA"
    if upper.startswith(KALSHI_NHL_PREFIX):
        return "NHL"
    return None


def is_supported_market(ticker: str) -> bool:
    """
    Check if a ticker is in a category the agent actively trades.

    Returns True only if:
    1. The ticker is a supported sport market
    2. The ticker matches a category in SUPPORTED_CATEGORIES
    """
    category = categorize_ticker(ticker)
    return category is not None and category in SUPPORTED_CATEGORIES


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    # Print all categories
    table = Table(title="Market Categories")
    table.add_column("Category", style="cyan")
    table.add_column("Value", style="green")
    table.add_column("Supported", style="yellow")

    for cat in MarketCategory:
        supported = "YES" if cat in SUPPORTED_CATEGORIES else "no"
        table.add_row(cat.name, cat.value, supported)

    console.print(table)

    # Test categorization
    console.print("\n[bold]Ticker Categorization Tests:[/bold]")
    test_tickers = [
        "KXNBA-LEBRON-PTS-O25",
        "KXNBA-LAKERS-WIN",
        "KXNHLGAME-26APR06TORSEA-TOR",
        "KXEPLGAME-26JAN25ARSMUN-ARS",
        "KXUCLGAME-26JAN28BENRMA-BEN",
        "KXLALIGAGAME-26JAN18RSOBAR-RSO",
        "KXWNBAGAME-25JUL15ATLCHI-ATL",
        "KXUFCFIGHT-25NOV15PANTOP-PAN",
        "KXWEATHER-NYC-TEMP",  # Not sports
        "RANDOM-TICKER",  # Unknown
    ]

    for ticker in test_tickers:
        cat = categorize_ticker(ticker)
        nba = is_nba_market(ticker)
        supported = is_supported_market(ticker)
        cat_str = cat.value if cat else "None"
        color = "green" if supported else ("yellow" if nba else "red")
        console.print(
            f"  [{color}]{ticker:40s} -> {cat_str:20s} "
            f"NBA={nba!s:5s} Supported={supported}[/{color}]"
        )

    console.print("\n[green]config/markets.py: All tests passed.[/green]")
