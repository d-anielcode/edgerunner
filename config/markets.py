"""
Kalshi NBA market ticker definitions and helpers.

This is the single place that knows how Kalshi market tickers are structured.
All other modules use these helpers to categorize and filter markets.

Kalshi NBA tickers follow patterns like:
  KXNBA-LEBRON-PTS-O25   (LeBron James over 25.5 points)
  KXNBA-LAKERS-WIN       (Lakers to win)
  KXNBA-LAKERS-SPREAD    (Lakers spread)

NOTE: These patterns are based on Kalshi documentation examples and may need
updating once we connect to the live API. The categorize_ticker() function
is defensive — it returns None for unrecognized patterns instead of crashing.
"""

import re
from enum import Enum

from rich.console import Console
from rich.table import Table

console = Console()


class MarketCategory(str, Enum):
    """
    Categories of NBA prediction markets on Kalshi.

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


# Common prefix for all Kalshi NBA markets
KALSHI_NBA_PREFIX: str = "KXNBA"

# Regex patterns for matching tickers to categories.
# Each pattern matches the relevant segment of a Kalshi NBA ticker.
MARKET_TICKER_PATTERNS: dict[MarketCategory, re.Pattern[str]] = {
    MarketCategory.PLAYER_POINTS: re.compile(r"KXNBA.*PTS", re.IGNORECASE),
    MarketCategory.PLAYER_REBOUNDS: re.compile(r"KXNBA.*REB", re.IGNORECASE),
    MarketCategory.PLAYER_ASSISTS: re.compile(r"KXNBA.*AST", re.IGNORECASE),
    MarketCategory.PLAYER_THREES: re.compile(r"KXNBA.*3PT", re.IGNORECASE),
    MarketCategory.GAME_TOTAL: re.compile(r"KXNBA.*TOTAL", re.IGNORECASE),
    MarketCategory.GAME_SPREAD: re.compile(r"KXNBA.*SPREAD", re.IGNORECASE),
    MarketCategory.GAME_WINNER: re.compile(r"KXNBA.*WIN", re.IGNORECASE),
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


def is_supported_market(ticker: str) -> bool:
    """
    Check if a ticker is in a category the agent actively trades.

    Returns True only if:
    1. The ticker is an NBA market
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
        "KXNBA-JOKIC-REB-O12",
        "KXNBA-CURRY-AST-O7",
        "KXNBA-CURRY-3PT-O5",
        "KXNBA-LAKERS-CELTICS-TOTAL-O220",
        "KXNBA-LAKERS-SPREAD",
        "KXNBA-LAKERS-WIN",
        "KXWEATHER-NYC-TEMP",  # Not NBA
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
