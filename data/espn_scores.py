"""
ESPN live scores feed for EdgeRunner.

Fetches live NBA game scores, quarter, and clock from ESPN's free
public API. No API key needed, no rate limits documented.

This data powers the quarter-aware player prop stop-loss system.
Without knowing the game clock, the agent can't distinguish between
"player is cold in Q1" (recoverable) and "player has 14 points with
4 min left" (unrecoverable).
"""

import asyncio
from dataclasses import dataclass

import aiohttp
from rich.console import Console

console = Console()

ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"


@dataclass
class LiveGameState:
    """Live state of an NBA game from ESPN."""

    home_team: str  # e.g., "DEN"
    away_team: str  # e.g., "SA"
    home_score: int
    away_score: int
    quarter: int  # 1-4, 5+ for OT
    clock: str  # e.g., "4:10", "0.0"
    status: str  # "Scheduled", "In Progress", "Final", "Halftime"
    game_id: str


async def fetch_live_scores() -> dict[str, LiveGameState]:
    """
    Fetch all live NBA game scores from ESPN.

    Returns a dict keyed by a normalized game key (e.g., "SASDEN")
    that matches Kalshi's game ID format.

    Free, no auth, no API key needed.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                ESPN_SCOREBOARD_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
    except Exception as e:
        console.print(f"[yellow]ESPN scores error: {type(e).__name__}: {e}[/yellow]")
        return {}

    games: dict[str, LiveGameState] = {}

    for event in data.get("events", []):
        try:
            competition = event["competitions"][0]
            status_data = competition["status"]
            competitors = competition["competitors"]

            # ESPN: competitors[0] = home, competitors[1] = away
            home = competitors[0]["team"]["abbreviation"]
            away = competitors[1]["team"]["abbreviation"]
            home_score = int(competitors[0].get("score", 0))
            away_score = int(competitors[1].get("score", 0))

            quarter = status_data.get("period", 0)
            clock = status_data.get("displayClock", "0:00")
            state = status_data["type"]["description"]

            # Normalize team abbreviations to match Kalshi format
            # ESPN uses "SA" for Spurs, Kalshi uses "SAS"
            team_map = {
                "SA": "SAS", "GS": "GSW", "NY": "NYK", "NO": "NOP",
                "WSH": "WAS", "UTAH": "UTA", "PHX": "PHO",
            }
            home_norm = team_map.get(home, home)
            away_norm = team_map.get(away, away)

            # Build game key matching Kalshi format: AWYHOM (e.g., SASDEN)
            game_key = away_norm + home_norm

            game = LiveGameState(
                home_team=home_norm,
                away_team=away_norm,
                home_score=home_score,
                away_score=away_score,
                quarter=quarter,
                clock=clock,
                status=state,
                game_id=event.get("id", ""),
            )
            games[game_key] = game

            # Also store reversed key (HOMAWAY) for matching
            games[home_norm + away_norm] = game

        except (KeyError, IndexError, TypeError):
            continue

    return games


def get_quarter_from_game(
    game_states: dict[str, LiveGameState], ticker: str
) -> int | None:
    """
    Extract the current quarter for a game from ESPN data,
    matching against a Kalshi ticker.

    Returns quarter (1-4, 5+ for OT) or None if not found.
    """
    import re

    # Extract game ID from ticker (e.g., KXNBAPTS-26APR04SASDEN-... -> SASDEN)
    match = re.search(r"KXNBA\w*-\d{2}[A-Z]{3}\d{2}([A-Z]{6})", ticker.upper())
    if not match:
        return None

    game_id = match.group(1)
    game = game_states.get(game_id)
    if game and game.status == "In Progress":
        return game.quarter

    return None


def parse_clock_minutes(clock: str) -> float:
    """Convert clock string like '4:10' to minutes remaining (4.17)."""
    try:
        parts = clock.split(":")
        if len(parts) == 2:
            return int(parts[0]) + int(parts[1]) / 60
        return float(clock)
    except (ValueError, IndexError):
        return 0.0


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        console.print("[bold]Testing data/espn_scores.py...[/bold]\n")

        games = await fetch_live_scores()
        console.print(f"Games found: {len(games) // 2}")  # Divided by 2 since we store both key directions

        for key, game in games.items():
            if len(key) == 6:  # Only print one direction
                console.print(
                    f"  {game.away_team} {game.away_score} @ "
                    f"{game.home_team} {game.home_score} | "
                    f"Q{game.quarter} {game.clock} | {game.status}"
                )

        # Test quarter extraction
        console.print("\nQuarter extraction tests:")
        test_tickers = [
            "KXNBAPTS-26APR04SASDEN-SASDVASSELL24-25",
            "KXNBAGAME-26APR04WASMIA-MIA",
            "KXNBASPREAD-26APR04DETPHI-PHI14",
        ]
        for ticker in test_tickers:
            q = get_quarter_from_game(games, ticker)
            console.print(f"  {ticker[:40]} -> Q{q}")

        console.print("\n[green]data/espn_scores.py: Test complete.[/green]")

    asyncio.run(_test())
