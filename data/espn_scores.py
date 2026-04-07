"""
ESPN live scores feed for EdgeRunner.

Fetches live NBA and NHL game scores from ESPN's free public API.
No API key needed, no rate limits documented.

This data powers:
- Quarter-aware player prop stop-loss system (NBA)
- Period-aware blowout veto (NBA Q3/Q4 + NHL P3)
- End-of-game trade blocking
"""

import asyncio
from dataclasses import dataclass

import aiohttp
from rich.console import Console

console = Console()

ESPN_NBA_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_NHL_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"


@dataclass
class LiveGameState:
    """Live state of a game from ESPN."""

    home_team: str  # e.g., "DEN"
    away_team: str  # e.g., "SA"
    home_score: int
    away_score: int
    quarter: int  # NBA: 1-4, 5+ OT | NHL: 1-3, 4+ OT
    clock: str  # e.g., "4:10", "0.0"
    status: str  # "Scheduled", "In Progress", "Final", "Halftime"
    game_id: str
    sport: str = "NBA"  # "NBA" or "NHL"


async def fetch_live_scores() -> dict[str, LiveGameState]:
    """
    Fetch all live NBA and NHL game scores from ESPN.

    Returns a dict keyed by a normalized game key (e.g., "SASDEN")
    that matches Kalshi's game ID format.

    Free, no auth, no API key needed.
    """
    games: dict[str, LiveGameState] = {}

    # NBA team abbreviation normalization
    nba_team_map = {
        "SA": "SAS", "GS": "GSW", "NY": "NYK", "NO": "NOP",
        "WSH": "WAS", "UTAH": "UTA", "PHX": "PHO",
    }

    # NHL team abbreviation normalization (ESPN vs Kalshi)
    nhl_team_map = {
        "TB": "TBL", "NJ": "NJD", "SJ": "SJS", "LA": "LAK",
    }

    for url, sport, team_map in [
        (ESPN_NBA_SCOREBOARD_URL, "NBA", nba_team_map),
        (ESPN_NHL_SCOREBOARD_URL, "NHL", nhl_team_map),
    ]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
        except Exception as e:
            console.print(f"[yellow]ESPN {sport} scores error: {type(e).__name__}: {e}[/yellow]")
            continue

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

                period = status_data.get("period", 0)
                clock = status_data.get("displayClock", "0:00")
                state = status_data["type"]["description"]

                home_norm = team_map.get(home, home)
                away_norm = team_map.get(away, away)

                # Build game key matching Kalshi format: AWYHOM (e.g., SASDEN)
                game_key = away_norm + home_norm

                game = LiveGameState(
                    home_team=home_norm,
                    away_team=away_norm,
                    home_score=home_score,
                    away_score=away_score,
                    quarter=period,
                    clock=clock,
                    status=state,
                    game_id=event.get("id", ""),
                    sport=sport,
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
    Extract the current period for a game from ESPN data,
    matching against a Kalshi ticker.

    Returns period number (NBA: 1-4, 5+ OT | NHL: 1-3, 4+ OT) or None.
    """
    import re

    # Extract game ID from ticker (NBA or NHL)
    match = re.search(r"KX(?:NBA|NHL)\w*-\d{2}[A-Z]{3}\d{2}([A-Z]{6})", ticker.upper())
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
        console.print("[bold]Testing data/espn_scores.py (NBA + NHL)...[/bold]\n")

        games = await fetch_live_scores()

        nba_games = [g for k, g in games.items() if len(k) == 6 and g.sport == "NBA"]
        nhl_games = [g for k, g in games.items() if len(k) == 6 and g.sport == "NHL"]
        console.print(f"NBA games: {len(nba_games)} | NHL games: {len(nhl_games)}")

        for key, game in games.items():
            if len(key) == 6:  # Only print one direction
                period_label = "Q" if game.sport == "NBA" else "P"
                console.print(
                    f"  [{game.sport}] {game.away_team} {game.away_score} @ "
                    f"{game.home_team} {game.home_score} | "
                    f"{period_label}{game.quarter} {game.clock} | {game.status}"
                )

        # Test quarter extraction for both sports
        console.print("\nPeriod extraction tests:")
        test_tickers = [
            "KXNBAPTS-26APR04SASDEN-SASDVASSELL24-25",
            "KXNBAGAME-26APR04WASMIA-MIA",
            "KXNHLGAME-26APR06TORSEA-TOR",
        ]
        for ticker in test_tickers:
            q = get_quarter_from_game(games, ticker)
            console.print(f"  {ticker[:45]} -> period={q}")

        console.print("\n[green]data/espn_scores.py: Test complete.[/green]")

    asyncio.run(_test())
