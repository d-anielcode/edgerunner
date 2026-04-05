"""
ESPN NBA standings and schedule data for EdgeRunner.

Provides team win/loss records, home/away splits, and back-to-back
detection. Used by the rules evaluator as veto filters — if a team
has a dominant record that justifies the high price, skip the fade.

Free ESPN API, no auth needed.
"""

import asyncio
from dataclasses import dataclass

import aiohttp
from rich.console import Console

console = Console()

ESPN_STANDINGS_URL = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
ESPN_SCHEDULE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"


@dataclass
class TeamRecord:
    """NBA team record from ESPN."""

    team_abbr: str
    team_name: str
    wins: int
    losses: int
    win_pct: float
    home_wins: int = 0
    home_losses: int = 0
    away_wins: int = 0
    away_losses: int = 0
    streak: int = 0  # Positive = winning streak, negative = losing
    conference_rank: int = 0


async def fetch_standings() -> dict[str, TeamRecord]:
    """
    Fetch current NBA standings from ESPN.

    Returns a dict keyed by team abbreviation (e.g., "BOS", "LAL").
    Free, no auth needed.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                ESPN_STANDINGS_URL, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
    except Exception as e:
        console.print(f"[yellow]ESPN standings error: {e}[/yellow]")
        return {}

    teams: dict[str, TeamRecord] = {}

    try:
        for group in data.get("children", []):
            for entry in group.get("standings", {}).get("entries", []):
                team_info = entry.get("team", {})
                abbr = team_info.get("abbreviation", "")
                name = team_info.get("displayName", "")

                # Parse stats
                stats = {}
                for stat in entry.get("stats", []):
                    stats[stat.get("name", "")] = stat.get("value", 0)

                wins = int(stats.get("wins", 0))
                losses = int(stats.get("losses", 0))
                total = wins + losses
                win_pct = wins / total if total > 0 else 0.0

                # Home/away splits
                home_record = stats.get("Home", "0-0")
                away_record = stats.get("Road", stats.get("Away", "0-0"))

                hw, hl = 0, 0
                aw, al = 0, 0
                if isinstance(home_record, str) and "-" in home_record:
                    parts = home_record.split("-")
                    hw, hl = int(parts[0]), int(parts[1])
                if isinstance(away_record, str) and "-" in away_record:
                    parts = away_record.split("-")
                    aw, al = int(parts[0]), int(parts[1])

                # Streak
                streak_val = int(stats.get("streak", 0))

                record = TeamRecord(
                    team_abbr=abbr,
                    team_name=name,
                    wins=wins,
                    losses=losses,
                    win_pct=round(win_pct, 3),
                    home_wins=hw,
                    home_losses=hl,
                    away_wins=aw,
                    away_losses=al,
                    streak=streak_val,
                )

                if abbr:
                    # Normalize abbreviations
                    norm = {
                        "SA": "SAS", "GS": "GSW", "NY": "NYK",
                        "NO": "NOP", "WSH": "WAS", "UTAH": "UTA",
                    }
                    abbr = norm.get(abbr, abbr)
                    teams[abbr] = record

    except (KeyError, TypeError, ValueError):
        pass

    return teams


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        console.print("[bold]Testing data/espn_standings.py...[/bold]\n")

        standings = await fetch_standings()
        console.print(f"Teams found: {len(standings)}")

        if standings:
            # Print top 5 by win %
            sorted_teams = sorted(
                standings.values(), key=lambda t: t.win_pct, reverse=True
            )
            console.print("\nTop 5 teams:")
            for t in sorted_teams[:5]:
                console.print(
                    f"  {t.team_abbr} ({t.team_name}): {t.wins}-{t.losses} "
                    f"({t.win_pct:.3f}) | Streak: {t.streak}"
                )

            console.print("\nBottom 5 teams:")
            for t in sorted_teams[-5:]:
                console.print(
                    f"  {t.team_abbr} ({t.team_name}): {t.wins}-{t.losses} "
                    f"({t.win_pct:.3f})"
                )

        console.print("\n[green]data/espn_standings.py: Test complete.[/green]")

    asyncio.run(_test())
