"""
EdgeRunner 24/7 Runner — Auto-restarts the agent between sessions.

How it works:
1. Checks if any games are happening today (NBA, NHL, EPL, etc.)
2. If yes, starts the agent ~30 min before the first game
3. Agent runs until auto-shutdown (after last game ends)
4. Runner waits, then checks for next game window
5. Repeats forever

Usage:
    python runner.py          # Run forever (Ctrl+C to stop)
    python runner.py --now    # Start agent immediately (skip schedule)

To run as a background process on Windows:
    start /min python runner.py
"""

import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import aiohttp

# ESPN schedule endpoints (free, no auth)
ESPN_ENDPOINTS = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "EPL": "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
    "UFC": "https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard",
}

# How early to start before first game (minutes)
EARLY_START_MINUTES = 30

# How long to wait between schedule checks when no games (minutes)
IDLE_CHECK_INTERVAL = 60

# Max consecutive crashes before giving up
MAX_CRASHES = 5


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] RUNNER: {msg}")


async def get_todays_games() -> dict[str, list[dict]]:
    """Check ESPN for today's games across all sports."""
    games_by_sport = {}

    async with aiohttp.ClientSession() as session:
        for sport, url in ESPN_ENDPOINTS.items():
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                events = data.get("events", [])
                sport_games = []
                for event in events:
                    try:
                        comp = event["competitions"][0]
                        status = comp["status"]["type"]["description"]
                        start_time = event.get("date", "")

                        # Parse start time
                        if start_time:
                            # ESPN returns ISO format
                            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                        else:
                            dt = None

                        sport_games.append({
                            "name": event.get("name", "Unknown"),
                            "status": status,
                            "start_time": dt,
                        })
                    except (KeyError, IndexError):
                        continue

                if sport_games:
                    games_by_sport[sport] = sport_games

            except Exception as e:
                log(f"ESPN {sport} check failed: {e}")

    return games_by_sport


async def get_next_game_time() -> datetime | None:
    """Find the earliest upcoming game start time across all sports."""
    games = await get_todays_games()
    now = datetime.now(timezone.utc)

    earliest = None
    for sport, sport_games in games.items():
        for game in sport_games:
            dt = game.get("start_time")
            if dt and dt > now:
                if earliest is None or dt < earliest:
                    earliest = dt

    return earliest


def run_agent() -> int:
    """Run the EdgeRunner agent as a subprocess. Returns exit code."""
    log("Starting EdgeRunner agent...")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        result = subprocess.run(
            [sys.executable, "main.py"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env,
        )
        return result.returncode
    except KeyboardInterrupt:
        log("Agent interrupted by user.")
        return 0
    except Exception as e:
        log(f"Agent crashed: {e}")
        return 1


async def main_loop(start_now: bool = False) -> None:
    """Main 24/7 loop."""
    consecutive_crashes = 0

    log("EdgeRunner 24/7 Runner started.")
    log(f"Monitoring sports: {', '.join(ESPN_ENDPOINTS.keys())}")

    while True:
        try:
            if not start_now:
                # Check for games
                log("Checking ESPN for today's games...")
                games = await get_todays_games()

                total_games = sum(len(g) for g in games.values())
                if total_games > 0:
                    for sport, sport_games in games.items():
                        scheduled = [g for g in sport_games if g["status"] in ("Scheduled", "Pre-Event")]
                        in_progress = [g for g in sport_games if g["status"] == "In Progress"]
                        if scheduled or in_progress:
                            log(f"  {sport}: {len(scheduled)} scheduled, {len(in_progress)} live")

                # Find next game start
                next_game = await get_next_game_time()

                if next_game is None:
                    # Check for in-progress games
                    any_live = any(
                        any(g["status"] == "In Progress" for g in sport_games)
                        for sport_games in games.values()
                    )

                    if any_live:
                        log("Games in progress! Starting agent now.")
                    else:
                        log(f"No upcoming games found. Checking again in {IDLE_CHECK_INTERVAL} min...")
                        await asyncio.sleep(IDLE_CHECK_INTERVAL * 60)
                        continue
                else:
                    now = datetime.now(timezone.utc)
                    start_at = next_game - timedelta(minutes=EARLY_START_MINUTES)
                    wait_seconds = (start_at - now).total_seconds()

                    if wait_seconds > 0:
                        local_start = start_at.astimezone()
                        local_game = next_game.astimezone()
                        log(f"Next game at {local_game.strftime('%I:%M %p')}. "
                            f"Agent starts at {local_start.strftime('%I:%M %p')} "
                            f"(in {wait_seconds/60:.0f} min)")

                        # Sleep until start time, checking every 5 min
                        while wait_seconds > 0:
                            sleep_time = min(wait_seconds, 300)
                            await asyncio.sleep(sleep_time)
                            wait_seconds -= sleep_time
                    else:
                        log("Game starting soon or already started!")

            # Run the agent
            start_now = False  # Only skip schedule on first run if --now
            exit_code = run_agent()

            if exit_code == 0:
                log("Agent shut down cleanly (session ended).")
                consecutive_crashes = 0
            else:
                consecutive_crashes += 1
                log(f"Agent exited with code {exit_code} "
                    f"(crash {consecutive_crashes}/{MAX_CRASHES})")

                if consecutive_crashes >= MAX_CRASHES:
                    log(f"Too many crashes ({MAX_CRASHES}). Stopping runner.")
                    break

                # Wait before restart on crash
                wait = min(60 * consecutive_crashes, 300)
                log(f"Restarting in {wait}s...")
                await asyncio.sleep(wait)
                continue

            # Post-session: wait a bit then check for more games
            log("Session complete. Checking for more games in 10 min...")
            await asyncio.sleep(600)

        except KeyboardInterrupt:
            log("Runner stopped by user.")
            break
        except Exception as e:
            log(f"Runner error: {e}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    start_now = "--now" in sys.argv
    try:
        asyncio.run(main_loop(start_now=start_now))
    except KeyboardInterrupt:
        log("Goodbye.")
