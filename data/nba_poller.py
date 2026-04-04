"""
NBA data poller for EdgeRunner.

Polls NBA data from two free sources on a fixed interval:
1. BallDontLie REST API — live game scores, player game logs (async via aiohttp)
2. nba_api package — season averages, historical stats (sync, wrapped in executor)

This is the agent's domain knowledge feed. When combined with Kalshi
orderbook data, it lets the agent detect when real-world events (injuries,
hot streaks) haven't been priced into the market yet.

Design decisions:
- nba_api uses `requests` internally (blocking). We wrap it with
  asyncio.run_in_executor() to avoid blocking the event loop. This is
  the standard pattern for sync libraries in async code.
- BallDontLie free tier has rate limits. We handle 429s gracefully.
- Polling interval is configurable via NBA_POLL_INTERVAL in .env.
"""

import asyncio
from datetime import datetime, timezone

import aiohttp
from rich.console import Console

from config.settings import BALLDONTLIE_API_KEY, DEBUG_MODE, NBA_POLL_INTERVAL
from data.cache import AgentCache, NbaGameUpdate, NbaStatsUpdate, QueueMsg

console = Console()
UTC = timezone.utc


class NbaPoller:
    """
    Periodically polls NBA data APIs and pushes updates to the queue and cache.

    Two data sources:
    - BallDontLie REST API (async, free tier): live games, player game logs
    - nba_api package (sync in executor): season averages, career stats

    Usage:
        poller = NbaPoller(queue=queue, cache=cache)
        await poller.run()  # runs until stop() is called
    """

    def __init__(
        self,
        queue: asyncio.Queue[QueueMsg],
        cache: AgentCache,
        tracked_players: list[dict] | None = None,
    ) -> None:
        """
        Args:
            queue: Shared asyncio.Queue for pushing updates to signal evaluator.
            cache: AgentCache singleton for storing latest data.
            tracked_players: List of dicts with 'name' and optionally 'id' keys.
                Example: [{"name": "LeBron James", "id": 2544}]
                In V1, this is set at startup. In V2, it syncs with market tickers.
        """
        self._queue = queue
        self._cache = cache
        self._tracked_players = tracked_players or []
        self._poll_interval = NBA_POLL_INTERVAL
        self._session: aiohttp.ClientSession | None = None
        self._running: bool = False
        self._bdl_base_url = "https://api.balldontlie.io/v1"

    # --- BallDontLie REST API (async) ---

    def _bdl_headers(self) -> dict[str, str]:
        """Build headers for BallDontLie API requests."""
        headers: dict[str, str] = {"Accept": "application/json"}
        if BALLDONTLIE_API_KEY:
            headers["Authorization"] = BALLDONTLIE_API_KEY
        return headers

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._bdl_headers())
        return self._session

    async def _fetch_live_games(self) -> list[dict]:
        """
        Fetch today's NBA games from BallDontLie.

        Endpoint: GET /v1/games?dates[]={today}
        Returns a list of game dicts. Empty list on failure.
        """
        session = await self._ensure_session()
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        url = f"{self._bdl_base_url}/games"
        params = {"dates[]": today}

        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 429:
                    console.print("[yellow]BallDontLie: Rate limited. Backing off.[/yellow]")
                    return []
                if resp.status != 200:
                    console.print(
                        f"[yellow]BallDontLie games: HTTP {resp.status}[/yellow]"
                    )
                    return []
                data = await resp.json()
                return data.get("data", [])
        except asyncio.TimeoutError:
            console.print("[yellow]BallDontLie games: Request timed out.[/yellow]")
            return []
        except Exception as e:
            console.print(
                f"[red]BallDontLie games error: {type(e).__name__}: {e}[/red]"
            )
            return []

    async def _fetch_player_game_log(self, player_id: int) -> list[dict]:
        """
        Fetch recent game logs for a player from BallDontLie.

        Endpoint: GET /v1/stats?player_ids[]={id}&per_page=5
        Returns last 5 games of stats. Empty list on failure.
        """
        session = await self._ensure_session()
        url = f"{self._bdl_base_url}/stats"
        params = {"player_ids[]": player_id, "per_page": 5}

        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 429:
                    console.print("[yellow]BallDontLie: Rate limited.[/yellow]")
                    return []
                if resp.status != 200:
                    console.print(
                        f"[yellow]BallDontLie stats: HTTP {resp.status}[/yellow]"
                    )
                    return []
                data = await resp.json()
                return data.get("data", [])
        except asyncio.TimeoutError:
            console.print("[yellow]BallDontLie stats: Request timed out.[/yellow]")
            return []
        except Exception as e:
            console.print(
                f"[red]BallDontLie stats error: {type(e).__name__}: {e}[/red]"
            )
            return []

    # --- nba_api (sync, wrapped in executor) ---

    async def _fetch_player_season_stats(self, player_name: str) -> dict | None:
        """
        Fetch season averages from nba_api.

        IMPORTANT: nba_api uses `requests` internally (blocking).
        We run it in asyncio's default ThreadPoolExecutor to avoid
        blocking the event loop. This is the standard escape hatch
        for sync libraries in async code.
        """
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None,  # default ThreadPoolExecutor
                self._sync_fetch_player_stats,
                player_name,
            )
            return result
        except Exception as e:
            console.print(
                f"[red]nba_api error for {player_name}: {type(e).__name__}: {e}[/red]"
            )
            return None

    def _sync_fetch_player_stats(self, player_name: str) -> dict | None:
        """
        Synchronous nba_api call. Runs in thread executor.

        Fetches player career stats and extracts the most recent season.
        Returns a dict with player_id, pts, reb, ast or None on failure.
        """
        from nba_api.stats.endpoints import playercareerstats
        from nba_api.stats.static import players, teams

        player_list = players.find_players_by_full_name(player_name)
        if not player_list:
            if DEBUG_MODE:
                console.print(f"[dim]nba_api: Player '{player_name}' not found.[/dim]")
            return None

        player_id = player_list[0]["id"]

        career = playercareerstats.PlayerCareerStats(player_id=player_id)
        df = career.get_data_frames()[0]
        if df.empty:
            return None

        latest = df.iloc[-1]
        gp = int(latest.get("GP", 1)) or 1  # avoid division by zero

        # Get team abbreviation from the most recent season
        team_abbr = ""
        team_id = latest.get("TEAM_ID")
        if team_id:
            try:
                team_info = teams.find_team_name_by_id(int(team_id))
                if team_info:
                    team_abbr = team_info.get("abbreviation", "")
            except Exception:
                pass

        return {
            "player_id": player_id,
            "player_name": player_name,
            "team": team_abbr,
            "pts": round(float(latest.get("PTS", 0)) / gp, 1),
            "reb": round(float(latest.get("REB", 0)) / gp, 1),
            "ast": round(float(latest.get("AST", 0)) / gp, 1),
            "gp": gp,
        }

    # --- Polling Cycle ---

    async def _poll_games(self) -> None:
        """Fetch today's games and push updates to cache and queue."""
        games_raw = await self._fetch_live_games()

        for game_raw in games_raw:
            try:
                home_team = game_raw.get("home_team", {})
                away_team = game_raw.get("visitor_team", {})

                game_update = NbaGameUpdate(
                    timestamp=datetime.now(UTC),
                    game_id=game_raw.get("id", 0),
                    home_team=home_team.get("abbreviation", "???"),
                    away_team=away_team.get("abbreviation", "???"),
                    home_score=game_raw.get("home_team_score", 0),
                    away_score=game_raw.get("visitor_team_score", 0),
                    period=game_raw.get("period", 0),
                    status=game_raw.get("status", "Unknown"),
                    game_time=game_raw.get("time", ""),
                )
                self._cache.update_live_game(game_update)
                await self._queue.put(game_update)
            except Exception as e:
                if DEBUG_MODE:
                    console.print(f"[dim]Game parse error: {e}[/dim]")

        if games_raw:
            console.print(f"[blue]NBA Poller: {len(games_raw)} games fetched.[/blue]")

    async def _poll_player_stats(self) -> None:
        """Fetch stats for all tracked players and push to cache and queue."""
        for player_info in self._tracked_players:
            player_name = player_info.get("name", "")
            if not player_name:
                continue

            # Fetch season averages from nba_api (runs in executor)
            stats = await self._fetch_player_season_stats(player_name)
            if stats is None:
                continue

            player_id = stats["player_id"]

            # Fetch recent game logs from BallDontLie
            game_logs = await self._fetch_player_game_log(player_id)

            recent_pts = [float(g.get("pts", 0)) for g in game_logs[:5]]
            recent_reb = [float(g.get("reb", 0)) for g in game_logs[:5]]
            recent_ast = [float(g.get("ast", 0)) for g in game_logs[:5]]

            stats_update = NbaStatsUpdate(
                timestamp=datetime.now(UTC),
                player_name=player_name,
                player_id=player_id,
                team=stats.get("team", ""),
                season_avg_pts=stats["pts"],
                season_avg_reb=stats["reb"],
                season_avg_ast=stats["ast"],
                recent_game_pts=recent_pts,
                recent_game_reb=recent_reb,
                recent_game_ast=recent_ast,
                status="Active",
            )

            self._cache.update_player_stats(stats_update)
            await self._queue.put(stats_update)

            # Small delay between players to avoid rate limiting
            await asyncio.sleep(1.0)

        if self._tracked_players:
            console.print(
                f"[blue]NBA Poller: {len(self._tracked_players)} players updated.[/blue]"
            )

    async def _poll_cycle(self) -> None:
        """
        One full polling cycle: fetch games, then player stats.

        Games are always fetched. Player stats are only fetched if
        there are tracked players configured.
        """
        await self._poll_games()

        if self._tracked_players:
            await self._poll_player_stats()

    # --- Main Loop ---

    async def run(self) -> None:
        """
        Main entry point. Polls on a fixed interval forever.

        Designed to be passed to asyncio.gather() in main.py.
        Errors in individual poll cycles are caught and logged —
        the poller keeps running.
        """
        self._running = True
        console.print(
            f"[blue]NBA Poller: Starting (interval={self._poll_interval}s, "
            f"players={len(self._tracked_players)}).[/blue]"
        )

        try:
            while self._running:
                try:
                    await self._poll_cycle()
                except Exception as e:
                    console.print(
                        f"[red]NBA Poller cycle error: {type(e).__name__}: {e}[/red]"
                    )

                await asyncio.sleep(self._poll_interval)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def stop(self) -> None:
        """Signal the poller to stop and close the HTTP session."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        console.print("[blue]NBA Poller: Stopped.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """
        Test the NBA data poller.

        Tests BallDontLie API (live games) and nba_api (player stats).
        Both may fail gracefully if APIs are down or rate limited.
        """
        from data.cache import get_cache

        console.print("[bold]Testing data/nba_poller.py...[/bold]\n")

        cache = get_cache()
        queue: asyncio.Queue[QueueMsg] = asyncio.Queue(maxsize=100)

        poller = NbaPoller(
            queue=queue,
            cache=cache,
            tracked_players=[
                {"name": "LeBron James"},
                {"name": "Stephen Curry"},
            ],
        )

        # Test 1: Fetch live games from BallDontLie
        console.print("[cyan]1. BallDontLie — Today's games:[/cyan]")
        games = await poller._fetch_live_games()
        if games:
            for g in games[:5]:
                home = g.get("home_team", {}).get("abbreviation", "?")
                away = g.get("visitor_team", {}).get("abbreviation", "?")
                status = g.get("status", "?")
                console.print(f"   {away} @ {home} — {status}")
        else:
            console.print("   [yellow]No games today (or API unavailable).[/yellow]")

        # Test 2: Fetch player season stats from nba_api
        console.print("\n[cyan]2. nba_api — Player season stats:[/cyan]")
        for player_name in ["LeBron James", "Stephen Curry"]:
            stats = await poller._fetch_player_season_stats(player_name)
            if stats:
                console.print(
                    f"   [green]{stats['player_name']}: "
                    f"{stats['pts']} PPG, {stats['reb']} RPG, "
                    f"{stats['ast']} APG ({stats['gp']} GP)[/green]"
                )
            else:
                console.print(
                    f"   [yellow]{player_name}: Could not fetch stats.[/yellow]"
                )

        # Test 3: Fetch player game logs from BallDontLie
        console.print("\n[cyan]3. BallDontLie — Recent game logs:[/cyan]")
        # Use LeBron's known BallDontLie ID (if available)
        game_logs = await poller._fetch_player_game_log(237)  # LeBron's BDL ID
        if game_logs:
            for log in game_logs[:3]:
                pts = log.get("pts", 0)
                reb = log.get("reb", 0)
                ast = log.get("ast", 0)
                console.print(f"   PTS={pts} REB={reb} AST={ast}")
        else:
            console.print(
                "   [yellow]No game logs returned (may need API key).[/yellow]"
            )

        # Test 4: Full poll cycle
        console.print("\n[cyan]4. Full poll cycle:[/cyan]")
        await poller._poll_cycle()
        console.print(f"   Queue size after poll: {queue.qsize()}")
        console.print(f"   Live games in cache: {len(cache.get_live_games())}")
        console.print(
            f"   Player stats in cache: {len(cache.get_all_player_stats())}"
        )

        # Cleanup
        await poller.stop()
        console.print("\n[green]data/nba_poller.py: Test complete.[/green]")

    asyncio.run(_test())
