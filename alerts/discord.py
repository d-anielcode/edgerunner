"""
Discord webhook alert pipeline for EdgeRunner.

Sends structured notifications to a private Discord channel:
- Trade execution alerts (the primary "dashboard")
- Error alerts for API failures and disconnects
- Daily summary with P&L, CLV, latency, and API cost

This is the operator's read-only mobile interface. You monitor
everything from your phone via Discord notifications.

Design:
- Uses raw HTTP POST to Discord webhook URL (no bot framework needed)
- Discord webhooks are simpler than Telegram: one URL, no token/chat ID
- All sends are async and non-blocking
- Alert failure NEVER blocks the trading loop
- Uses Discord embed format for rich formatting
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp
from rich.console import Console

from config.settings import DEBUG_MODE, DISCORD_WEBHOOK_URL

console = Console()
UTC = timezone.utc


class DiscordAlerter:
    """
    Sends formatted notifications to a Discord channel via webhook.

    All methods are async and fire-and-forget — they catch their own
    errors so a Discord failure never crashes the trading loop.

    Usage:
        alerter = DiscordAlerter()
        await alerter.send_trade_alert(trade, bankroll)
        await alerter.send_error_alert("Claude API", "Circuit breaker OPEN", "PASSing all trades")
        await alerter.send_daily_summary(stats)
    """

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._webhook_url = DISCORD_WEBHOOK_URL

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_message(self, content: str, embeds: list[dict] | None = None) -> bool:
        """
        Send a message to the Discord webhook.

        Returns True on success, False on failure.
        Failures are logged but never raised.

        Discord webhook accepts:
        - content: plain text message (up to 2000 chars)
        - embeds: list of rich embed objects (up to 6000 chars total)
        """
        session = await self._ensure_session()
        payload: dict = {}

        if content:
            payload["content"] = content[:2000]
        if embeds:
            payload["embeds"] = embeds

        try:
            async with session.post(
                self._webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                # Discord returns 204 No Content on success
                if resp.status in (200, 204):
                    return True
                error = await resp.text()
                if DEBUG_MODE:
                    console.print(f"[dim]Discord HTTP {resp.status}: {error[:100]}[/dim]")
                return False
        except asyncio.TimeoutError:
            console.print("[yellow]Discord: Send timed out.[/yellow]")
            return False
        except Exception as e:
            console.print(f"[yellow]Discord send error: {type(e).__name__}: {e}[/yellow]")
            return False

    async def _send_embed(
        self,
        title: str,
        description: str,
        color: int,
        fields: list[dict] | None = None,
    ) -> bool:
        """
        Send a rich embed message.

        Discord embeds support structured fields, colors, and formatting.
        Color values: green=0x00FF00, red=0xFF0000, yellow=0xFFFF00, blue=0x0099FF
        """
        embed: dict = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        if fields:
            embed["fields"] = fields

        return await self._send_message(content="", embeds=[embed])

    # --- Alert Types ---

    async def send_trade_alert(
        self,
        ticker: str,
        side: str,
        price: Decimal,
        bet_amount: Decimal,
        bankroll_pct: float,
        rationale: str,
        bankroll: Decimal,
        latency_ms: int = 0,
    ) -> bool:
        """
        Send a trade execution alert.

        Fired the millisecond a trade executes. Uses a green embed.
        """
        return await self._send_embed(
            title="EXECUTION: Kalshi",
            description=rationale,
            color=0x00FF00,  # green
            fields=[
                {"name": "Market", "value": ticker, "inline": False},
                {"name": "Side", "value": f"{side.upper()} @ ${price}", "inline": True},
                {"name": "Bet Size", "value": f"${bet_amount} ({bankroll_pct:.1f}% Kelly)", "inline": True},
                {"name": "Bankroll", "value": f"${bankroll}", "inline": True},
                {"name": "Latency", "value": f"{latency_ms}ms", "inline": True},
            ],
        )

    async def send_error_alert(
        self,
        component: str,
        detail: str,
        status: str,
    ) -> bool:
        """Send an error alert for API failures, disconnects, etc."""
        return await self._send_embed(
            title=f"ERROR: {component}",
            description=detail,
            color=0xFF0000,  # red
            fields=[
                {"name": "Status", "value": status, "inline": False},
            ],
        )

    async def send_daily_summary(
        self,
        trades_executed: int,
        wins: int,
        losses: int,
        day_pnl: Decimal,
        bankroll: Decimal,
        avg_clv: float = 0.0,
        avg_latency_ms: int = 0,
        api_cost: float = 0.0,
    ) -> bool:
        """Send the end-of-session daily summary."""
        pnl_sign = "+" if day_pnl >= 0 else ""
        return await self._send_embed(
            title="DAILY SUMMARY",
            description=f"Day P&L: {pnl_sign}${day_pnl}",
            color=0x00FF00 if day_pnl >= 0 else 0xFF0000,
            fields=[
                {"name": "Trades", "value": str(trades_executed), "inline": True},
                {"name": "Win/Loss", "value": f"{wins}-{losses}", "inline": True},
                {"name": "Bankroll", "value": f"${bankroll}", "inline": True},
                {"name": "Avg CLV", "value": f"{avg_clv:+.1f} cents", "inline": True},
                {"name": "Avg Latency", "value": f"{avg_latency_ms}ms", "inline": True},
                {"name": "Claude Cost", "value": f"${api_cost:.2f}", "inline": True},
            ],
        )

    async def send_debrief(self, debrief_text: str) -> bool:
        """Send the AI post-session debrief analysis."""
        return await self._send_embed(
            title="SESSION DEBRIEF",
            description=debrief_text[:4000],
            color=0x9B59B6,  # purple
        )

    async def send_startup(self, trading_mode: str, bankroll: Decimal) -> bool:
        """Send a startup notification when the agent begins."""
        return await self._send_embed(
            title="EdgeRunner Started",
            description=f"Mode: **{trading_mode.upper()}** | Bankroll: **${bankroll}**",
            color=0x0099FF,  # blue
            fields=[
                {"name": "Time", "value": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "inline": True},
            ],
        )

    async def send_shutdown(self, reason: str = "Manual stop") -> bool:
        """Send a shutdown notification when the agent stops."""
        return await self._send_embed(
            title="EdgeRunner Stopped",
            description=f"Reason: {reason}",
            color=0x95A5A6,  # gray
            fields=[
                {"name": "Time", "value": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "inline": True},
            ],
        )

    # --- Cleanup ---

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """
        Test the Discord alerter.

        With placeholder webhook URL, messages will fail to send — that's expected.
        The test verifies message formatting and graceful error handling.
        """
        console.print("[bold]Testing alerts/discord.py...[/bold]\n")

        alerter = DiscordAlerter()

        # Test 1: Trade alert
        console.print("[cyan]1. Trade alert (will fail with placeholder URL):[/cyan]")
        result = await alerter.send_trade_alert(
            ticker="KXNBA-LEBRON-PTS-O25",
            side="yes",
            price=Decimal("0.42"),
            bet_amount=Decimal("5.00"),
            bankroll_pct=4.2,
            rationale="Davis OUT — usage rate implies 65% true prob vs 42% market.",
            bankroll=Decimal("95.00"),
            latency_ms=340,
        )
        console.print(f"   Sent: {result}")
        if not result:
            console.print("   [yellow]Expected failure with placeholder webhook URL.[/yellow]")

        # Test 2: Error alert
        console.print("\n[cyan]2. Error alert:[/cyan]")
        result = await alerter.send_error_alert(
            component="Claude API",
            detail="Circuit breaker OPEN — 3 consecutive timeouts",
            status="PASSing all new trades. Retry in 120s.",
        )
        console.print(f"   Sent: {result}")

        # Test 3: Daily summary
        console.print("\n[cyan]3. Daily summary:[/cyan]")
        result = await alerter.send_daily_summary(
            trades_executed=7,
            wins=5,
            losses=2,
            day_pnl=Decimal("8.40"),
            bankroll=Decimal("108.40"),
            avg_clv=3.2,
            avg_latency_ms=340,
            api_cost=1.47,
        )
        console.print(f"   Sent: {result}")

        # Test 4: Startup
        console.print("\n[cyan]4. Startup alert:[/cyan]")
        result = await alerter.send_startup("paper", Decimal("100.00"))
        console.print(f"   Sent: {result}")

        await alerter.close()
        console.print("\n[green]alerts/discord.py: Test complete.[/green]")

    asyncio.run(_test())
