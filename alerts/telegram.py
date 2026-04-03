"""
Telegram alert pipeline for EdgeRunner.

Sends structured notifications to a private Telegram chat:
- Trade execution alerts (the primary "dashboard")
- Error alerts for API failures and disconnects
- Daily summary with P&L, CLV, latency, and API cost

This is the operator's read-only mobile interface. It eliminates
the need for a UI — you monitor everything from your phone.

Design:
- Uses raw HTTP POST to Telegram Bot API (simpler than python-telegram-bot
  for one-way notifications — no need for command handlers or polling)
- All sends are async and non-blocking
- Alert failure NEVER blocks the trading loop
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp
from rich.console import Console

from config.settings import DEBUG_MODE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

console = Console()
UTC = timezone.utc

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


class TelegramAlerter:
    """
    Sends formatted notifications to a private Telegram chat.

    All methods are async and fire-and-forget — they catch their own
    errors so a Telegram failure never crashes the trading loop.

    Usage:
        alerter = TelegramAlerter()
        await alerter.send_trade_alert(trade, bankroll)
        await alerter.send_error_alert("Claude API", "Circuit breaker OPEN", "PASSing all trades")
        await alerter.send_daily_summary(stats)
    """

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._chat_id = TELEGRAM_CHAT_ID

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Send a message to the configured Telegram chat.

        Returns True on success, False on failure.
        Failures are logged but never raised.
        """
        session = await self._ensure_session()
        url = f"{TELEGRAM_API_URL}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }

        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True
                error = await resp.text()
                if DEBUG_MODE:
                    console.print(f"[dim]Telegram HTTP {resp.status}: {error[:100]}[/dim]")
                return False
        except asyncio.TimeoutError:
            console.print("[yellow]Telegram: Send timed out.[/yellow]")
            return False
        except Exception as e:
            console.print(f"[yellow]Telegram send error: {type(e).__name__}: {e}[/yellow]")
            return False

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

        This is the primary notification — fired the millisecond a trade executes.
        Format matches the PRD template exactly.
        """
        text = (
            f"<b>🟢 EXECUTION: Kalshi</b>\n"
            f"<b>Market:</b> {ticker}\n"
            f"<b>Side:</b> {side.upper()} @ ${price}\n"
            f"<b>Bet Size:</b> ${bet_amount} ({bankroll_pct:.1f}% of Bankroll via Kelly)\n"
            f"<b>Edge:</b> {rationale}\n"
            f"<b>Bankroll:</b> ${bankroll}\n"
            f"<b>Latency:</b> {latency_ms}ms"
        )
        return await self._send_message(text)

    async def send_error_alert(
        self,
        component: str,
        detail: str,
        status: str,
    ) -> bool:
        """
        Send an error alert for API failures, disconnects, etc.
        """
        text = (
            f"<b>🔴 ERROR: {component}</b>\n"
            f"<b>Detail:</b> {detail}\n"
            f"<b>Status:</b> {status}"
        )
        return await self._send_message(text)

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
        """
        Send the end-of-session daily summary.
        """
        text = (
            f"<b>📊 DAILY SUMMARY</b>\n"
            f"<b>Trades:</b> {trades_executed}\n"
            f"<b>Win/Loss:</b> {wins}-{losses}\n"
            f"<b>Day P&L:</b> {'+'if day_pnl >= 0 else ''}{day_pnl}\n"
            f"<b>Bankroll:</b> ${bankroll}\n"
            f"<b>Avg CLV:</b> {avg_clv:+.1f} cents\n"
            f"<b>Avg Latency:</b> {avg_latency_ms}ms\n"
            f"<b>Claude API Cost:</b> ${api_cost:.2f}"
        )
        return await self._send_message(text)

    async def send_debrief(self, debrief_text: str) -> bool:
        """Send the AI post-session debrief analysis."""
        text = f"<b>🧠 SESSION DEBRIEF</b>\n\n{debrief_text[:3500]}"
        return await self._send_message(text)

    async def send_startup(self, trading_mode: str, bankroll: Decimal) -> bool:
        """Send a startup notification when the agent begins."""
        text = (
            f"<b>🚀 EdgeRunner Started</b>\n"
            f"<b>Mode:</b> {trading_mode.upper()}\n"
            f"<b>Bankroll:</b> ${bankroll}\n"
            f"<b>Time:</b> {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return await self._send_message(text)

    async def send_shutdown(self, reason: str = "Manual stop") -> bool:
        """Send a shutdown notification when the agent stops."""
        text = (
            f"<b>🛑 EdgeRunner Stopped</b>\n"
            f"<b>Reason:</b> {reason}\n"
            f"<b>Time:</b> {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return await self._send_message(text)

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
        Test the Telegram alerter.

        With placeholder bot token, messages will fail to send — that's expected.
        The test verifies message formatting and graceful error handling.
        """
        console.print("[bold]Testing alerts/telegram.py...[/bold]\n")

        alerter = TelegramAlerter()

        # Test 1: Trade alert
        console.print("[cyan]1. Trade alert (will fail with placeholder token):[/cyan]")
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
            console.print("   [yellow]Expected failure with placeholder token.[/yellow]")

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
        console.print("\n[green]alerts/telegram.py: Test complete.[/green]")

    asyncio.run(_test())
