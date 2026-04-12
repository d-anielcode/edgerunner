"""
Discord webhook alerts for EdgeRunner.

Sends structured notifications to a private Discord channel.
Redesigned for the rules-based agent (no LLM, sport-specific PT,
Bayesian updating, portfolio-aware drawdown).
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
    """Discord webhook alerter with rich embed formatting."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._webhook_url = DISCORD_WEBHOOK_URL

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_message(self, content: str, embeds: list[dict] | None = None) -> bool:
        session = await self._ensure_session()
        payload: dict = {}
        if content:
            payload["content"] = content[:2000]
        if embeds:
            payload["embeds"] = embeds
        try:
            async with session.post(
                self._webhook_url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 204):
                    return True
                if DEBUG_MODE:
                    error = await resp.text()
                    console.print(f"[dim]Discord HTTP {resp.status}: {error[:100]}[/dim]")
                return False
        except asyncio.TimeoutError:
            return False
        except Exception as e:
            console.print(f"[yellow]Discord: {type(e).__name__}: {e}[/yellow]")
            return False

    async def _send_embed(
        self, title: str, description: str, color: int,
        fields: list[dict] | None = None,
    ) -> bool:
        embed: dict = {
            "title": title, "description": description,
            "color": color, "timestamp": datetime.now(UTC).isoformat(),
        }
        if fields:
            embed["fields"] = fields
        return await self._send_message(content="", embeds=[embed])

    # ─── Trade Execution ───

    async def send_trade_alert(
        self, ticker: str, side: str, price: Decimal,
        bet_amount: Decimal, bankroll_pct: float,
        rationale: str, bankroll: Decimal, latency_ms: int = 0,
        portfolio_value: Decimal | None = None,
        sport: str | None = None, profit_take_pct: float | None = None,
    ) -> bool:
        """Trade execution alert — shows sport, PT target, and portfolio value."""
        sport_tag = f"[{sport}] " if sport else ""
        pt_str = f"{profit_take_pct:.0f}% PT" if profit_take_pct else "HOLD"

        return await self._send_embed(
            title=f"{sport_tag}TRADE EXECUTED",
            description=rationale,
            color=0x2ECC71,  # green
            fields=[
                {"name": "Market", "value": ticker, "inline": False},
                {"name": "Side", "value": f"{side.upper()} @ ${price}", "inline": True},
                {"name": "Size", "value": f"${bet_amount:.2f} ({bankroll_pct:.1f}% Kelly)", "inline": True},
                {"name": "Exit Rule", "value": pt_str, "inline": True},
                {"name": "Cash", "value": f"${bankroll:.2f}", "inline": True},
                {"name": "Portfolio", "value": f"${portfolio_value:.2f}" if portfolio_value else "N/A", "inline": True},
                {"name": "Latency", "value": f"{latency_ms}ms", "inline": True},
            ],
        )

    # ─── Position Exit ───

    async def send_exit_alert(
        self, ticker: str, side: str, entry_price: Decimal,
        exit_price: Decimal, pnl: Decimal, pnl_pct: float,
        reason: str, bankroll: Decimal,
    ) -> bool:
        """Position exit alert — profit-take or settlement."""
        win = pnl >= 0
        emoji = "WIN" if win else "LOSS"
        color = 0x2ECC71 if win else 0xE74C3C

        return await self._send_embed(
            title=f"{emoji}: {reason[:50]}",
            description=f"**{ticker}**",
            color=color,
            fields=[
                {"name": "Side", "value": f"SELL {side.upper()} @ ${exit_price}", "inline": True},
                {"name": "Entry", "value": f"${entry_price}", "inline": True},
                {"name": "P&L", "value": f"${pnl:+.2f} ({pnl_pct:+.0f}%)", "inline": True},
                {"name": "Bankroll", "value": f"${bankroll:.2f}", "inline": True},
            ],
        )

    # ─── Daily Recap ───

    async def send_daily_summary(
        self, trades_executed: int, wins: int, losses: int,
        day_pnl: Decimal, bankroll: Decimal,
        portfolio_value: Decimal | None = None,
        open_positions: int = 0,
        signals_generated: int = 0, signals_evaluated: int = 0,
        bayesian_updates: int = 0,
        **kwargs,  # Accept legacy params without breaking
    ) -> bool:
        """Daily recap — portfolio-aware, shows Bayesian learning progress."""
        pnl_sign = "+" if day_pnl >= 0 else ""
        color = 0x2ECC71 if day_pnl >= 0 else 0xE74C3C
        signal_rate = f"{signals_generated}/{signals_evaluated}" if signals_evaluated else "0/0"

        fields = [
            {"name": "Trades", "value": str(trades_executed), "inline": True},
            {"name": "W / L", "value": f"{wins} / {losses}", "inline": True},
            {"name": "Day P&L", "value": f"{pnl_sign}${day_pnl:.2f}", "inline": True},
            {"name": "Cash", "value": f"${bankroll:.2f}", "inline": True},
            {"name": "Portfolio", "value": f"${portfolio_value:.2f}" if portfolio_value else "N/A", "inline": True},
            {"name": "Positions", "value": str(open_positions), "inline": True},
            {"name": "Signals", "value": signal_rate, "inline": True},
            {"name": "Bayesian", "value": f"{bayesian_updates} updates", "inline": True},
        ]

        return await self._send_embed(
            title="DAILY RECAP",
            description=f"Day P&L: {pnl_sign}${day_pnl:.2f}",
            color=color,
            fields=fields,
        )

    # ─── Startup / Shutdown ───

    async def send_startup(self, trading_mode: str, bankroll: Decimal,
                           portfolio_value: Decimal | None = None,
                           active_markets: int = 0) -> bool:
        """Startup alert with portfolio value and market count."""
        desc = f"Mode: **{trading_mode.upper()}** | Cash: **${bankroll:.2f}**"
        if portfolio_value:
            desc += f" | Portfolio: **${portfolio_value:.2f}**"

        fields = [
            {"name": "Time", "value": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "inline": True},
        ]
        if active_markets:
            fields.append({"name": "Markets", "value": str(active_markets), "inline": True})

        return await self._send_embed(
            title="EdgeRunner Started",
            description=desc,
            color=0x3498DB,  # blue
            fields=fields,
        )

    async def send_shutdown(self, reason: str = "Manual stop",
                            pnl: Decimal | None = None,
                            bankroll: Decimal | None = None,
                            trades: int = 0) -> bool:
        """Shutdown alert with session summary."""
        desc = f"Reason: {reason}"
        fields = [
            {"name": "Time", "value": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"), "inline": True},
        ]
        if pnl is not None:
            fields.append({"name": "Session P&L", "value": f"${pnl:+.2f}", "inline": True})
        if bankroll is not None:
            fields.append({"name": "Bankroll", "value": f"${bankroll:.2f}", "inline": True})
        if trades:
            fields.append({"name": "Trades", "value": str(trades), "inline": True})

        return await self._send_embed(
            title="EdgeRunner Stopped",
            description=desc,
            color=0x95A5A6,  # gray
            fields=fields,
        )

    # ─── Error / Warning ───

    async def send_error_alert(self, component: str, detail: str, status: str) -> bool:
        return await self._send_embed(
            title=f"ERROR: {component}",
            description=detail,
            color=0xFF0000,
            fields=[{"name": "Status", "value": status, "inline": False}],
        )

    async def send_debrief(self, debrief_text: str) -> bool:
        return await self._send_embed(
            title="SESSION DEBRIEF",
            description=debrief_text[:4000],
            color=0x9B59B6,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
