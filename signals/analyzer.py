"""
Claude API integration for EdgeRunner — the brain of the agent.

This module sends market data to Claude Haiku via the Anthropic API
and receives structured TradeDecision responses. It's the core intelligence
layer that decides whether a market opportunity has a real mathematical edge.

Architecture:
- Uses anthropic.AsyncAnthropic (async client, non-blocking)
- System prompt is cached for 5 minutes via cache_control (90% cost savings)
- Strict tool use guarantees schema-compliant JSON on every response
- Circuit breaker via aiobreaker (3 failures → open for 120s → PASS fallback)
- Tracks cumulative API cost against monthly budget limit

Cost model (Haiku 4.5 with caching):
- Cache write: $1.25/MTok (first call in 5-min window)
- Cache read: $0.10/MTok (subsequent calls — 90% savings)
- Output: $5.00/MTok
- ~500 calls/day at ~$45/month target
"""

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import anthropic
from aiobreaker import CircuitBreaker, CircuitBreakerError
from rich.console import Console

from config.settings import (
    ANTHROPIC_API_KEY,
    CLAUDE_MONTHLY_BUDGET_LIMIT,
    DEBUG_MODE,
)
from data.cache import (
    AgentCache,
    NbaStatsUpdate,
    OrderbookEntry,
    SmartMoneySignal,
)
from signals.prompts import build_market_context, get_system_prompt
from signals.schemas import TRADE_TOOL_SCHEMA, TradeDecision, pass_decision

console = Console()
UTC = timezone.utc

# Model ID — pinned to Haiku 4.5 for cost efficiency
MODEL_ID: str = "claude-haiku-4-5"

# Circuit breaker: opens after 3 consecutive failures, resets after 120 seconds.
# When open, all calls immediately return PASS instead of hitting the API.
_claude_breaker = CircuitBreaker(
    fail_max=3,
    timeout_duration=timedelta(seconds=120),
    name="claude_api",
)


class MarketAnalyzer:
    """
    Evaluates prediction markets using Claude Haiku as the reasoning engine.

    Sends market context (orderbook, player stats, smart money) to Claude
    and receives a structured TradeDecision. Handles caching, circuit breaking,
    budget tracking, and error recovery.

    Usage:
        analyzer = MarketAnalyzer()
        decision = await analyzer.analyze_market(
            ticker="KXNBA-LEBRON-PTS-O25",
            title="LeBron James Over 25.5 Points",
            cache=cache,
        )
    """

    def __init__(self) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        self._system_prompt = get_system_prompt()
        self._tools = [TRADE_TOOL_SCHEMA]
        self._cumulative_cost: float = 0.0
        self._total_calls: int = 0
        self._cache_hits: int = 0

    # --- Cost Tracking ---

    def _track_cost(self, usage: anthropic.types.Usage) -> float:
        """
        Estimate the cost of a single API call from usage data.

        Haiku 4.5 pricing:
        - Input (base): $1.00/MTok
        - Cache write: $1.25/MTok
        - Cache read: $0.10/MTok
        - Output: $5.00/MTok

        The usage object contains:
        - input_tokens: total input tokens
        - output_tokens: total output tokens
        - cache_creation_input_tokens: tokens written to cache (first call)
        - cache_read_input_tokens: tokens read from cache (subsequent calls)
        """
        cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        base_input = usage.input_tokens - cache_write - cache_read
        output = usage.output_tokens

        cost = (
            (base_input / 1_000_000) * 1.00
            + (cache_write / 1_000_000) * 1.25
            + (cache_read / 1_000_000) * 0.10
            + (output / 1_000_000) * 5.00
        )

        self._cumulative_cost += cost
        self._total_calls += 1
        if cache_read > 0:
            self._cache_hits += 1

        if DEBUG_MODE:
            console.print(
                f"[dim]Claude cost: ${cost:.4f} | "
                f"Cumulative: ${self._cumulative_cost:.2f} | "
                f"Cache hit: {'YES' if cache_read > 0 else 'NO'} | "
                f"Cache rate: {self._cache_hits}/{self._total_calls}[/dim]"
            )

        return cost

    @property
    def budget_remaining(self) -> float:
        """How much of the monthly Claude API budget remains."""
        return CLAUDE_MONTHLY_BUDGET_LIMIT - self._cumulative_cost

    @property
    def is_over_budget(self) -> bool:
        """Whether cumulative spending has exceeded the monthly limit."""
        return self._cumulative_cost >= CLAUDE_MONTHLY_BUDGET_LIMIT

    # --- Core Analysis ---

    async def analyze_market(
        self,
        ticker: str,
        title: str,
        cache: AgentCache,
        orderbook: OrderbookEntry | None = None,
        player_stats: NbaStatsUpdate | None = None,
        smart_money: SmartMoneySignal | None = None,
        game_data: dict | None = None,
        time_to_close_min: float | None = None,
    ) -> TradeDecision:
        """
        Evaluate a single market using Claude Haiku.

        This is the core method. It:
        1. Checks budget and circuit breaker
        2. Builds the market context from live data
        3. Sends to Claude with cached system prompt + strict tool use
        4. Parses the structured response into a TradeDecision
        5. Tracks cost and cache hit rate

        Returns a TradeDecision. NEVER raises — returns PASS on any failure.
        """
        # Budget check
        if self.is_over_budget:
            return pass_decision(
                ticker,
                f"Monthly Claude API budget exceeded (${self._cumulative_cost:.2f}/{CLAUDE_MONTHLY_BUDGET_LIMIT}).",
            )

        # Build the user message from live data
        user_message = build_market_context(
            ticker=ticker,
            title=title,
            orderbook=orderbook,
            player_stats=player_stats,
            smart_money=smart_money,
            game_data=game_data,
            time_to_close_min=time_to_close_min,
            current_positions=cache.get_position_count(),
            bankroll=cache.get_bankroll(),
        )

        console.print(f"[yellow]Analyzing: {title}...[/yellow]")

        try:
            decision = await self._call_claude(ticker, user_message)

            if decision.is_actionable:
                console.print(
                    f"[green]EDGE FOUND: {decision.action} on {ticker} | "
                    f"Edge: {decision.edge:.1%} | Kelly: {decision.kelly_fraction:.3f} | "
                    f"Confidence: {decision.confidence_score:.2f}[/green]"
                )
            else:
                if DEBUG_MODE:
                    console.print(
                        f"[dim]PASS: {ticker} — {decision.rationale}[/dim]"
                    )

            return decision

        except CircuitBreakerError:
            console.print(
                "[red]Claude API circuit breaker OPEN — returning PASS.[/red]"
            )
            return pass_decision(ticker, "Claude API circuit breaker is open. Standing aside.")

        except Exception as e:
            console.print(
                f"[red]Claude analysis error: {type(e).__name__}: {e}[/red]"
            )
            return pass_decision(ticker, f"Analysis failed: {type(e).__name__}")

    @_claude_breaker
    async def _call_claude(self, ticker: str, user_message: str) -> TradeDecision:
        """
        Make the actual Claude API call. Protected by circuit breaker.

        Uses:
        - Cached system prompt (cache_control: ephemeral, 5-min TTL)
        - Strict tool use (guaranteed schema compliance)
        - tool_choice: forces Claude to use execute_prediction_trade
        """
        start_time = time.monotonic()

        response = await self._client.messages.create(
            model=MODEL_ID,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": self._system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=self._tools,
            tool_choice={"type": "tool", "name": "execute_prediction_trade"},
            messages=[{"role": "user", "content": user_message}],
        )

        latency_ms = int((time.monotonic() - start_time) * 1000)
        self._track_cost(response.usage)

        if DEBUG_MODE:
            console.print(f"[dim]Claude latency: {latency_ms}ms[/dim]")

        # Extract the tool call from the response
        for block in response.content:
            if block.type == "tool_use" and block.name == "execute_prediction_trade":
                # Parse Claude's output through Pydantic validation
                decision = TradeDecision(**block.input)
                return decision

        # If no tool call found (shouldn't happen with tool_choice forced)
        console.print("[red]Claude returned no tool call — unexpected.[/red]")
        return pass_decision(ticker, "Claude returned no tool call.")

    # --- Post-Session Debrief ---

    async def post_session_debrief(self, trades: list[dict]) -> str:
        """
        Generate an end-of-day analysis of all trades.

        One Claude call per day analyzing patterns, Kelly accuracy,
        and strategy adjustments. Costs ~$0.002 per debrief.
        """
        if not trades:
            return "No trades to debrief."

        if self.is_over_budget:
            return "Debrief skipped — monthly budget exceeded."

        trades_summary = json.dumps(trades[:50], indent=2, default=str)  # Cap at 50 trades

        try:
            response = await self._client.messages.create(
                model=MODEL_ID,
                max_tokens=2048,
                system="You are a quantitative trading analyst reviewing the day's automated trades on Kalshi prediction markets. Be concise, data-driven, and actionable.",
                messages=[
                    {
                        "role": "user",
                        "content": f"""Analyze today's trading session. Here are the trades:

{trades_summary}

Evaluate:
1. Were the edges real? (Compare entry prices to closing prices if available)
2. Was the Kelly sizing appropriate or too aggressive/conservative?
3. What patterns do you see in winning vs losing trades?
4. Any strategy adjustments recommended for tomorrow?
5. Overall session grade (A-F) with one-line justification.

Be concise — this goes to a Telegram alert.""",
                    }
                ],
            )

            self._track_cost(response.usage)

            # Extract text response
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text

            return "Debrief generation failed — no text in response."

        except Exception as e:
            console.print(f"[red]Debrief error: {type(e).__name__}: {e}[/red]")
            return f"Debrief failed: {type(e).__name__}"

    # --- Status ---

    def get_status(self) -> dict:
        """Return analyzer status for monitoring."""
        cache_rate = (self._cache_hits / self._total_calls * 100) if self._total_calls > 0 else 0.0
        return {
            "total_calls": self._total_calls,
            "cache_hits": self._cache_hits,
            "cache_rate_pct": round(cache_rate, 1),
            "cumulative_cost": round(self._cumulative_cost, 4),
            "budget_remaining": round(self.budget_remaining, 2),
            "is_over_budget": self.is_over_budget,
            "circuit_breaker_state": type(_claude_breaker.state).__name__,
        }


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """
        Test the Claude market analyzer.

        Requires a real ANTHROPIC_API_KEY in .env to make actual API calls.
        With placeholder credentials, the test verifies initialization
        and graceful error handling.
        """
        from data.cache import get_cache

        console.print("[bold]Testing signals/analyzer.py...[/bold]\n")

        cache = get_cache()
        cache.set_bankroll(Decimal("100.00"))

        analyzer = MarketAnalyzer()

        # Test 1: Initialization
        console.print("[cyan]1. Initialization:[/cyan]")
        console.print(f"   Model: {MODEL_ID}")
        console.print(f"   System prompt length: {len(analyzer._system_prompt):,} chars")
        console.print(f"   Budget limit: ${CLAUDE_MONTHLY_BUDGET_LIMIT}")
        status = analyzer.get_status()
        console.print(f"   Circuit breaker: {status['circuit_breaker_state']}")
        console.print("   [green]Initialization OK.[/green]")

        # Test 2: Analyze a mock market
        console.print("\n[cyan]2. Mock market analysis (requires real API key):[/cyan]")

        # Create mock orderbook
        from data.cache import OrderbookEntry, NbaStatsUpdate, SmartMoneySignal

        mock_ob = OrderbookEntry("KXNBA-LEBRON-PTS-O25")
        mock_ob.best_bid = Decimal("0.42")
        mock_ob.best_ask = Decimal("0.45")
        mock_ob.bid_volume = Decimal("150")
        mock_ob.ask_volume = Decimal("50")
        mock_ob.ofi = 0.5

        mock_stats = NbaStatsUpdate(
            timestamp=datetime.now(UTC),
            player_name="LeBron James",
            player_id=2544,
            season_avg_pts=27.1,
            season_avg_reb=7.3,
            season_avg_ast=8.0,
            recent_game_pts=[30.0, 25.0, 32.0, 28.0, 22.0],
            status="Active",
        )

        mock_smart = SmartMoneySignal(
            timestamp=datetime.now(UTC),
            market_title="LeBron Over 25.5 Points",
            consensus_side="yes",
            trader_count=4,
            total_size_usd=35000.0,
            avg_entry_price=0.44,
            top_trader_names=["beachboy4", "sovereign2013", "RN1"],
        )

        try:
            decision = await analyzer.analyze_market(
                ticker="KXNBA-LEBRON-PTS-O25",
                title="LeBron James Over 25.5 Points",
                cache=cache,
                orderbook=mock_ob,
                player_stats=mock_stats,
                smart_money=mock_smart,
                game_data={
                    "Matchup": "LAL vs BOS",
                    "Opponent Def Rating": "108.5 (12th)",
                },
                time_to_close_min=45.0,
            )

            console.print(f"   Action: {decision.action}")
            console.print(f"   Market Prob: {decision.implied_market_probability:.2f}")
            console.print(f"   Agent Prob: {decision.agent_calculated_probability:.2f}")
            console.print(f"   Edge: {decision.edge:.1%}")
            console.print(f"   Kelly: {decision.kelly_fraction:.4f}")
            console.print(f"   Confidence: {decision.confidence_score:.2f}")
            console.print(f"   Rationale: {decision.rationale}")
            console.print("   [green]Analysis complete![/green]")

        except Exception as e:
            console.print(f"   [yellow]API call failed: {type(e).__name__}: {e}[/yellow]")
            console.print(
                "   [yellow]This is expected with placeholder API key. "
                "Set a real ANTHROPIC_API_KEY in .env to test.[/yellow]"
            )

        # Test 3: Status check
        console.print("\n[cyan]3. Analyzer status:[/cyan]")
        status = analyzer.get_status()
        for key, value in status.items():
            console.print(f"   {key}: {value}")

        # Test 4: PASS fallback
        console.print("\n[cyan]4. PASS fallback (no orderbook):[/cyan]")
        pass_d = await analyzer.analyze_market(
            ticker="KXNBA-TEST",
            title="Test Market",
            cache=cache,
            orderbook=None,  # No data → should PASS
        )
        console.print(f"   Action: {pass_d.action}")
        console.print(f"   Rationale: {pass_d.rationale}")

        console.print("\n[green]signals/analyzer.py: Test complete.[/green]")

    asyncio.run(_test())
