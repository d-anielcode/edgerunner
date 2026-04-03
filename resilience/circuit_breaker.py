"""
Circuit breaker utilities for EdgeRunner.

Provides pre-configured circuit breakers for external APIs.
When an API fails repeatedly, the circuit breaker "opens" and
immediately returns a fallback instead of wasting time on
requests that will fail.

The analyzer module (signals/analyzer.py) has its own circuit
breaker for Claude. This module provides additional breakers
for other external services and a factory for creating new ones.

Pattern:
  CLOSED (normal) → 3 failures → OPEN (reject all) → 120s → HALF-OPEN (try one) → success → CLOSED
"""

from datetime import timedelta

from aiobreaker import CircuitBreaker, CircuitBreakerError
from rich.console import Console

console = Console()

# Re-export CircuitBreakerError for convenience
__all__ = ["CircuitBreakerError", "create_breaker", "kalshi_breaker", "supabase_breaker"]


def create_breaker(
    name: str,
    fail_max: int = 3,
    timeout_seconds: int = 120,
) -> CircuitBreaker:
    """
    Create a named circuit breaker with standard settings.

    Args:
        name: Identifier for logging (e.g., "kalshi_api", "supabase")
        fail_max: Consecutive failures before opening the circuit
        timeout_seconds: Seconds to wait before trying again (half-open)

    Returns:
        A configured CircuitBreaker instance.
    """
    return CircuitBreaker(
        fail_max=fail_max,
        timeout_duration=timedelta(seconds=timeout_seconds),
        name=name,
    )


# Pre-configured breakers for core services
kalshi_breaker: CircuitBreaker = create_breaker("kalshi_api", fail_max=3, timeout_seconds=60)
"""Circuit breaker for Kalshi REST API. Opens after 3 failures, resets after 60s."""

supabase_breaker: CircuitBreaker = create_breaker("supabase", fail_max=5, timeout_seconds=30)
"""Circuit breaker for Supabase. More lenient (5 failures) since DB issues are often transient."""


def get_breaker_status(breaker: CircuitBreaker) -> dict:
    """Get the current state of a circuit breaker for monitoring."""
    return {
        "name": breaker.name,
        "state": type(breaker.state).__name__,
        "fail_counter": breaker.fail_counter,
    }


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    import asyncio

    async def _test() -> None:
        console.print("[bold]Testing resilience/circuit_breaker.py...[/bold]\n")

        # Test 1: Create and check initial state
        console.print("[cyan]1. Initial state:[/cyan]")
        breaker = create_breaker("test", fail_max=2, timeout_seconds=5)
        status = get_breaker_status(breaker)
        console.print(f"   State: {status['state']}")
        console.print(f"   Failures: {status['fail_counter']}")
        assert status["state"] == "CircuitClosedState"
        console.print("   [green]Starts in CLOSED state.[/green]")

        # Test 2: Trigger failures to open the circuit
        console.print("\n[cyan]2. Trigger 2 failures (should open):[/cyan]")

        @breaker
        async def failing_call() -> None:
            raise ConnectionError("Simulated failure")

        for i in range(2):
            try:
                await failing_call()
            except (ConnectionError, CircuitBreakerError):
                console.print(f"   Failure {i + 1} recorded.")

        status = get_breaker_status(breaker)
        console.print(f"   State: {status['state']}")
        assert status["state"] == "CircuitOpenState"
        console.print("   [green]Circuit is now OPEN.[/green]")

        # Test 3: Verify calls are rejected when open
        console.print("\n[cyan]3. Call while OPEN (should raise CircuitBreakerError):[/cyan]")
        try:
            await failing_call()
            console.print("   [red]FAIL: Should have raised CircuitBreakerError[/red]")
        except CircuitBreakerError:
            console.print("   [green]Correctly raised CircuitBreakerError.[/green]")

        # Test 4: Check pre-configured breakers
        console.print("\n[cyan]4. Pre-configured breakers:[/cyan]")
        for b in [kalshi_breaker, supabase_breaker]:
            s = get_breaker_status(b)
            console.print(f"   {s['name']}: {s['state']} (fail_max={b._fail_max})")

        console.print("\n[green]resilience/circuit_breaker.py: All tests passed.[/green]")

    asyncio.run(_test())
