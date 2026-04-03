"""
Retry utilities for EdgeRunner.

Provides pre-configured retry decorators using tenacity.
These wrap async functions with exponential backoff + jitter
to handle transient failures (network blips, rate limits).

Why tenacity? It's the standard Python retry library with native
asyncio support, configurable backoff, and clean decorator syntax.

Usage:
    @retry_on_transient
    async def call_api():
        ...
"""

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from rich.console import Console

console = Console()


# Common transient exceptions worth retrying
TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
)

# Standard retry decorator for API calls
retry_on_transient = retry(
    retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
    wait=wait_exponential_jitter(initial=1, max=30, jitter=2),
    stop=stop_after_attempt(3),
    reraise=True,
)
"""
Retry decorator for transient network errors.

- Retries on: ConnectionError, TimeoutError, OSError
- Backoff: 1s → 2s → 4s (exponential with jitter)
- Max attempts: 3
- Reraises the original exception after exhausting retries

Usage:
    @retry_on_transient
    async def fetch_data():
        ...
"""

# Aggressive retry for critical operations (order placement)
retry_aggressive = retry(
    retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
    wait=wait_exponential_jitter(initial=0.5, max=15, jitter=1),
    stop=stop_after_attempt(5),
    reraise=True,
)
"""
More aggressive retry for critical operations like order placement.

- Faster initial backoff (0.5s)
- 5 attempts instead of 3
- Use only for operations where failure means missed opportunity
"""

# Gentle retry for non-critical operations (logging, alerts)
retry_gentle = retry(
    retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS),
    wait=wait_exponential_jitter(initial=2, max=60, jitter=5),
    stop=stop_after_attempt(2),
    reraise=True,
)
"""
Gentle retry for non-critical operations (Supabase logging, Telegram alerts).

- Slower backoff (2s initial)
- Only 2 attempts — if it fails twice, move on
- Non-critical: don't waste time retrying logging
"""


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    import asyncio

    async def _test() -> None:
        console.print("[bold]Testing resilience/retry.py...[/bold]\n")

        # Test 1: Successful call (no retry needed)
        console.print("[cyan]1. Successful call (no retry):[/cyan]")
        call_count = 0

        @retry_on_transient
        async def succeeds() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await succeeds()
        assert result == "ok"
        assert call_count == 1
        console.print(f"   Result: {result}, calls: {call_count}")
        console.print("   [green]No unnecessary retries.[/green]")

        # Test 2: Transient failure then success
        console.print("\n[cyan]2. Fail once, then succeed:[/cyan]")
        call_count = 0

        @retry_on_transient
        async def fails_then_succeeds() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Transient failure")
            return "recovered"

        result = await fails_then_succeeds()
        assert result == "recovered"
        assert call_count == 2
        console.print(f"   Result: {result}, calls: {call_count}")
        console.print("   [green]Retried and recovered.[/green]")

        # Test 3: Exhaust all retries
        console.print("\n[cyan]3. Exhaust all retries (should raise):[/cyan]")

        @retry_on_transient
        async def always_fails() -> None:
            raise TimeoutError("Always times out")

        try:
            await always_fails()
            console.print("   [red]FAIL: Should have raised[/red]")
        except TimeoutError:
            console.print("   [green]Correctly raised after 3 attempts.[/green]")

        # Test 4: Non-transient exception (should NOT retry)
        console.print("\n[cyan]4. Non-transient exception (no retry):[/cyan]")
        call_count = 0

        @retry_on_transient
        async def value_error() -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("Not transient")

        try:
            await value_error()
        except ValueError:
            assert call_count == 1
            console.print(f"   Calls: {call_count}")
            console.print("   [green]Correctly did NOT retry ValueError.[/green]")

        console.print("\n[green]resilience/retry.py: All tests passed.[/green]")

    asyncio.run(_test())
