"""
rate_limiter.py — Rate limiting DISABLED for load testing.
TokenBucket.try_consume always returns True (no throttling).
"""


class TokenBucket:
    """No-op token bucket — all requests are allowed through."""

    def __init__(self, capacity: float, refill_per_sec: float):
        pass  # no state needed

    def try_consume(self, cost: float = 1) -> bool:
        return True  # always allow