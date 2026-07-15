"""Pure business logic for the load generator, isolated from database I/O.

These functions contain the domain rules (order total, anomaly injection,
status lifecycle) with no side effects, so they can be unit-tested without a
Postgres connection. generator.py imports and orchestrates them around the
actual SQL.
"""

from __future__ import annotations

from decimal import Decimal

# Status lifecycle
STATUS_FLOW = {
    "created": "confirmed",
    "confirmed": "preparing",
    "preparing": "delivered",
}
OPEN_STATUSES = ("created", "confirmed", "preparing")
UNKNOWN_STATUSES = ("pending_sync", "unknown", "payment_review")

# Anomaly kinds
ANOMALY_NEGATIVE_TOTAL = "negative_total"
ANOMALY_UNKNOWN_STATUS = "unknown_status"


def order_total(items: list[tuple[int, int, Decimal]]) -> Decimal:
    """Sum quantity * unit_price over items [(product_id, qty, price), ...]."""
    return sum((price * qty for _, qty, price in items), Decimal("0"))


def choose_anomaly(rand: float, anomaly_rate: float) -> str | None:
    """Decide whether this order is anomalous, given a [0,1) random draw.

    Returns the anomaly kind or None. Kept separate from the random source so
    tests can pass deterministic values.
    """
    if rand >= anomaly_rate:
        return None
    half = anomaly_rate / 2
    return ANOMALY_NEGATIVE_TOTAL if rand < half else ANOMALY_UNKNOWN_STATUS


def apply_anomaly(
    total: Decimal, status: str, anomaly: str | None, unknown_status: str
) -> tuple[Decimal, str]:
    """Return (total, status) after applying the anomaly, if any.

    negative_total flips the sign (mimics a pricing/refund bug); unknown_status
    replaces the status with an unmapped value (mimics an upstream integration).
    """
    if anomaly == ANOMALY_NEGATIVE_TOTAL:
        return -total, status
    if anomaly == ANOMALY_UNKNOWN_STATUS:
        return total, unknown_status
    return total, status


def next_status(current: str, cancel: bool) -> str:
    """Next status in the lifecycle, or 'cancelled' if cancel is True."""
    if cancel:
        return "cancelled"
    return STATUS_FLOW[current]


def adjusted_price(price: Decimal, factor: float) -> Decimal:
    """Apply a price factor and round to 2 decimals (exact decimal money)."""
    return (price * Decimal(str(factor))).quantize(Decimal("0.01"))


def can_deactivate(is_active: bool, active_count: int, min_active: int) -> bool:
    """Whether a restaurant may be deactivated without starving order creation."""
    if not is_active:
        return True
    return active_count > min_active
