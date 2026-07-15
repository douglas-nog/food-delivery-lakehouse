"""Unit tests for the load generator's pure business logic."""

from decimal import Decimal

from food_delivery_shared import load_logic as L


class TestOrderTotal:
    def test_sums_quantity_times_price(self):
        items = [(1, 2, Decimal("10.00")), (2, 1, Decimal("5.50"))]
        assert L.order_total(items) == Decimal("25.50")

    def test_empty_is_zero(self):
        assert L.order_total([]) == Decimal("0")

    def test_preserves_exact_decimal(self):
        items = [(1, 3, Decimal("0.10"))]
        assert L.order_total(items) == Decimal("0.30")


class TestChooseAnomaly:
    def test_no_anomaly_above_rate(self):
        assert L.choose_anomaly(0.5, 0.05) is None

    def test_negative_total_in_lower_half(self):
        assert L.choose_anomaly(0.01, 0.05) == L.ANOMALY_NEGATIVE_TOTAL

    def test_unknown_status_in_upper_half(self):
        assert L.choose_anomaly(0.04, 0.05) == L.ANOMALY_UNKNOWN_STATUS

    def test_boundary_exactly_at_rate_is_none(self):
        assert L.choose_anomaly(0.05, 0.05) is None

    def test_zero_rate_never_anomalous(self):
        assert L.choose_anomaly(0.0, 0.0) is None


class TestApplyAnomaly:
    def test_negative_total_flips_sign(self):
        total, status = L.apply_anomaly(
            Decimal("50.00"), "created", L.ANOMALY_NEGATIVE_TOTAL, "unknown"
        )
        assert total == Decimal("-50.00")
        assert status == "created"

    def test_unknown_status_replaces_status(self):
        total, status = L.apply_anomaly(
            Decimal("50.00"), "created", L.ANOMALY_UNKNOWN_STATUS, "pending_sync"
        )
        assert total == Decimal("50.00")
        assert status == "pending_sync"

    def test_none_leaves_unchanged(self):
        total, status = L.apply_anomaly(Decimal("50.00"), "created", None, "x")
        assert total == Decimal("50.00")
        assert status == "created"


class TestNextStatus:
    def test_normal_progression(self):
        assert L.next_status("created", cancel=False) == "confirmed"
        assert L.next_status("confirmed", cancel=False) == "preparing"
        assert L.next_status("preparing", cancel=False) == "delivered"

    def test_cancel_overrides(self):
        assert L.next_status("created", cancel=True) == "cancelled"


class TestAdjustedPrice:
    def test_rounds_to_two_decimals(self):
        assert L.adjusted_price(Decimal("10.00"), 1.055) == Decimal("10.55")

    def test_exact_decimal(self):
        assert L.adjusted_price(Decimal("100.00"), 0.9) == Decimal("90.00")


class TestCanDeactivate:
    def test_reactivating_always_allowed(self):
        assert L.can_deactivate(
            is_active=False, active_count=2, min_active=2) is True

    def test_blocked_at_minimum(self):
        assert L.can_deactivate(
            is_active=True, active_count=2, min_active=2) is False

    def test_allowed_above_minimum(self):
        assert L.can_deactivate(
            is_active=True, active_count=3, min_active=2) is True
