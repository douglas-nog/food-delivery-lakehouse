"""Unit tests for the Debezium schema contracts (no Spark session needed)."""

from pyspark.sql.types import StringType, LongType

from food_delivery_shared import schemas


class TestOrdersSchema:
    def test_has_expected_fields(self):
        names = {f.name for f in schemas.ORDERS.fields}
        assert names == {
            "order_id", "customer_id", "restaurant_id", "status",
            "total_amount", "created_at", "updated_at",
        }

    def test_total_amount_is_string_for_debezium(self):
        field = next(f for f in schemas.ORDERS.fields if f.name ==
                     "total_amount")
        assert isinstance(field.dataType, StringType)

    def test_ids_are_long(self):
        field = next(f for f in schemas.ORDERS.fields if f.name == "order_id")
        assert isinstance(field.dataType, LongType)


class TestEnvelopeSchema:
    def test_wraps_after_before_op_lsn(self):
        env = schemas.envelope_schema(schemas.ORDERS)
        payload = next(f for f in env.fields if f.name == "payload").dataType
        names = {f.name for f in payload.fields}
        assert names == {"after", "before", "op", "source"}

    def test_source_has_lsn(self):
        env = schemas.envelope_schema(schemas.ORDERS)
        payload = next(f for f in env.fields if f.name == "payload").dataType
        source = next(f for f in payload.fields if f.name == "source").dataType
        assert {f.name for f in source.fields} == {"lsn"}

    def test_decimal_precision_constant(self):
        assert schemas.DECIMAL_PRECISION == "decimal(10,2)"
        assert "total_amount" in schemas.DECIMAL_COLS
