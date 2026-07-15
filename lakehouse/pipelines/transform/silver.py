from pyspark import pipelines as dp
from pyspark.sql import functions as F

from food_delivery_shared import schemas


# ---------------------------------------------------------------------------
# Entities ingested through the generic factory (4 of 5).
# orders is handled separately below because it uses a quarantine pattern.
# (name, after_schema, key, scd_type, drop_rules, warn_rules)
#   drop_rules  -> @expect_or_drop  (bad record is dropped)
#   warn_rules  -> @expect          (record kept, violation tracked)
# ---------------------------------------------------------------------------
ENTITIES = [
    (
        "customers", schemas.CUSTOMERS, "customer_id", 1,
        {"valid_key": "customer_id IS NOT NULL"},
        {"email_present": "email IS NOT NULL",
            "phone_present": "phone IS NOT NULL"},
    ),
    (
        "restaurants", schemas.RESTAURANTS, "restaurant_id", 2,
        {"valid_key": "restaurant_id IS NOT NULL"},
        {"name_present": "name IS NOT NULL"},
    ),
    (
        "products", schemas.PRODUCTS, "product_id", 2,
        {"valid_key": "product_id IS NOT NULL"},
        {"positive_price": "price > 0", "name_present": "name IS NOT NULL"},
    ),
    (
        "order_items", schemas.ORDER_ITEMS, "order_item_id", 2,
        {"valid_key": "order_item_id IS NOT NULL"},
        {"positive_qty": "quantity > 0"},
    ),
]


def _parse_expr(name, envelope, key, data_cols):
    """Shared parsing logic: Debezium envelope -> flat change records.

    Debezium emits NUMERIC columns as exact decimal strings
    (decimal.handling.mode=string), so they are cast back to DECIMAL here.
    Casting to decimal (not double) preserves exact precision for money.
    """
    parsed = (
        spark.readStream.table(f"bronze.{name}_raw")
        .select(F.from_json(F.col("value").cast("string"), envelope).alias("e"))
    )

    def _col(c):
        expr = F.col(f"e.payload.after.{c}")
        if c in schemas.DECIMAL_COLS:
            expr = expr.cast(schemas.DECIMAL_PRECISION)
        return expr.alias(c)

    cols = [
        F.col("e.payload.op").alias("op"),
        F.col("e.payload.source.lsn").alias("lsn"),
        F.coalesce(F.col(f"e.payload.after.{key}"),
                   F.col(f"e.payload.before.{key}")).alias(key),
    ]
    cols += [_col(c) for c in data_cols if c != key]
    return parsed.select(*cols)


def build_entity(name, after_schema, key, scd_type, drop_rules, warn_rules):
    envelope = schemas.envelope_schema(after_schema)
    data_cols = [f.name for f in after_schema.fields]

    @dp.view(name=f"{name}_parsed")
    @dp.expect_all_or_drop(drop_rules)
    @dp.expect_all(warn_rules)
    def _parsed(name=name, envelope=envelope, key=key, data_cols=data_cols):
        return _parse_expr(name, envelope, key, data_cols)

    dp.create_streaming_table(name=name)

    dp.create_auto_cdc_flow(
        target=name,
        source=f"{name}_parsed",
        keys=[key],
        sequence_by=F.col("lsn"),
        apply_as_deletes=F.expr("op = 'd'"),
        except_column_list=["op", "lsn"],
        stored_as_scd_type=scd_type,
    )


for entity in ENTITIES:
    build_entity(*entity)


# ---------------------------------------------------------------------------
# orders: quarantine pattern.
#   - parse the envelope
#   - flag rows that violate business rules (is_quarantined)
#   - valid rows feed the AUTO CDC target (orders)
#   - invalid rows land in orders_quarantine for inspection
# ---------------------------------------------------------------------------
_ORDERS_KEY = "order_id"
_ORDERS_ENVELOPE = schemas.envelope_schema(schemas.ORDERS)
_ORDERS_COLS = [f.name for f in schemas.ORDERS.fields]

# Business rules; a row is quarantined if it fails ANY of them.
_ORDERS_RULES = {
    "valid_key": "order_id IS NOT NULL",
    "non_negative_amount": "total_amount >= 0",
}
_ORDERS_QUARANTINE_EXPR = "NOT({0})".format(
    " AND ".join(_ORDERS_RULES.values()))


@dp.view(name="orders_parsed")
@dp.expect("known_status",
           "status IN ('created','confirmed','preparing','delivered','cancelled')")
def orders_parsed():
    parsed = _parse_expr("orders", _ORDERS_ENVELOPE, _ORDERS_KEY, _ORDERS_COLS)
    return parsed.withColumn("is_quarantined", F.expr(_ORDERS_QUARANTINE_EXPR))


@dp.view(name="orders_valid")
def orders_valid():
    return spark.readStream.table("orders_parsed").filter("is_quarantined = false")


@dp.table(name="orders_quarantine",
          comment="Order change records that failed business rules")
def orders_quarantine():
    return spark.readStream.table("orders_parsed").filter("is_quarantined = true")


dp.create_streaming_table(name="orders")

dp.create_auto_cdc_flow(
    target="orders",
    source="orders_valid",
    keys=[_ORDERS_KEY],
    sequence_by=F.col("lsn"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "lsn", "is_quarantined"],
    stored_as_scd_type=2,
)
