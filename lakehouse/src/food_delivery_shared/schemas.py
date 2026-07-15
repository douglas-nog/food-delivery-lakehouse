from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType,
)

# Debezium is configured with decimal.handling.mode=string, so NUMERIC columns
# arrive as exact decimal strings (e.g. "135.00") rather than base64-encoded
# bytes. They are declared as StringType here and cast to DecimalType in the
# silver layer, which preserves exact precision (never use float for money).
DECIMAL_COLS = {"price", "total_amount", "unit_price"}
DECIMAL_PRECISION = "decimal(10,2)"


def envelope_schema(after_schema: StructType) -> StructType:
    """Debezium envelope wrapping a given table's row schema.

    Only the fields consumed downstream are declared: payload.after,
    payload.before, payload.op, and payload.source.lsn (for ordering).
    """
    return StructType([
        StructField("payload", StructType([
            StructField("after", after_schema),
            StructField("before", after_schema),
            StructField("op", StringType()),
            StructField("source", StructType([
                StructField("lsn", LongType()),
            ])),
        ]))
    ])


CUSTOMERS = StructType([
    StructField("customer_id", LongType()),
    StructField("full_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("city", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

RESTAURANTS = StructType([
    StructField("restaurant_id", LongType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("city", StringType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

PRODUCTS = StructType([
    StructField("product_id", LongType()),
    StructField("restaurant_id", LongType()),
    StructField("name", StringType()),
    StructField("price", StringType()),
    StructField("is_available", BooleanType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

ORDERS = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("restaurant_id", LongType()),
    StructField("status", StringType()),
    StructField("total_amount", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

ORDER_ITEMS = StructType([
    StructField("order_item_id", LongType()),
    StructField("order_id", LongType()),
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", StringType()),
    StructField("created_at", StringType()),
])
