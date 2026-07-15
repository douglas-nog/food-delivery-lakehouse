from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Gold layer: business aggregates as materialized views.
#
# Source silver tables are SCD Type 2 (except customers), so each read is
# filtered to the current version with `__END_AT IS NULL`.
#
# All MVs are written to the `gold` schema (fully-qualified names, since the
# pipeline default schema is `silver`), use liquid clustering, and enable
# Change Data Feed to support downstream reverse ETL into Lakebase.
# ---------------------------------------------------------------------------

CDF = {"delta.enableChangeDataFeed": "true"}


def _current(table: str):
    """Current version of an SCD Type 2 silver table."""
    return spark.read.table(f"silver.{table}").filter("__END_AT IS NULL")


# --- MV 1: daily orders and revenue by restaurant -------------------------
@dp.materialized_view(
    name="gold.daily_orders_by_restaurant",
    comment="Daily order count and revenue per restaurant",
    table_properties=CDF,
    cluster_by=["order_date", "restaurant_id"],
)
def daily_orders_by_restaurant():
    orders = _current("orders")
    restaurants = _current("restaurants")
    return (
        orders.alias("o")
        .join(
            F.broadcast(restaurants.alias("r")),
            F.col("o.restaurant_id") == F.col("r.restaurant_id"),
            "inner",
        )
        .withColumn("order_date", F.to_date("o.created_at"))
        .groupBy("order_date", "o.restaurant_id", "r.name")
        .agg(
            F.count("o.order_id").alias("order_count"),
            F.sum("o.total_amount").alias("revenue"),
        )
        .withColumnRenamed("name", "restaurant_name")
    )


# --- MV 2: revenue by restaurant city -------------------------------------
@dp.materialized_view(
    name="gold.revenue_by_city",
    comment="Total revenue and order count per restaurant city",
    table_properties=CDF,
    cluster_by=["city"],
)
def revenue_by_city():
    orders = _current("orders")
    restaurants = _current("restaurants")
    return (
        orders.alias("o")
        .join(
            F.broadcast(restaurants.alias("r")),
            F.col("o.restaurant_id") == F.col("r.restaurant_id"),
            "inner",
        )
        .groupBy("r.city")
        .agg(
            F.count("o.order_id").alias("order_count"),
            F.sum("o.total_amount").alias("revenue"),
        )
    )


# --- MV 3: top products by quantity sold ----------------------------------
@dp.materialized_view(
    name="gold.top_products",
    comment="Products ranked by total quantity sold, with restaurant context",
    table_properties=CDF,
    cluster_by=["restaurant_id"],
)
def top_products():
    order_items = _current("order_items")
    products = _current("products")
    restaurants = _current("restaurants")
    return (
        order_items.alias("oi")
        .join(
            F.broadcast(products.alias("p")),
            F.col("oi.product_id") == F.col("p.product_id"),
            "inner",
        )
        .join(
            F.broadcast(restaurants.alias("r")),
            F.col("p.restaurant_id") == F.col("r.restaurant_id"),
            "inner",
        )
        .groupBy(
            F.col("p.product_id"),
            F.col("p.name").alias("product_name"),
            F.col("p.restaurant_id"),
            F.col("r.name").alias("restaurant_name"),
        )
        .agg(
            F.sum("oi.quantity").alias("total_quantity"),
            F.sum(F.col("oi.quantity") * F.col("oi.unit_price")
                  ).alias("total_revenue"),
        )
    )

# ---------------------------------------------------------------------------
# order_enriched: denormalized serving view for operational apps.
#
# Built with AUTO CDC, not as a plain streaming read of silver.orders. The
# silver tables are produced by AUTO CDC themselves, so they receive MERGEs and
# are NOT append-only, which a streaming source requires. The change feed
# (orders_valid) IS append-only, so it drives this table instead.
#
# The result is a streaming table (standard CDF, so Lakebase can sync it
# incrementally in TRIGGERED mode) holding one current row per order, joined
# with customer and restaurant context. An operational app (order tracking,
# support desk) looks it up by order_id with no runtime joins.
#
# SCD Type 1: serving wants the current state of an order, not its history
# (the history lives in silver.orders).
# ---------------------------------------------------------------------------


@dp.view(name="order_enriched_feed")
def order_enriched_feed():
    # Append-only change feed of valid orders (defined in silver.py).
    orders = spark.readStream.table("orders_valid")
    # Dimensions are read as static snapshots (stream-static join).
    customers = spark.read.table("silver.customers")
    restaurants = _current("restaurants")

    return (
        orders.alias("o")
        .join(
            F.broadcast(customers.alias("c")),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            F.broadcast(restaurants.alias("r")),
            F.col("o.restaurant_id") == F.col("r.restaurant_id"),
            "left",
        )
        .select(
            F.col("o.op"),
            F.col("o.lsn"),
            F.col("o.order_id"),
            F.col("o.status"),
            F.col("o.total_amount"),
            F.col("o.created_at").alias("ordered_at"),
            F.col("o.customer_id"),
            F.col("c.full_name").alias("customer_name"),
            F.col("c.phone").alias("customer_phone"),
            F.col("c.city").alias("customer_city"),
            F.col("o.restaurant_id"),
            F.col("r.name").alias("restaurant_name"),
            F.col("r.city").alias("restaurant_city"),
        )
    )


dp.create_streaming_table(
    name="gold.order_enriched",
    comment="Denormalized current-state order view for low-latency serving",
    table_properties=CDF,
    cluster_by=["order_id"],
)

dp.create_auto_cdc_flow(
    target="gold.order_enriched",
    source="order_enriched_feed",
    keys=["order_id"],
    sequence_by=F.col("lsn"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "lsn"],
    stored_as_scd_type=1,
)
