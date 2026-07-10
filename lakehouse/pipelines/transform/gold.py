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
    return spark.read.table(f"food_delivery.silver.{table}").filter("__END_AT IS NULL")


# --- MV 1: daily orders and revenue by restaurant -------------------------
@dp.materialized_view(
    name="food_delivery.gold.daily_orders_by_restaurant",
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
    name="food_delivery.gold.revenue_by_city",
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
    name="food_delivery.gold.top_products",
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
# Unlike the aggregate MVs above, this is a STREAMING TABLE (not a
# materialized view). A streaming table supports standard CDF, which lets the
# Lakebase synced table run in TRIGGERED (incremental) mode without the
# Auto-CDF private preview that materialized-view sources require.
#
# Pattern: stream-static join. orders is read as a stream; customers and
# restaurants are read as static snapshots (dimensions). This avoids the
# watermark/state requirements of a stream-stream join.
#
# One row per order_id, joining order + customer + restaurant context, so an
# operational app (order tracking, support desk) can look up everything about
# an order by key in Lakebase with no runtime joins.
# ---------------------------------------------------------------------------


@dp.table(
    name="food_delivery.gold.order_enriched",
    comment="Denormalized order view (order + customer + restaurant) for low-latency serving",
    table_properties=CDF,
    cluster_by=["order_id"],
)
def order_enriched():
    orders = spark.readStream.table("food_delivery.silver.orders")
    # customers is SCD Type 1 (no __END_AT), read directly.
    customers = spark.read.table("food_delivery.silver.customers")
    # restaurants is SCD Type 2, take current version only.
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
