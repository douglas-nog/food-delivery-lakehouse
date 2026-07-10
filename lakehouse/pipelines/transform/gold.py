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
