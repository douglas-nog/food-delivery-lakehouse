from pyspark import pipelines as dp

KAFKA_BOOTSTRAP = spark.conf.get("kafka.bootstrap")

TABLES = ["customers", "restaurants", "products", "orders", "order_items"]


def make_bronze_table(table_name: str):
    @dp.table(
        name=f"{table_name}_raw",
        comment=f"Raw CDC events from Debezium for the {table_name} table"
    )
    def _table():
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", f"delivery.public.{table_name}")
            .option("startingOffsets", "earliest")
            .load()
        )
    return _table


for table in TABLES:
    make_bronze_table(table)
