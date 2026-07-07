from pyspark import pipelines as dp

KAFKA_BOOTSTRAP = spark.conf.get("kafka.bootstrap")


@dp.table(
    name="customers_raw",
    comment="Raw CDC events from Debezium for the customers table"
)
def customers_raw():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "delivery.public.customers")
        .option("startingOffsets", "earliest")
        .load()
    )
