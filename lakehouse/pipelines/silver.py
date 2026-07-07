from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

customers_after_schema = StructType([
    StructField("customer_id", LongType()),
    StructField("full_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("city", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

customers_envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("after", customers_after_schema),
        StructField("before", customers_after_schema),
        StructField("op", StringType()),
        StructField("source", StructType([
            StructField("lsn", LongType()),
        ])),
    ]))
])


@dp.view(name="customers_parsed")
def customers_parsed():
    return (
        spark.readStream.table("customers_raw")
        .select(F.from_json(F.col("value").cast("string"), customers_envelope_schema).alias("e"))
        .select(
            F.col("e.payload.op").alias("op"),
            F.col("e.payload.source.lsn").alias("lsn"),
            F.coalesce(F.col("e.payload.after.customer_id"), F.col(
                "e.payload.before.customer_id")).alias("customer_id"),
            F.col("e.payload.after.full_name").alias("full_name"),
            F.col("e.payload.after.email").alias("email"),
            F.col("e.payload.after.phone").alias("phone"),
            F.col("e.payload.after.city").alias("city"),
            F.col("e.payload.after.created_at").alias("created_at"),
            F.col("e.payload.after.updated_at").alias("updated_at"),
        )
    )


dp.create_streaming_table("customers_silver")

dp.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_parsed",
    keys=["customer_id"],
    sequence_by=F.col("lsn"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "lsn"],
    stored_as_scd_type=1,
)
