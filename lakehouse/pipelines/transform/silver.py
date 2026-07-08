from shared import schemas
from pyspark.sql import functions as F
from pyspark import pipelines as dp
import sys
import os
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..")))


# Entity config: (name, after_schema, key, scd_type)
ENTITIES = [
    ("customers",    schemas.CUSTOMERS,    "customer_id",    1),
    ("restaurants",  schemas.RESTAURANTS,  "restaurant_id",  2),
    ("products",     schemas.PRODUCTS,     "product_id",     2),
    ("orders",       schemas.ORDERS,       "order_id",       2),
    ("order_items",  schemas.ORDER_ITEMS,  "order_item_id",  2),
]


def build_entity(name: str, after_schema, key: str, scd_type: int):
    envelope = schemas.envelope_schema(after_schema)
    data_cols = [f.name for f in after_schema.fields]

    # Parse view: Debezium envelope -> flat change records
    @dp.view(name=f"{name}_parsed")
    def _parsed(name=name, envelope=envelope, key=key, data_cols=data_cols):
        parsed = (
            spark.readStream.table(f"food_delivery.bronze.{name}_raw")
            .select(F.from_json(F.col("value").cast("string"), envelope).alias("e"))
        )
        cols = [
            F.col("e.payload.op").alias("op"),
            F.col("e.payload.source.lsn").alias("lsn"),
            F.coalesce(F.col(f"e.payload.after.{key}"),
                       F.col(f"e.payload.before.{key}")).alias(key),
        ]
        # remaining data columns (except the key, already added)
        cols += [F.col(f"e.payload.after.{c}").alias(c)
                 for c in data_cols if c != key]
        return parsed.select(*cols)

    # Target streaming table
    dp.create_streaming_table(name=name)

    # AUTO CDC flow
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
