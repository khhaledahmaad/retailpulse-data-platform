from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders"

DATA_LAKE_PATH = "/opt/retailpulse/data_lake"

BRONZE_PATH = f"{DATA_LAKE_PATH}/bronze/orders"
SILVER_PATH = f"{DATA_LAKE_PATH}/silver/orders"
QUARANTINE_PATH = f"{DATA_LAKE_PATH}/quarantine/orders"

BRONZE_CHECKPOINT = f"{DATA_LAKE_PATH}/checkpoints/bronze_orders"
SILVER_CHECKPOINT = f"{DATA_LAKE_PATH}/checkpoints/silver_orders"
QUARANTINE_CHECKPOINT = f"{DATA_LAKE_PATH}/checkpoints/quarantine_orders"


ORDER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("currency", StringType(), True),
    ]
)


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )


def build_bronze(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
        to_date(col("timestamp")).alias("ingestion_date"),
    )


def parse_orders(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df.withColumn(
            "event",
            from_json(
                col("raw_payload"),
                ORDER_SCHEMA,
            ),
        )
        .select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingested_at",
            "raw_payload",
            "event.*",
        )
        .withColumn(
            "event_timestamp",
            to_timestamp("event_timestamp"),
        )
    )


def add_validation(parsed_df: DataFrame) -> DataFrame:
    return parsed_df.withColumn(
        "validation_error",
        when(
            col("event_id").isNull(),
            "missing_or_invalid_event_id",
        )
        .when(
            col("order_id").isNull(),
            "missing_order_id",
        )
        .when(
            col("product_id").isNull(),
            "missing_product_id",
        )
        .when(
            col("event_timestamp").isNull(),
            "invalid_event_timestamp",
        )
        .when(
            col("quantity").isNull() | (col("quantity") <= 0),
            "invalid_quantity",
        )
        .when(
            col("unit_price").isNull() | (col("unit_price") < 0),
            "invalid_unit_price",
        )
        .when(
            col("currency") != "GBP",
            "unsupported_currency",
        )
        .otherwise(None),
    )


def build_silver(validated_df: DataFrame) -> DataFrame:
    return (
        validated_df.filter(col("validation_error").isNull())
        .withColumn(
            "order_value",
            spark_round(
                col("quantity") * col("unit_price"),
                2,
            ),
        )
        .withColumn(
            "event_date",
            to_date("event_timestamp"),
        )
        .select(
            "event_id",
            "event_type",
            "event_timestamp",
            "event_date",
            "order_id",
            "customer_id",
            "product_id",
            "category",
            "quantity",
            "unit_price",
            "order_value",
            "currency",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingested_at",
        )
    )


def build_quarantine(validated_df: DataFrame) -> DataFrame:
    return validated_df.filter(col("validation_error").isNotNull()).select(
        "raw_payload",
        "validation_error",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "ingested_at",
    )


def main() -> None:
    spark = SparkSession.builder.appName("RetailPulseOrderLakeStream").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(spark)

    bronze_df = build_bronze(kafka_df)
    parsed_df = parse_orders(bronze_df)
    validated_df = add_validation(parsed_df)

    silver_df = build_silver(validated_df)
    quarantine_df = build_quarantine(validated_df)

    bronze_query = (
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option(
            "checkpointLocation",
            BRONZE_CHECKPOINT,
        )
        .start(BRONZE_PATH)
    )

    silver_query = (
        silver_df.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("event_date")
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT,
        )
        .start(SILVER_PATH)
    )

    quarantine_query = (
        quarantine_df.writeStream.format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            QUARANTINE_CHECKPOINT,
        )
        .start(QUARANTINE_PATH)
    )

    print("RetailPulse streaming pipeline started.")
    print(f"Bronze:     {BRONZE_PATH}")
    print(f"Silver:     {SILVER_PATH}")
    print(f"Quarantine: {QUARANTINE_PATH}")

    spark.streams.awaitAnyTermination()

    bronze_query.stop()
    silver_query.stop()
    quarantine_query.stop()


if __name__ == "__main__":
    main()
