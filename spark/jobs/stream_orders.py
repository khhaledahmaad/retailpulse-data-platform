from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "orders"


ORDER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("currency", StringType(), False),
    ]
)


def main() -> None:
    spark = SparkSession.builder.appName("RetailPulseOrderStream").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            from_json(
                col("json_value"),
                ORDER_SCHEMA,
            ),
        )
        .select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event.*",
        )
    )

    query = (
        parsed_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
