import argparse
import os
import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

def build_spark_session():
    spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("SentinelFlow")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
        )
        .getOrCreate()
    )
    return spark

def consumer_kafka_to_sparks(bootstrap, topic):
    spark = build_spark_session()
    kafka_df = (
        spark.readStream
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema_sensor_event = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_source", StringType()),
    StructField("site_id", StringType()),
    StructField("asset_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("metric_name", StringType()),
    StructField("metric_value", FloatType()),
    StructField("unit", StringType()),
    StructField("quality_code", StringType()),
    StructField("anomaly_type", StringType()),
    StructField("sequence_no", IntegerType()),
    StructField("firmware_version", StringType()),
    ])

    readable_df = kafka_df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )

    parsed_df = readable_df.select(
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        sf.from_json(readable_df.value, schema_sensor_event).alias("event")
    )
    
    event_df = parsed_df.select("event.*")
    event_df = event_df.withColumn("event_time", sf.to_timestamp("event_time"))
    result = event_df\
            .withWatermark("event_time", "10 minutes")\
            .groupBy(sf.window("event_time", "5 minutes"),"sensor_id")\
            .avg("metric_value")

    query = (
        result.writeStream
        .format("console")
        .outputMode("update")
        .start()
    )

    query.awaitTermination()



def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "sensor-events"),
    )

    args = parser.parse_args()

    consumer_kafka_to_sparks(args.bootstrap_servers, args.topic)

# def main():
#     spark = build_spark_session()
#     data = [
#     ("sensor-temp-01", 52.3, "OK"),
#     ("sensor-pressure-01", 9.2, "WARN"),
#     ("sensor-temp-01", 48.7, "OK"),
#     ]

#     df = spark.createDataFrame(
#     data,
#     ["sensor_id", "metric_value", "quality_code"]
#     )
#     df.printSchema()
#     df.show()
#     spark.stop()

if __name__ == "__main__":
    main()