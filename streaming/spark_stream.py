import argparse
import os

from pyspark.sql import SparkSession

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
    readable_df = kafka_df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )
    query = (
        readable_df.writeStream
        .format("console")
        .outputMode("append")
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