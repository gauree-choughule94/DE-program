from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StringType, TimestampType
import sys

def main(kafka_bootstrap_servers, topics_str, bronze_path):
    topics = topics_str.split(',')

    # Define the event schema
    event_schema = StructType() \
        .add("event_type", StringType()) \
        .add("product_id", StringType()) \
        .add("user_id", StringType()) \
        .add("timestamp", TimestampType()) \
        .add("payload", StringType())  # Optional

    spark = SparkSession.builder \
        .appName("KafkaToBronzeStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", ','.join(topics)) \
        .option("startingOffsets", "earliest") \
        .load()

    df_events = df_kafka.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp"))

    query = df_events.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", bronze_path) \
        .option("checkpointLocation", f"{bronze_path}/_checkpoints") \
        .partitionBy("event_type", "year", "month", "day") \
        .trigger(processingTime='1 minute') \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark_streaming_kafka_to_bronze.py <bootstrap_servers> <topics> <bronze_output_path>")
        sys.exit(1)

    kafka_bootstrap_servers = sys.argv[1]
    topics = sys.argv[2]
    bronze_output_path = sys.argv[3]

    main(kafka_bootstrap_servers, topics, bronze_output_path)
