from pyspark.sql import SparkSession
import sys

def transform_load(input_csv1, input_csv2, output_path):
    spark = SparkSession.builder \
        .appName("IncrementalETL") \
        .master("local[*]") \
        .getOrCreate()

    df1 = spark.read.option("header", True).csv(input_csv1)
    df2 = spark.read.option("header", True).csv(input_csv2)

    df = df1.unionByName(df2)
    df = df.dropDuplicates(["id"])
    df.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    transform_load(sys.argv[1], sys.argv[2], sys.argv[3])
