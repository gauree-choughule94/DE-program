from pyspark.sql import SparkSession
import pandas as pd
import mysql.connector

spark = SparkSession.builder \
    .appName("Process API Data") \
    .getOrCreate()

df = spark.read.json("/opt/airflow/dags/tmp/products.json")
df = df.select("id", "title", "price", "brand")

df.show()

# Save to MySQL (pandas + connector used here for simplicity)
pandas_df = df.toPandas()

if not pandas_df.empty:
    conn = mysql.connector.connect(
        host="mysql",
        user="airflow",
        password="airflow",
        database="airflowdb"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT PRIMARY KEY,
            title VARCHAR(255),
            price FLOAT,
            brand VARCHAR(255)
        )
    """)
    for _, row in pandas_df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO products (id, title, price, brand) VALUES (%s, %s, %s, %s)
        """, tuple(row))
    conn.commit()
    conn.close()
