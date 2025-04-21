# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("TestApp").getOrCreate()
# df = spark.range(5)
# df.show()

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("ReadCSVExample").getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("sample.csv", header=True, inferSchema=True)

# Show the data
print("Full Data:")
df.show()

# Select specific columns
print("Only names and cities:")
df.select("name", "city").show()

# Filter: people older than 30
print("People older than 30:")
df.filter(df.age > 30).show()

# Group by city and count
print("Group by city:")
df.groupBy("city").count().show()
