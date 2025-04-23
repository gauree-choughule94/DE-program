# PySpark - Topics Covered

## 🔹 Selecting Columns

- Selecting single/multiple columns using:
  - `df.select("col1", "col2")`
  - `df.select(col("col1"), col("col2"))`
  - `df.selectExpr("col1", "col2 as alias_col2")`

## 🔹 Filtering Rows

- Using `filter()` and `where()`:

## 🔹 Adding Columns

- Using `withColumn()` and `lit()`:

## 🔹 concat

## 🔹 Renaming Columns

- Using:
  - `withColumnRenamed("old", "new")`
  - `df.select(col("old").alias("new"))`
  - `selectExpr("old as new")`

---

## 🔹 Type Casting

## 🔹 Removing Columns

## 🔹 Combining DataFrames

- `union()` and `unionAll()` (for DataFrames with same schema)
- `unionByName()` (allows column order to differ)

## 🔹 Conditional Logic [if-else]

- SQL style: `CASE WHEN` (in Spark SQL queries)
- DataFrame style:
  - `when(condition, value).otherwise(default_value)`

## 🔹 Unique & Sorted Records

## 🔹 Counting Rows

## 🔹 Aggregations

- Using aggregate functions:
  - `count()`, `sum()`, `avg()`, `min()`, `max()`

## 🔹 Group By

## started working on joins