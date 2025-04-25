# PySpark - Course Summary

## Topics Covered (Theory)

- Transformations & Actions 
- DAG & Lazy Evaluation
- Spark SQL Engine / Query Optimizer / Catalyst Optimizer
- RDD (Resilient Distributed Dataset)
- SparkSession & SparkContext
- Applications → Jobs → Stages → Tasks
- Repartition vs Coalesce
- Spark Joins  
  - shuffle sort merge join 
  - shuffle hash join
  - Broadcast Join
- Spark Memory Management
  - Driver Memory  
  - Executor Memory  

## Practical Sessions

### How to Flatten Nested JSON
#### `explode()`
Flattens an array or map into multiple rows and skips null or empty arrays.

#### `explode_outer()`
Flattens an array and Preserves null by returning one row with null.
