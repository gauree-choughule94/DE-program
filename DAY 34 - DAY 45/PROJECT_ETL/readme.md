## Data Pipeline with PySpark, Airflow, Kafka, PostHog, and Superset

This project implements a modular data pipeline that supports batch ETL, event streaming, and interactive analytics using open-source tools.

## Layers

1. Bronze Tier
Ingests raw transactional data from MySQL daily. Data is stored in Parquet format in the local filesystem with minimal transformation.

2. Silver Tier
Reads data from the Bronze tier, applies cleaning, schema validation, and transformation. Joins multiple datasets to prepare for analytics.

3. Gold Tier
Aggregates cleaned data to create summary tables or views optimized for reporting and business insights.

## Data Warehouse (PostgreSQL)
Gold tier data is loaded into a PostgreSQL DWH for easy querying and BI dashboarding.

## Data Archiving
Older data from all tiers (Bronze, Silver, Gold) is archived to long-term storage to optimize performance and reduce costs.

## Event Streaming and Analytics

# Kafka Producer & Consumer
Sends application events to Kafka topics and reads them via consumers for further processing.

# PostHog Integration
Captures product and user events for behavior analysis, session recording, and feature flagging.

# Gold Events DAG
Tracks and forwards Gold-tier analytics events to PostHog.

## BI and Dashboarding

# Apache Superset
Connects to the PostgreSQL DWH for visual analytics. Dashboards and charts are built using curated Gold tier data.

## DAGs (via Airflow)

Bronze, Silver, Gold data processing, Load-to-DWH DAGs

Archive DAGs for each layer

Event tracking DAG to stream Gold-tier events

## Initialization Scripts

mysql_init/init.sql: Sets up MySQL tables 

postgres_init/init_dw.sql: Creates PostgreSQL Data Warehouse schemas 
    
#### Component and its Purpose
MySQL	- Source of daily transactional data
Kafka	- Event streaming backbone
Airflow	- Orchestration of ETL jobs
PySpark	- Batch processing for all tiers
Local Filesystem -	Acts as the data lake (Bronze/Silver/Gold/Archive)
PostgreSQL	- Data Warehouse storing curated analytics data
PostHog	- Event tracking and product analytics
Kafka Consumer -	Streams event data to PostHog
Apache Superset	- BI dashboard and data exploration tool

## Containerization

All services (MySQL, Kafka, Airflow, PySpark, Superset, PostHog, PostgreSQL, etc.) are containerized using Docker, making the setup reproducible and portable.