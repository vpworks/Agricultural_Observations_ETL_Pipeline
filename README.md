# PostgreSQL-Kafka-Spark-Pipeline

This repository contains an end-to-end data pipeline that extracts agricultural data from PostgreSQL, streams it to Kafka, and processes it using Apache Spark. The pipeline demonstrates batch and streaming data workflows, schema parsing, data transformations, and storage to CSV for downstream analysis.

# Producer script tasks

1) Data Extraction from PostgreSQL.
2) Serializing records to JSON messages/bytes.
3) Kafka bytes get buffered in memory temporarily.
4) Broker moves bytes to the defined topic.

# Modules Used

- json
- psycopg2
- kafka  

# Consumer Script tasks

1) Reads data bytes from Kafka.
2) Deserializes bytes to JSON strings.
3) Parses JSON strings to Spark Structured Columns.
4) Applies transformations ->
   - Date type conversion
   - Renaming columns
   - Filter
   - Column addition
5) Moves data to local storage

# Modules Used

- pyspark.sql.types
- pyspark.sql.functions

