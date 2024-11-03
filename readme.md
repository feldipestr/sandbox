# High-Performance Batch ETL: Ingest S3 Parquet Data to ScyllaDB with Spark & Airflow

This project is a high-performance ETL (Extract, Transform, Load) framework 
designed for efficient data ingestion from Amazon S3 to [ScyllaDB](https://www.scylladb.com/), 
making it ideal for large-scale, batch data processing. Using Apache Spark and Apache Airflow, 
it reads data stored as Parquet files in an Amazon S3 data lake, processes it, 
and writes the results to ScyllaDB. 

This setup is optimized for environments that demand high throughput and low latency.

### Configuration Values
All configuration values in this repository are placeholders. Replace them with real values as needed.

### Spark SQL Queries
The SQL queries are examples to illustrate the data flow in the ETL process. 
Replace them with your actual queries.

### Spark Context
The Spark context here is an example setup and should be configured with real values.

## Simple Data Flow
This repository includes a sample pipeline called [job1](/job1). It has two tasks:
 - [job1-prepare-data](/job1/job1_prepare_data.py): pulls Parquet files from 
   the S3 data lake for transformation.
 - [job1-save-data](/job1/job1_save_data.py): saves results to ScyllaDB.

Each task has a corresponding Airflow DAG (see [dags](/dags) folder) to run 
them separately on their own schedule.

### Adding New Pipelines
To add new pipelines, create folders such as ```job2```, ```job3```, etc. 
Place each task in a corresponding DAG in the ``dags`` folder and schedule them.

### Adding New Storage Options
To add a new storage option as a data source or destination, create a custom 
driver in the [storages](/storages) folder, similar to 
[s3_storage](/storages/s3_storage) and [scylla_storage](/storages/scylla_storage).
