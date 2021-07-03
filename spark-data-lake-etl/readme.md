### TL;DR;

- ETL framework to run Apache Spark batch pipelines in Apache Airflow.
- Read source data from *.parquet files in S3 data lake.
- Write results to Scylla DB (NoSQL, Cassandra based) wide-column data store.


# Spark-Data-Lake-ETL

This repository represents comprehensive ETL framework to run batch [Apache Spark](https://spark.apache.org/) pipelines in [Apache Airflow](https://airflow.apache.org/). The framework can be expanded with different storages which can be used both as data sources and destinations. 
In this repository I use [Amazon S3](https://aws.amazon.com/s3/) storage as data lake which contains *source* data and [Scylla DB](https://www.scylladb.com/)  as *target* columnar key-value storage with higher throughputs and lower latencies in comparison to [Apache Cassandra](https://cassandra.apache.org/).

# Context limitations
### Configuration values
All the configuration values in the repository are fake and should be substituted by real values.

### Spark SQL queries
All the real SQL queries are substituted by fake ones and help to understand the whole data flow logic in the ETL framework. 

### Spark context
Spark context doesn't consist of all required configuration and has to be replaced by the real one.

# Data flow
There is only one pipeline in the repository **job1** (`job1` folder) provided as an example. 

The **job1** consists of 2 autonomous/independent tasks:
 - **job1-prepare-data** (`/job1/job1_prepare_data.py` file).
 - **job1-save-data** (`/job1/job1_save_data.py` file).
 
In **job1-prepare-data** all the required data is pulled from a data lake (S3 parquet files), transformed and stored again to a results folder.

In **job1-save-data** the results folder is scanned to retrieve the last modified result file and store it to a key-value storage (Scylla DB).

Both tasks have corresponding Airflow tasks in 2 DAGs (`dags` folder) `dag_sources.py` and `dag_sinks.py`. 

Place the whole repository files structure to e.g. **airflow**'s user home directory into a separate folder (e.g. `~/<project>`) on your Apache Airflow server. 
Then move `~/<project>/dags/adding_ccpl_pipelines_dag_bag.py` file to `~/dags` directory to let Airflow know about all the available DAGs of the repository. 

### Appending new pipelines
To append a new pipeline create a new folder (e.g. **job2**) with all the new tasks required and create corresponding Airflow DAGs in `dags` folder to trigger tasks on schedule.

### Appending new storages
Append new source (or destination) storage (or database) custom driver to the `storages` folder similarly as it is done for `/storages/s3_storage` and `/storages/scylla_storage`. 

Don't forget to think about *singleton* for the source (destination) connection.
 
# ScyllaDB sink peculiarities
Although ScyllaDB has higher throughputs and lower latencies it still doesn't allow to update tens of millions key-value pairs fast without blocking reading sessions.

Fast update of the target table (e.g. 20 million key-value pairs) and at the same time not blocking all of the readers can be achieved by using two target tables. Readers can be *switched* between these tables each time the **job1-save-data** runs.

Thus, while a reader (a session) selects key-value pair(s) from the active table, say, *ScyllaDB.target_table_**1*** the pipeline truncates and appends new data to *ScyllaDB.target_table_**2*** and then makes this table active for all the readers. 
Next time the pipeline runs it truncates  and appends data to the *ScyllaDB.target_table_**1*** and again makes this table active for readers and so on and so forth.

The *switch* is possible if we use a 3-rd table where currently active target table name is stored. Each time users read key-value pairs they have to run 2 queries: **1-st** query is to select the current active target table name from the 3rd table and **2-nd** query is to search value(s) by key(s) in the current active table. 
Additional overhead for **1-st**  query is extremely small (thanks to ScyllaDB) and can be cached on a client's side with e.g. `ttl = 1 second` (or more).