# SPARK
spark_host = '192.168.1.1'
spark_port = '1234'

# SCYLLA
cassandra_clusters = {
    'cluster_name_1': {
        "spark.cassandra.connection.host": "192.168.0.1, 192.168.0.2, 192.168.0.3",
        "spark.cassandra.auth.username": "user",
        "spark.cassandra.auth.password": 'encrypted_password',
        "spark.cassandra.connection.port": "9042"
    },
    'cluster_name_2':  {
        "spark.cassandra.connection.host": "192.168.0.4, 192.168.0.5, 192.168.0.6",
        "spark.cassandra.auth.username": "user",
        "spark.cassandra.auth.password": 'encrypted_password',
        "spark.cassandra.connection.port": "9042"
    },
}
cassandra_keyspace = 'default_keyspace'

tables_keys = {
    'table_1': ['key_column_name_1', 'key_column_name_2', ],
    'table_2': ['key_column_name_1', 'key_column_name_3', ],
}

# S3
s3_bucket_name = 'bucket_name'
s3_access_key = 'access_kay'
s3_secret_key = 'encrypted_password'
s3_region_name = 'region_name'
s3_url = 'https://bucket_url'

# LOGGER
logger_allow_std_output = True
logger_host = '192.168.1.10'
logger_port = '5678'

# TIMEOUTS
timeout_default = 0.5

