from helpers.request import request_get
from helpers.decrypt import decr, decrypt_attribute
from config import cassandra_clusters


def get_cassandra_cluster_options(cluster_name: str):
    options = cassandra_clusters.get(cluster_name, {})
    decrypt_attribute(options, 'spark.cassandra.auth.password')
    return options
