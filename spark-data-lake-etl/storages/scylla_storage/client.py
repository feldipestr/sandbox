from typing import Any, List, Dict
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory
from cassandra.protocol import SyntaxException, RequestValidationException, RequestExecutionException, InvalidRequestException, InvalidRequest
from collections import namedtuple
import warnings
from wrappers import json_dump
from pydantic import BaseModel


class ScyllaBatchQuery(BaseModel):
    key: Any = None
    query_stm: str = ''
    query_params: dict = {}
    query_result: Any = None
    class_row_factory: Any = None


class Scylla(object):
    def new_session(self, keyspace: str = '', cluster_options: Dict = {}):
        auth = PlainTextAuthProvider(username=cluster_options.get('spark.cassandra.auth.username', ''),
                                     password=cluster_options.get('spark.cassandra.auth.password', ''))

        cluster = Cluster(contact_points=cluster_options.get('spark.cassandra.connection.host').split(','),
                          port=int(cluster_options.get('spark.cassandra.connection.port', '9042')),
                          protocol_version=2,
                          executor_threads=15,
                          auth_provider=auth,
                          connect_timeout=10,
                          load_balancing_policy=RoundRobinPolicy(),
        )

        new_session = cluster.connect(keyspace=keyspace, wait_for_all_pools=False)
        new_session.row_factory = dict_factory
        return new_session, cluster

    @staticmethod
    def rows_wrapper(row_dict: dict, class_row_factory: Any = None):
        if class_row_factory is not None:
            wrapped_data = class_row_factory()
            for k, val in row_dict.items():
                if hasattr(wrapped_data, k) and val is not None:
                    setattr(wrapped_data, k, val)
        else:
            wrapped_data = row_dict
        return wrapped_data

    @staticmethod
    def execute_query(session, query: namedtuple) -> List:
        result = []
        try:
            query_result = session.execute(query=query.query_stm, parameters=query.query_params)
            result = query_result.current_rows
        except (SyntaxException,
                RequestValidationException,
                RequestExecutionException,
                InvalidRequestException,
                InvalidRequest) as ex:
            warnings.warn(f'Scylla query="{query.query_stm}", params="{json_dump(query.query_params)}" execution exception: {ex}')

        return result

    def execute_batch(self,
                      batch_queries: List[namedtuple] = [],
                      keyspace: str = '',
                      cluster_options: Dict = {}) -> bool:
        res = False

        session, cluster = self.new_session(keyspace=keyspace, cluster_options=cluster_options)

        for query in batch_queries:
            result_rows = Scylla.execute_query(session, query=query)
            if result_rows:
                query.query_result = [Scylla.rows_wrapper(row_dict, query.class_row_factory) for row_dict in result_rows]
            res = True

        cluster.shutdown()

        return res


# Singleton
class ScyllaConnection(object):
    class __ScyllaConnection:
        def __init__(self):
            self.client = Scylla()

    instance = None

    def __new__(cls):
        if not ScyllaConnection.instance:
            ScyllaConnection.instance = ScyllaConnection.__ScyllaConnection()
        return ScyllaConnection.instance
