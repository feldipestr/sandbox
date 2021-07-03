from pyspark.sql import Window, functions as F, DataFrame
import pyspark.sql.types as T
import spark_env
from numpy import median
from config import s3_bucket_name
from logsys import LogManager
from interfaces import AbstractETLJob
from helpers import get_cassandra_cluster_options
from storages import S3Storage
from storages import ScyllaBatchQuery, ScyllaConnection


class DefaultETLJob(AbstractETLJob):
    @staticmethod
    def field_exists(field):
        if field:
            return 1
        else:
            return 0

    @staticmethod
    def bit_str_to_int(bit_str: str):
        return int(bit_str, 2)

    def __init__(self, job_name='<undefined>', **kwargs):
        self.logman = LogManager().client
        self.job_name = job_name
        self.input_path = kwargs.get('input_path', '')
        self.output_path = kwargs.get('output_path', '')
        spark_config = kwargs.get('spark_config', {})

        self.cluster_options = {}
        cluster_name = kwargs.get('cluster_name', '')
        if cluster_name:
            self.cluster_options = get_cassandra_cluster_options(cluster_name)
            spark_config.update(self.cluster_options)

        self.sp_session = spark_env.create_default_session(app_name=self.job_name, **spark_config)
        self.sp_context = spark_env.create_spark_context(session=self.sp_session)
        self.sql_context = spark_env.create_sql_context(context=self.sp_context)

        self.udf = {'field_exists': F.udf(DefaultETLJob.field_exists, T.IntegerType()),
                    'median': F.udf(lambda x: float(median(x)), T.FloatType()),
                    'bit_str_to_int': F.udf(DefaultETLJob.bit_str_to_int, T.StringType()),
                    }

    def validate(self, *args, **kwargs):
        return self.job_name is not None and self.job_name and self.job_name != '<undefined>'

    def run(self, *args, **kwargs):
        pass

    @staticmethod
    def source_df_cache_path():
        return 's3_bucket/source_df_cached_data.parquet'

    def get_source_df(self, use_cache: bool = False):
        if use_cache:
            source_df = self.sql_context.read.format("parquet").load(DefaultETLJob.source_df_cache_path())
        else:
            source_df = self.sql_context.read.format("parquet").load("s3_bucket/raw_source_df.parquet")
            source_df = source_df.filter(~F.array_contains("list_column_name_1", 'value1')).filter(source_df.deleted == 0)

            source_df = source_df \
                .withColumn("field_exists_1", self.udf['field_exists'](F.col("column_name_1"))) \
                .drop('column_name_2')

        return source_df

    def get_source_df_1(self, use_cache: bool = False):
        source_df = self.get_source_df(use_cache=use_cache)

        kv_filters = {
            'key1': ['val1', 'val2'],
            'key2': ['val3', 'val4'],
        }

        # some business logic
        source_df = source_df.filter(source_df.column_101.isin(['A', 'B', 'C', ])) \
            .filter(source_df.id.isNotNull()) \
            .withColumn('group', F.when(F.col('column_name_3').isin(kv_filters['key1']), 'column_name_5')
                        .when(F.col('column_name_3').isin(kv_filters['key2']), 'column_name_6')
                        .otherwise('none'))

        return source_df

    def write_to_scylla(self,
                        dataframe: DataFrame,
                        table: str,
                        keyspace: str,
                        order_by_columns: list = [],
                        force_truncate: bool = True) -> bool:

        options = {'table': table, 'keyspace': keyspace}

        if order_by_columns:
            dataframe = dataframe.orderBy(order_by_columns)

        if force_truncate:
            truncate_stm = f'truncate table {keyspace}.{table} '

            batch_query = ScyllaBatchQuery(
                key=f'truncate_table_{table}',
                query_stm=truncate_stm,
                query_params={},
                query_result=None,
                class_row_factory=None
            )

            scy_client = ScyllaConnection().client
            scy_client.execute_batch(
                batch_queries=[batch_query],
                keyspace=keyspace,
                cluster_options=self.cluster_options
            )

            dataframe.write \
                .mode("Append") \
                .format("org.apache.spark.sql.cassandra") \
                .options(**options) \
                .save()
        else:
            dataframe.write \
                .mode("Overwrite") \
                .option("confirm.truncate", "true") \
                .format("org.apache.spark.sql.cassandra") \
                .options(**options) \
                .save()

        return True

    def scylla_queue_sink(self, queue_folder: str, keyspace: str, table_name: str, order_by_columns: list = []):
        # retrieve last modified source file
        s3 = S3Storage()
        folder_files = s3.get_sorted_folder_files(folder=queue_folder)

        # write data to Scylla
        res = False
        if folder_files[0]:
            source_file_path = f'{s3_bucket_name}/{folder_files[0]}'

            dataframe = self.sql_context.read.format("parquet").load(source_file_path)

            # write data to scylla
            res = self.write_to_scylla(
                dataframe=dataframe,
                table=table_name,
                keyspace=keyspace,
                order_by_columns=order_by_columns
        )

        return res

    def scylla_get_working_table_name(self, keyspace: str, alias: str):
        res = (alias, alias)

        sql_stm = f'select working_table_name, ' \
                  f'       next_table_name ' \
                  f'  from {keyspace}.metadata_current_table ' \
                  f' where main_alias = %(alias)s'

        stm_params = {'alias': alias}

        batch_query = ScyllaBatchQuery(
            key=f'scylla_get_working_table_name_{alias}',
            query_stm=sql_stm,
            query_params=stm_params,
            query_result=None,
            class_row_factory=None
        )

        scy_client = ScyllaConnection().client
        batch_res = scy_client.execute_batch(
            batch_queries=[batch_query],
            keyspace=keyspace,
            cluster_options=self.cluster_options
        )

        if batch_res and batch_query.query_result:
            table_name = batch_query.query_result[0]['next_table_name']
            table_name_next = batch_query.query_result[0]['working_table_name']

            res = (table_name, table_name_next)

        return res

    def scylla_set_working_table_name(self, keyspace: str, alias: str, working_table_name: str, next_table_name: str):
        upd_stm = f'update {keyspace}.metadata_current_table' \
                  f' set working_table_name = %(working_table_name)s, ' \
                  f'     next_table_name = %(next_table_name)s' \
                  f' where main_alias = %(alias)s'
        stm_params = {'working_table_name': working_table_name, 'next_table_name': next_table_name, 'alias': alias}

        upd_batch_query = ScyllaBatchQuery(
            key=f'scylla_set_working_table_name_{alias}',
            query_stm=upd_stm,
            query_params=stm_params,
            query_result=None,
            class_row_factory=None
        )

        scy_client = ScyllaConnection().client
        res = scy_client.execute_batch(
            batch_queries=[upd_batch_query],
            keyspace=keyspace,
            cluster_options=self.cluster_options
        )

        return res

    def scylla_table_sink(self, keyspace: str, table_main_alias: str, queue_folder: str, order_by_columns: list = []):
        res = False

        table_name, table_name_next = self.scylla_get_working_table_name(keyspace=keyspace, alias=table_main_alias)

        sink_result = self.scylla_queue_sink(
            queue_folder=queue_folder,
            keyspace=keyspace,
            table_name=table_name,
            order_by_columns=order_by_columns
        )

        if sink_result and table_name != table_name_next:
            res = self.scylla_set_working_table_name(
                keyspace=keyspace,
                alias=table_main_alias,
                working_table_name=table_name,
                next_table_name=table_name_next
        )

        return res
