from datetime import datetime
import argparse
import all_types
import config
from defaults import DefaultETLJob


class PipelineJob1Sink(DefaultETLJob):
    def __init__(self, job_name='<undefined>', **kwargs):
        super().__init__(job_name=job_name, **kwargs)
        self.tab_alias = kwargs.get('target_table_alias', '')

    def run(self, keyspace: str):
        self.logman.info('pipeline_job_start', self.job_name, self.__class__.__name__, f'{datetime.utcnow()} (UTC)')

        run_result = all_types.PipelineResult()

        run_result.result_bool = self.scylla_table_sink(
            keyspace=keyspace,
            table_main_alias=self.tab_alias,
            queue_folder=self.input_path,
            order_by_columns=config.get(self.tab_alias, [])
        )

        self.logman.info('pipeline_job_finish', self.job_name, self.__class__.__name__, f'{datetime.utcnow()} (UTC)')

        return run_result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument("--cassandra_cluster_name", required=False, default='cluster_name_1')
    required_args.add_argument("--cassandra_keyspace", required=False, default=config.cassandra_keyspace)
    required_args.add_argument("--job_name", required=False, default='job1: scylla sink')
    required_args.add_argument("--input_path", required=False, default='s3_bucket_path/job1_prepare_data_results')
    required_args.add_argument("--output_path", required=False, default='')

    parsed_args = parser.parse_args()

    # job specific configuration
    job_spark_config = {
        "spark.cores.max": '6',
        "spark.executor.instances": '6',
        "spark.executor.cores": '1',
        "spark.executor.memory": '8g',
        "spark.cassandra.output.batch.size.rows": 10000,
        "spark.cassandra.output.concurrent.writes": 50,
    }

    job_cassandra_table_alias = 'table_1'

    # main pipeline
    p = PipelineJob1Sink(
        job_name=parsed_args.job_name,
        input_path=parsed_args.input_path,
        output_path='',
        cluster_name=parsed_args.cassandra_cluster_name,
        spark_config=job_spark_config,
        target_table_alias=job_cassandra_table_alias,
    )

    res = p.run(parsed_args.cassandra_keyspace)
