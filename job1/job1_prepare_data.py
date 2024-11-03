import time
from datetime import datetime
import argparse
from pyspark.sql import Window, functions as F
import all_types
from defaults import DefaultETLJob


class PipelineEMDFeatures(DefaultETLJob):
    def __init__(self, job_name='<undefined>', **kwargs):
        super().__init__(job_name=job_name, **kwargs)

    def run(self):
        self.logman.info('pipeline_job_start', self.job_name, self.__class__.__name__, f'{datetime.utcnow()} (UTC)')

        run_result = all_types.PipelineResult()

        if not self.validate():
            run_result.result_bool = False
            self.logman.error('pipeline_validation_fail', self.job_name, self.__class__.__name__)
            self.logman.info('pipeline_job_finish', self.job_name, self.__class__.__name__, f'{datetime.utcnow()} (UTC)')
            return run_result

        data_lake_source_df = self.get_source_df_1(use_cache=True)

        w1 = Window.partitionBy(data_lake_source_df.uid) \
            .orderBy(data_lake_source_df.column_name_1) \
            .rowsBetween(Window.unboundedPreceding, -1)

        w2 = Window.partitionBy(data_lake_source_df.uid) \
            .orderBy(data_lake_source_df.column_name_1) \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        data_lake_source_df = data_lake_source_df \
            .withColumn('sample_duration', F.coalesce((F.unix_timestamp('timestamp_2_utc') - F.unix_timestamp('timestamp_1_utc')) / 3600, F.lit(0))) \
            .withColumn('hour_1', F.coalesce(F.hour("timestamp_2_utc"), F.lit(0))) \
            .withColumn('hour_2', F.coalesce(F.hour("timestamp_1_utc"), F.lit(0))) \
            .withColumn('month_1', F.coalesce(F.month("timestamp_2_utc"), F.lit(0))) \
            .withColumn("sample_count_over", F.count("key_column_1").over(w1)) \
            .withColumn('column_name_1_min', F.coalesce(F.min("column_name_1").over(w2), F.lit(0))) \
            .withColumn("udf_example_1", F.coalesce(self.udf['field_exists'](F.col("column_name_3")), F.lit('0'))) \
            .filter((F.col('column_name_4') == 'some_value_1') | ((F.col('column_name_3') != 'some_value_2') & (F.col('column_name_4') > 100.0)))

        # write to results folder
        day_name = time.strftime('%Y%m%d')
        hours_name = time.strftime('%H%M%S')
        file_name = f'{day_name}-{hours_name}.parquet'
        scylla_queue_path = self.output_path.format(file_name)
        data_lake_source_df\
            .select(['key_column_1', 'month_1', 'sample_duration', 'sample_count_over', 'column_name_1_min', 'udf_example_1', ])\
            .write.mode('overwrite')\
            .parquet(scylla_queue_path)

        self.logman.info('pipeline_job_finish', self.job_name, self.__class__.__name__, f'{datetime.utcnow()} (UTC)')

        run_result.result_bool = True

        return run_result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument("--job_name", required=False, default='job1: prepare source data')
    required_args.add_argument("--input_path", required=False, default='')
    required_args.add_argument("--output_path", required=False, default='s3_bucket_path/job1_prepare_data_results/{0}')
    parsed_args = parser.parse_args()

    p1 = PipelineEMDFeatures(job_name=parsed_args.job_name,
                             input_path=parsed_args.input_path,
                             output_path=parsed_args.output_path,
                             spark_config={"spark.cores.max": '12',
                                           "spark.executor.instances": '4',
                                           "spark.executor.cores": '3',
                                           "spark.executor.memory": '8g',
                                           }
                             )

    res = p1.run()
