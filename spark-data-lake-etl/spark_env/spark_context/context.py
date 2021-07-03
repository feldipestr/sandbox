import config
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from helpers.decrypt import decr
from logsys import LogManager


logman = LogManager().client


def create_default_session(app_name: str, **spark_config):
    try:
        spark_session = SparkSession.builder \
            .appName(app_name) \
            .master(f'spark://{config.spark_host}:{config.spark_port}') \
            .config("spark.port.maxRetries", "32") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "0") \
            .config("spark.shuffle.service.enabled", "true") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.instances", "3")\
            .config("spark.cores.max", "12") \
            .config("spark.executor.extraClassPath", "/opt/jars/s3/s3.jar:/opt/jars/cassandra.jar") \
            .config("spark.driver.extraClassPath", "/opt/jars/s3/s3.jar:/opt/jars/cassandra.jar")\
            .config("fs.s3a.endpoint", config.S3_URL)\
            .config("fs.s3a.access.key", config.S3_ACCESS_KEY)\
            .config("fs.s3a.secret.key", decr(config.S3_SECRET_KEY)) \
            .config("fs.s3a.path.style.access", "true")

        for config_name, config_value in spark_config.items():
            if config_name.startswith('spark.'):
                spark_session.config(config_name, config_value)

        spark_session = spark_session.getOrCreate()
    except Exception as ex:
        spark_session = None
        logman.error('spark_context_failed', ex)

    return spark_session


def create_spark_context(session: SparkSession):
    spark_context = None

    if session is not None:
        try:
            spark_context = session.sparkContext
        except Exception as ex:
            logman.error('spark_context_failed', ex)

    return spark_context


def create_sql_context(context: SparkContext):
    sql_context = None

    if context is not None:
        try:
            sql_context = SQLContext(context)
        except Exception as ex:
            logman.error('spark_sql_context_failed', ex)

    return sql_context
