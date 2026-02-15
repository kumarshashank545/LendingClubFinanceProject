from pyspark.sql import SparkSession
from lib import config_reader


def get_spark_session(env):
    spark_conf = config_reader.get_pyspark_config(env)

    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=spark_conf) \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
