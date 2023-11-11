from pyspark.sql import SparkSession
from accumulator_process import process_data


def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()


def test_rdd(spark):
    return spark.sparkContext.parallelize([1, 2, 3])


def global_accumulator_module():
    spark = get_spark()
    #global cnt
    cnt = spark.sparkContext.accumulator(0)
    print(process_data(test_rdd(spark)))


global_accumulator_module()