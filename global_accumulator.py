from pyspark.sql import SparkSession

cnt=0


def add_items(x):
    global cnt
    cnt += x


def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()


def test_rdd(spark):
    return spark.sparkContext.parallelize([1, 2, 3])


def global_accumulator():
    spark = get_spark()
    global cnt
    cnt = spark.sparkContext.accumulator(0)
    test_rdd(spark).foreach(add_items)
    print(cnt.value)

global_accumulator()