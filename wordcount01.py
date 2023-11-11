from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,IntegerType


spark = SparkSession.builder.master("local[*]").appName("romeoJulietWordCount").getOrCreate()
sc = spark.sparkContext
inprdd = sc.textFile("D:/Spark_Scala/data/wordcount/romeojuliet.txt")
#convert to lower case,
# split based on the space
#count the words

"""for x in inprdd.take(10):
    print(x)"""
outputRDD =  inprdd\
    .map(lambda x: x.lower())\
    .flatMap(lambda x: x.split(" ")).map(lambda x : (x,1)).filter(lambda x: ((x[0] != ''),x[1]))\
    .reduceByKey(lambda a,b : a + b)
    #.toDF("word","count")
#outputDF.show()
for x in outputRDD.take(10):
    print(x)


outputDF = outputRDD.toDF(["words","count"])
"""schema = StructType([StructField("words",StringType(),True),
                     StructField("count",IntegerType(),True)])
outputDF = spark.createDataFrame(outputRDD,schema=schema)"""
outputDF.show()