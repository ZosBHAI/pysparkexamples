from pyspark.sql import SparkSession
import re
spark = SparkSession.builder.appName("JustEnoughScalaForSpark").master("local[*]").getOrCreate()


import os

def info(message):
    return message

def error(message):
    fullMessage = """   
    |********************************************************************
    |
    | ERROR: {}
    |
    |********************************************************************
|
    """.format(message)

    return fullMessage


print(error("This is awfull"))

files = os.listdir("D:\Spark_Scala\data\wordcount")

plays = ("tamingoftheshrew", "comedyoferrors", "loveslabourslost", "midsummersnightsdream"
"merrywivesofwindsor", "muchadoaboutnothing", "asyoulikeit", "twelfthnight","romeojuliet.txt")
for file in files:
    if file  not in plays:
        print(error(f"Data directory path doesn't exist! {file}"))
    else:
        print(info(f"{file} exists"))


#path = "D:\Spark_Scala\data\wordcount\"



firstRDD = spark.sparkContext.wholeTextFiles("D:\Spark_Scala\data\wordcount")
tmp_storage1 = firstRDD.flatMap(lambda x: re.split("\W+",x[1])).take(10)
print(tmp_storage1)
#tmp_storage = firstRDD.take(10)
#print(tmp_storage)
