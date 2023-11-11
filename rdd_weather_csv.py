from pyspark.sql import SparkSession
#program to compute the maximum temperature in a Year
spark = SparkSession.builder.master("local[*]") \
    .appName("RDDapp").getOrCreate()

rdd = spark.sparkContext.textFile("C:\SparkDataset\weathercsv\weather.csv",3)
#print("Sample file content {}".format(rdd.take(10)))

selected_fields_rdd = rdd.map(lambda  line: (int(line.split("-")[0]),int(line.split(",")[2])))
max_temperature_rdd = selected_fields_rdd.reduceByKey(lambda x,y : x if x > y else y)
result = max_temperature_rdd.collect()
print(result)
