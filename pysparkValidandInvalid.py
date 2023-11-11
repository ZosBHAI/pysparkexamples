from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").master("local[*]").appName("InvalidValid").getOrCreate()

inputDF = spark.read.csv("D:\Spark_Scala\data\weather\weather.csv", header=True)
validDF = inputDF.filter("`Temperature (C)` is not null or Humidity is not null \
   or `Wind Speed (km/h)` is not null or `Pressure (millibars)` is not null ")
invalidDF = inputDF.filter("`Temperature (C)` is null or Humidity is  null \
   or `Wind Speed (km/h)` is  null or `Pressure (millibars)` is  null ")

validDF.write.option("header",True).format("csv").\
mode("Overwrite"). \
save("D:\Spark_Scala\outputData\weather\Valid")

invalidDF.write.option("header",True).format("csv"). \
mode("Overwrite").save("D:\Spark_Scala\outputData\weather\Invalid")