import sys
import datetime
from pyspark.sql import SparkSession
from modules.dataGenerator import DataGenerator

# Spark session & context
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("data-generator-driver")
         .getOrCreate())

# Set dynamic partitions to overwrite only the partition in DF
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

sc = spark.sparkContext

sc.setLogLevel("WARN")

##################################
# Arguments
# sys.argv[1] = file_format
# sys.argv[2] = entity_base_name
# sys.argv[3] = num_rows
##################################

##################################
# Generate test data
##################################
# Set up parameters
file_format = "parquet"
base_name = "test_data"
entity = "{entity_base_name}_{file_format}".format(file_format=file_format, entity_base_name=base_name)
data_path = "D:\Spark_Scala\data\schemaevolution{}".format(entity)
num_rows = 10

# Generate test data
print("\n**********************************************************")
print("Generating test data...".format(file_format))
print("Entity:", entity)
print("Format:", file_format)
print("**********************************************************")
data_gen = DataGenerator(spark, sc)
data_gen.gen_data_simple_schema(data_path, datetime.date(2020,1,1), num_rows, file_format)
data_gen.gen_data_add_nested_struct(data_path, datetime.date(2020,2,1), num_rows, file_format)
data_gen.gen_data_add_columns(data_path, datetime.date(2020,3,1), num_rows, file_format)
data_gen.gen_data_change_datatype_add_struct(data_path, datetime.date(2020,4,1), num_rows, file_format)
data_gen.gen_data_change_column_name(data_path, datetime.date(2020,5,1), num_rows, file_format)
data_gen.gen_data_remove_column(data_path, datetime.date(2020,6,1), num_rows, file_format)