from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType

spark = SparkSession.builder.master("local[*]").appName("Local Application").getOrCreate()
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
rddDF = rdd.toDF(columns)
rddDF.show()

with open("testschema.json","w") as f:
    json.dump(rddDF.schema.jsonValue(),f)

# Read the schema
with open("testschema.json") as f:
    new_schema = StructType.fromJson(json.load(f))
    print(new_schema.simpleString())



schemabased_rdd_df = spark.createDataFrame(data=data,schema=new_schema)
schemabased_rdd_df.show()

hist_path=r"C:\SparkDataset\CDC\test_actualdata.txt"
snap_path=r"C:\SparkDataset\CDC\test_snapshotdata.txt"
hist = spark.read.option("delimiter","|").option("header",True).option("inferSchema",True).format("csv").load(hist_path)
snap = spark.read.option("delimiter","|").option("header",True).option("inferSchema",True).format("csv").load(snap_path)


with open("history.json","w") as f:
    json.dump(hist.schema.jsonValue(),f)

with open("snap.json","w") as f:
    json.dump(snap.schema.jsonValue(),f)


with open("history.json") as f:
    new_schema_hist = StructType.fromJson(json.load(f))
    print(new_schema_hist.simpleString())

with open("snap.json") as f:
    new_schema_snap = StructType.fromJson(json.load(f))
    print(new_schema_snap.simpleString())


new_hist = spark.read.schema(new_schema_hist).option("delimiter","|").format("csv").load("C:\\SparkDataset\\CDC\\test_no_header_actualdata.txt")
new_hist.show()
new_snap = spark.read.schema(new_schema_snap).option("delimiter","|").format("csv").load("C:\\SparkDataset\\CDC\\test_no_header_snapshotdata.txt")
new_snap.show()

test_new_snap = spark.read.option("delimiter","|").option("inferschema",True).format("csv").load("C:\\SparkDataset\\CDC\\test_no_header_snapshotdata.txt")
test_new_snap.show()