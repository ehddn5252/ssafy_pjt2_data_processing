from pyspark.sql import SparkSession
from pprint import pprint
# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# CSV
lines = spark.read.option("header", True).csv("./data/events_sample.csv")
pprint(lines)
# TXT
# lines = spark.sparkContext.textFile("./data/fake_person.txt")

# TXT with schema
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# schema = StructType(
#     [StructField("id", IntegerType(), True), StructField("name", StringType(), True),]
# )
#
# names = spark.read.option("sep", " ").schema(schema).csv("./data/Marvel-names.txt")
