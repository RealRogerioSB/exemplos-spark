from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("36636", "Finance", 3000, "USA"),
    ("40288", "Finance", 5000, "IND"),
    ("42114", "Sales", 3900, "USA"),
    ("39192", "Marketing", 2500, "CAN"),
    ("34534", "Sales", 6500, "USA")
]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("location", StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

# Convert columns to Map
df = (df.withColumn(
    "propertiesMap",
    create_map(lit("salary"), col("salary"), lit("location"), col("location")))
      .drop("salary", "location"))
df.printSchema()
df.show(truncate=False)
