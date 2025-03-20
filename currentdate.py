from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

schema = StructType([StructField("seq", StringType(), True)])

dates = ["1"]

df = spark.createDataFrame(list("1"), schema=schema)

df.show()
