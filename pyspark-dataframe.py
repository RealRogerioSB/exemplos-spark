from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

print(spark)
