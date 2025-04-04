from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

df = spark.createDataFrame([["02-03-2013"], ["05-06-2023"]], ["input"])

df.select(col("input"), to_date(col("input"), "MM-dd-yyyy").alias("date")).show()

spark.sql("select to_date('02-03-2013', 'MM-dd-yyyy') date").show()
