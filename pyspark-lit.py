from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("111", 50000),
    ("222", 60000),
    ("333", 40000)
]

columns = ["EmpId", "Salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.select(col("EmpId"), col("Salary"), lit("1").alias("lit_value1"))
df2.show(truncate=False)

df3 = df2.withColumn("lit_value2",
                     when(col("Salary") >= 40000 & col("Salary") <= 50000, lit("100")).otherwise(lit("200")))
df3.show(truncate=False)
