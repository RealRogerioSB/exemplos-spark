from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

df = spark.read.option("header", True).csv("resources/simple-zipcodes.csv")

newDF = df.repartition(3)
print(newDF.rdd.getNumPartitions())

newDF.write.option("header", True).mode("overwrite").csv("resources/zipcodes-state")

df2 = df.repartition(4, "state")
df2.write.option("header", True).mode("overwrite").csv("resources/zipcodes-state-3states")

df3 = df.repartition("state")
df3.write.option("header", True).mode("overwrite").csv("resources/zipcodes-state-allstates")
