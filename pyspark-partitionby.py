from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

df = spark.read.option("header", True) \
    .csv("resources/simple-zipcodes.csv")
df.show()
print(df.rdd.getNumPartitions())

df.write.option("header", True) \
    .partitionBy("state") \
    .mode("overwrite") \
    .csv("resources/zipcodes-state")

df.write.option("header", True) \
    .partitionBy("state", "city") \
    .mode("overwrite") \
    .csv("resources/zipcodes-state-city")

df = df.repartition(2)
print(df.rdd.getNumPartitions())

df.write.option("header", True) \
    .partitionBy("state") \
    .mode("overwrite") \
    .csv("resources/zipcodes-state-more")

dfPartition = spark.read.option("header", True) \
    .csv("resources/zipcodes-state")
dfPartition.printSchema()

dfSinglePart = spark.read.option("header", True) \
    .csv("resources/zipcodes-state/state=AL/city=SPRINGVILLE")
dfSinglePart.printSchema()
dfSinglePart.show()

parqDF = spark.read.option("header", True) \
    .csv("resources/zipcodes-state")
parqDF.createOrReplaceTempView("ZIPCODE")
spark.sql("select * from ZIPCODE where state='AL' and city = 'SPRINGVILLE'") \
    .show()

df.write.option("header", True) \
    .option("maxRecordsPerFile", 2) \
    .partitionBy("state") \
    .mode("overwrite") \
    .csv("resources/zipcodes-state-maxrecords")
