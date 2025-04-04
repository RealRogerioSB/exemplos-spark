from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True)
])
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
df.printSchema()

df1 = spark.sparkContext.parallelize([]).toDF(schema)
df1.printSchema()

df2 = spark.createDataFrame([], schema)
df2.printSchema()

df3 = spark.emptyDataFrame()
df3.printSchema()
