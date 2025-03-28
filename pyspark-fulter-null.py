from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", None, "M"),
    ("Anna", "NY", "F"),
    ("Julia", None, None)
]

df = spark.createDataFrame(data, ["name", "state", "gender"])
df.show()

df.filter("state is NULL").show()
df.filter(df.state.isNull()).show()
df.filter(col("state").isNull()).show()

df.na.drop("state").show()
