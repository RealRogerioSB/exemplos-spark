from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

columns = ["name", "languagesAtSchool", "currentState"]
data = [
    ("James,,Smith", ["Java", "Scala", "C++"], "CA"),
    ("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"),
    ("Robert,,Williams", ["CSharp", "VB"], "NV")
]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("languagesAtSchool", concat_ws(",", col("languagesAtSchool")))
df2.printSchema()
df2.show(truncate=False)

df.createOrReplaceTempView("ARRAY_STRING")
spark.sql("select name, concat_ws(languagesAtSchool) as languagesAtSchool, currentState from ARRAY_STRING") \
    .show(truncate=False)
