from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James, A, Smith", "2018", "M", 3000),
    ("Michael, Rose, Jones", "2010", "M", 4000),
    ("Robert,K,Williams", "2010", "M", 4000),
    ("Maria,Anne,Jones", "2005", "F", 4000),
    ("Jen,Mary,Brown", "2010", "", -1)
]

columns = ["name", "dob_year", "gender", "salary"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df.show(truncate=False)

df2 = df.select(split(col("name"), ",").alias("NameArray")).drop("name")
df2.printSchema()
df2.show()

df.createOrReplaceTempView("PERSON")
spark.sql("select split(name) as NameArray from PERSON").show()
