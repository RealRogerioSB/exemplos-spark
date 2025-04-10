from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "Smith", "M", 3000),
    ("Anna", "Rose", "F", 4100),
    ("Robert", "Williams", "NA", 6200),
    (None, "Rob", "F", 6200)
]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()

df2 = df.withColumn("salary", df.salary * 3)
df2.show()

df3 = df \
    .withColumn("gender", when(col("gender") == "M", "Male")
                .when(col("gender") == "F", "Female")
                .otherwise(df.gender))
df3.show()

df4 = df.withColumn("salary", df.salary.cast("String"))
df4.printSchema()

df.createOrReplaceTempView("PER")
df5 = spark.sql("select firstname, gender, salary * 3 as salary from PER")
df5.show()
