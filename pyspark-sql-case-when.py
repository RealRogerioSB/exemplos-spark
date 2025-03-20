from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "M", 60000),
    ("Michael", "M", 70000),
    ("Robert", None, 400000),
    ("Maria", "F", 500000),
    ("Jen", "", None)
]

columns = ["name", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()

# Using When otherwise
df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male")
                    .when(col("gender") == "F", "Female")
                    .when(df.gender.isNull(), "")
                    .otherwise(df.gender))
df2.show()
df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male")
                    .when(col("gender") == "F", "Female")
                    .when(df.gender.isNull(), "")
                    .otherwise(df.gender))

df3 = df.select(col("*"), when(col("gender") == "M", "Male")
                .when(col("gender") == "F", "Female")
                .when(df.gender.isNull(), "")
                .otherwise(df.gender).alias("new_gender"))
df3.show()
# Using SQL Case When
df4 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
                                       "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
                                       "ELSE gender END"))
df4.show()

df5 = df.select(col("*"), expr("CASE WHEN gender = 'M' THEN 'Male' " +
                               "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
                               "ELSE gender END").alias("new_gender"))

df.createOrReplaceTempView("EMP")
spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " +
          "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
          "ELSE gender END as new_gender from EMP").show()
