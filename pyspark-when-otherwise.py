from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "", "Smith", "36636", "M", 60000),
    ("Michael", "Rose", "", "40288", "M", 70000),
    ("Robert", "", "Williams", "42114", "", 400000),
    ("Maria", "Anne", "Jones", "39192", "F", 500000),
    ("Jen", "Mary", "Brown", "", "F", 0)
]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

# Using when otherwise
df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male")
                    .when(col("gender") == "F", "Female")
                    .otherwise("Unknown"))
df2.show(truncate=False)

df.select(col("*"), when(col("gender") == "M", "Male")
          .when(col("gender") == "F", "Female")
          .otherwise("Unknown").alias("new_gender")).show(truncate=False)

# Using case when
df3 = df.withColumn("new_gender",
                    expr("case when gender = 'M' then 'Male' when gender = 'F' then 'Female' else 'Unknown' end"))
df3.show(truncate=False)

# Using case when
df4 = df.select(col("*"),
                expr("case when gender = 'M' then 'Male' when gender = 'F' then 'Female' else 'Unknown' end") \
                .alias("new_gender"))
df4.show(truncate=False)

data2 = [(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]

df5 = spark.createDataFrame(data=data2, schema=["id", "code", "amt"])

df5.withColumn("new_column", when(col("code") == "a" | col("code") == "d", "A")
               .when(col("code") == "b" & col("amt") == "4", "B")
               .otherwise("A1")).show()
