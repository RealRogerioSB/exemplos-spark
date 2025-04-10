from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct, avg, collect_list, collect_set, count, countDistinct, first,
    kurtosis, last, max, mean, min, skewness, stddev, stddev_pop, stddev_samp, sum,
    sumDistinct, var_pop, var_samp, variance)

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

simpleData = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

print("approx_count_distinct:", str(df.select(approx_count_distinct("salary")).collect()[0][0]))
print("avg:", str(df.select(avg("salary")).collect()[0][0]))

df.select(collect_list("salary")).show(truncate=False)
df.select(collect_set("salary")).show(truncate=False)

df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department &amp; Salary:", str(df2.collect()[0][0]))

print("count:", str(df.select(count("salary")).collect()[0]))
df.select(first("salary")).show(truncate=False)
df.select(last("salary")).show(truncate=False)
df.select(kurtosis("salary")).show(truncate=False)
df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)
df.select(skewness("salary")).show(truncate=False)
df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)
df.select(sumDistinct("salary")).show(truncate=False)
df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(truncate=False)
