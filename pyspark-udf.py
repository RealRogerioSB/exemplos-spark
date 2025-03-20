from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders")]
columns = ["Seqno", "Name"]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)


def convert_case(_str):
    res_str = ""
    arr = str.split(" ")

    for x in arr:
        res_str += x[0:1].upper() + x[1:len(x)] + " "

    return res_str


""" Converting function to UDF """
convertUDF = udf(lambda z: convert_case(z))

df.select(col("Seqno"), \
          convertUDF(col("Name")).alias("Name")) \
    .show(truncate=False)


@udf(returnType=StringType())
def upper_case(_str):
    return _str.upper()


upperCaseUDF = udf(lambda z: upper_case(z), StringType())

df.withColumn("Cureated Name", upper_case(col("Name"))).show(truncate=False)

# Using UDF on SQL
spark.udf.register("convertUDF", convert_case, StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE").show(truncate=False)

spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE " +
          "where Name is not null and convertUDF(Name) like '%John%'") \
    .show(truncate=False)

# null check
columns = ["Seqno", "Name"]
data = [
    ("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders"),
    ("4", None)
]

df2 = spark.createDataFrame(data=data, schema=columns)
df2.show(truncate=False)
df2.createOrReplaceTempView("NAME_TABLE2")

spark.udf.register("_nullsafeUDF", lambda _str: convert_case(_str) if _str else "", StringType())

spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2").show(truncate=False)

spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " +
          " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
    .show(truncate=False)
