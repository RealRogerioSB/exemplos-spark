import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

data = [["Scott", 50], ["Jeff", 45], ["Thomas", 54], ["Ann", 34]]

# Create the pandas DataFrame
pandasDF = pd.DataFrame(data, columns=["Name", "Age"])

# print dataframe.
print(pandasDF)

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

sparkDF = spark.createDataFrame(pandasDF)
sparkDF.printSchema()
sparkDF.show()

# sparkDF = spark.createDataFrame(pandasDF.astype(str))

mySchema = StructType([
    StructField("First Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

sparkDF2 = spark.createDataFrame(pandasDF, schema=mySchema)
sparkDF2.printSchema()
sparkDF2.show()

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

pandasDF2 = sparkDF2.select("*").toPandas
print(pandasDF2)

test = spark.conf.get("spark.sql.execution.arrow.enabled")
print(test)

test123 = spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
print(test123)
