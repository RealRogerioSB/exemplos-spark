import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = [("Scott", 50), ("Jeff", 45), ("Thomas", 54), ("Ann", 34)]

sparkDF = spark.createDataFrame(data, ["name", "age"])
sparkDF.printSchema()
sparkDF.show()

print((sparkDF.count(), len(sparkDF.columns)))


def spark_shape(data_frame):
    return data_frame.count(), len(data_frame.columns)


pyspark.sql.dataframe.DataFrame.shape = spark_shape
print(sparkDF.shape())

pandasDF = sparkDF.toPandas()
print(pandasDF.shape)
