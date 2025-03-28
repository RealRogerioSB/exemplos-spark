from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, map_keys
from pyspark.sql.types import MapType, StringType, StructField, StructType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

dataDictionary = [
    ("James", {"hair": "black", "eye": "brown"}),
    ("Michael", {"hair": "brown", "eye": None}),
    ("Robert", {"hair": "red", "eye": "black"}),
    ("Washington", {"hair": "grey", "eye": "grey"}),
    ("Jefferson", {"hair": "brown", "eye": ""})
]

df = spark.createDataFrame(data=dataDictionary, schema=["name", "properties"])
df.printSchema()
df.show(truncate=False)

# Using StructType schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True)
])
df2 = spark.createDataFrame(data=dataDictionary, schema=schema)
df2.printSchema()
df2.show(truncate=False)

df3 = df.rdd.map(lambda x: (x.name, x.properties["hair"], x.properties["eye"])) \
    .toDF(["name", "hair", "eye"])
df3.printSchema()
df3.show()

df.withColumn("hair", df.properties.getItem("hair")) \
    .withColumn("eye", df.properties.getItem("eye")) \
    .drop("properties") \
    .show()

df.withColumn("hair", df.properties["hair"]) \
    .withColumn("eye", df.properties["eye"]) \
    .drop("properties") \
    .show()

# Functions
keysDF = df.select(explode(map_keys(df.properties))).distinct()
keysList = keysDF.rdd.map(lambda x: x[0]).collect()
keyCols = list(map(lambda x: col("properties").getItem(x).alias(str(x)), keysList))
df.select(df.name, *keyCols).show()
