from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

arrayType = ArrayType(IntegerType(), False)
print(arrayType.jsonValue())
print(arrayType.simpleString())
print(arrayType.typeName())

mapType = MapType(StringType(), IntegerType())
print(mapType.keyType)
print(mapType.valueType)
print(mapType.valueContainsNull)

data = [
    ("James", "", "Smith", "36", "M", 3000),
    ("Michael", "Rose", "", "40", "M", 4000),
    ("Robert", "", "Williams", "42", "M", 4000),
    ("Maria", "Anne", "Jones", "39", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)
