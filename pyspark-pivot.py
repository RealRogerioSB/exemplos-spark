from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
    ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
    ("Banana", 2000, "Canadá"), ("Carrots", 2000, "Canadá"), ("Beans", 2000, "México")
]

columns = ["Product", "Amount", "Country"]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)

pivotDF = df \
    .groupBy("Product", "Country") \
    .sum("Amount") \
    .groupBy("Product") \
    .pivot("Country") \
    .sum("sum(Amount)")
pivotDF.printSchema()
pivotDF.show(truncate=False)

# unpivot
unpivotExpr = "stack(3, 'Canadá', Canada, 'China', China, 'México', Mexico) as (Country, Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)).where("Total is not null")
unPivotDF.show(truncate=False)
