from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "Smith", "M", 30),
    ("Anna", "Rose", "F", 41),
    ("Robert", "Williams", "M", 62),
]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()

df.select(concat_ws(",", df.firstname, df.lastname).alias("name"),
          df.gender, lit(df.salary * 2).alias("new_salary")).show()
print(df.collect())

rdd = df.rdd.map(lambda x: (x[0] + "," + x[1], x[2], x[3] * 2))
df2 = rdd.toDF(["name", "gender", "new_salary"])
df2.show()

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x["firstname"] + "," + x["lastname"], x["gender"], x["salary"] * 2))

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x.firstname + "," + x.lastname, x.gender, x.salary * 2))


def func1(x):
    first_name = x.firstname
    last_name = x.lastName
    name = first_name + "," + last_name
    gender = x.gender.lower()
    salary = x.salary * 2
    return name, gender, salary


rdd2 = df.rdd.map(lambda x: func1(x))


# Foreach example
def f(x):
    print(x)


df.rdd.foreach(f)

df.rdd.foreach(lambda x: print("Data ==>" + x["firstname"] + "," + x["lastname"] + "," + x["gender"] + "," +
                               str(x["salary"] * 2)))

# Iterate collected data
dataCollect = df.collect()
for row in dataCollect:
    print(row["firstname"] + "," + row["lastname"])

# Convert to Pandas and Iterate
dataCollect = df.rdd.toLocalIterator()
for row in dataCollect:
    print(row["firstname"] + "," + row["lastname"])

pandasDF = df.toPandas()
for index, row in pandasDF.iterrows():
    print(row["firstname"], row["gender"])
