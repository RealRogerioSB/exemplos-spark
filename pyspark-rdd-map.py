from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    "Project", "Gutenberg’s", "Alice’s", "Adventures", "in", "Wonderland",
    "Project", "Gutenberg’s", "Adventures", "in", "Wonderland", "Project",
    "Gutenberg’s"
]

rdd = spark.sparkContext.parallelize(data)

rdd2 = rdd.map(lambda x: (x, 1))
for element in rdd2.collect():
    print(element)

data = [
    ("James", "Smith", "M", 30),
    ("Anna", "Rose", "F", 41),
    ("Robert", "Williams", "M", 62),
]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()

rdd2 = df.rdd.map(lambda x: (x[0] + "," + x[1], x[2], x[3] * 2))
df2 = rdd2.toDF(["name", "gender", "new_salary"])
df2.show()

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x["firstname"] + "," + x["lastname"], x["gender"], x["salary"] * 2))

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x.firstname + "," + x.lastname, x.gender, x.salary * 2))


def func1(x):
    first_name = x.firstname
    last_name = x.lastname
    name = first_name + "," + last_name
    gender = x.gender.lower()
    salary = x.salary * 2
    return name, gender, salary


rdd2 = df.rdd.map(lambda x: func1(x)).toDF().show()
rdd2 = df.rdd.map(func1).toDF().show()
