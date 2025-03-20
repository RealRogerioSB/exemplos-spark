from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "Smith", "M", 3000),
    ("Anna", "Rose", "F", 4100),
    ("Robert", "Williams", "M", 6200),
]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()


def reformat1(partition):
    for row in partition:
        yield [row.firstname + "," + row.lastname, row.salary * 10 / 100]


df.rdd.mapPartitions(reformat1).toDF().show()


def reformat2(partition):
    updated = []
    for row in partition:
        name = row.firstname + "," + row.lastname
        bonus = row.salary * 10 / 100
        updated.append([name, bonus])
    return iter(updated)


df2 = df.rdd.mapPartitions(reformat2).toDF("name", "bonus")
df2.show()
