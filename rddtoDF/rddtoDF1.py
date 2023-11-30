from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.appName("Convert RDD to DF").getOrCreate()
dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
rdd = spark.sparkContext.parallelize(dept)

# Convert rdd to DF
df = rdd.toDF()
df.show()
df.printSchema()

deptColumns = ["dept_name", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=True)

spark = SparkSession.builder.appName("RDDtoDF").getOrCreate()

data = [("James", ",", "Smith", "36636", "M", 3000),
        ("Michael", ",", "Smith", "26636", "M", 4000),
        ("Robert", ",", "Langdon", "36636", "M", 3000),
        ("Jinny", ",", "Smith", "36636", "F", 3000)]
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

structureData = [
    (("James", ",", "Smith"), "36636", "M", 3000),
    (("Michael", ",", "Smith"), "26636", "M", 4000),
    (("Robert", ",", "Langdon"), "36636", "M", 3000),
    (("Jinny", ",", "Smith"), "36636", "F", 3000)
]
StructureSchema = StructType([
    StructField('name', StructType([
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', StringType(), True)
])

df2 = spark.createDataFrame(data=structureData, schema=StructureSchema)
df2.printSchema()
df2.show(truncate=False)
