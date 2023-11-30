from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, StructField
from pyspark.sql.functions import col, array_contains

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

spark = SparkSession.builder.master("local[*]").appName("filter examples").getOrCreate()

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

df.filter(df.state == "OH").show()

df.filter(df.state != "OH").show()

# Using Sql col functions
df.filter(col("state") == "OH").show()

# Filter with multiple conditions
df.filter((df.state == "OH") & (df.gender == 'F')).show()

"""
When you want to filter values present in an array collection column
array_contains() from pyspark.sql.functions returns boolean value/true or false
"""

df.filter(array_contains(df.languages, 'Java')).show()
