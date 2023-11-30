"""
PySpark column class | Operators and Functions
pyspark.sql.Column class provides several functions to work with DataFrame
to manipulate the Column values, evaluiate the boolean expresion to filter rows, retrieve a
value or part of a value from a DataFrame column and to work with list, map and struct coluns
"""

from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.\
master('local[*]').appName("test").getOrCreate()


callObj = lit("pyspark demo")

data = [("James", "M"), ("Ann", "F")]
df = spark.createDataFrame(data).toDF("name.fname","gender")
df.printSchema()

df.select(df.gender).show()
df.select(df["`name.fname`"]).show()

# Using sql function col()
df.select(col("gender")).show()
df.select(col("`name.fname`")).show()
