"""
PySpark filter condition is used to filter rows based on certain condition.
We can use where() if you are using sql instead of filter.

How to Filter Rows with NULL/NONE (IS NULL & IS NOT NULL) in PySpark
Spark Filter – startsWith(), endsWith() Examples
Spark Filter – contains(), like(), rlike() Examples

Syntax:-
filter(condition)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,ArrayType
from pyspark.sql.functions import col,array_contains

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
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

spark = SparkSession.builder.appName("filter-where-example").master("local[3]").getOrCreate()

sample_df = spark.createDataFrame(data=data,schema=schema)

sample_df.printSchema()
sample_df.show(truncate=False)

sample_df.filter((col("state")=="OH") & (col("gender")=="M")).show()

sample_df.filter(col("gender")=="M").show(truncate=False)

# Filter based on list of values- use is in() function of column class

# Filter IS IN list values

list1 = ['OH','NY']

sample_df.filter(col("state").isin(list1)).show()

language_list =['Java','Scala','C++']

# Imp to filter based on array _contains
sample_df.filter(array_contains(col("languages"),"Java")).show(truncate=False)

# Filter on nested struct columns

sample_df.filter(col("name.lastname")=="Smith").show()

# filter based on starts with, ends with, contains
# using startswith
sample_df.filter(col("state").startswith('O')).show()

# using endswith

sample_df.filter(col("state").endswith('Y')).show()

# contains

sample_df.filter(col("state").contains("H")).show()


